/*
 * Copyright (c) 2002, 2021, Oracle and/or its affiliates.
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

import java.io.InputStream;
import java.io.Reader;
import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.URL;
import java.sql.Array;
import java.sql.Clob;
import java.sql.Date;
import java.sql.JDBCType;
import java.sql.NClob;
import java.sql.ParameterMetaData;
import java.sql.Ref;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLType;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Wrapper;
import java.util.ArrayList;
import java.util.Calendar;

import com.mysql.cj.BindValue;
import com.mysql.cj.CancelQueryTask;
import com.mysql.cj.ClientPreparedQuery;
import com.mysql.cj.ClientPreparedQueryBindings;
import com.mysql.cj.Messages;
import com.mysql.cj.MysqlType;
import com.mysql.cj.NativeSession;
import com.mysql.cj.ParseInfo;
import com.mysql.cj.PreparedQuery;
import com.mysql.cj.Query;
import com.mysql.cj.QueryBindings;
import com.mysql.cj.QueryReturnType;
import com.mysql.cj.conf.PropertyKey;
import com.mysql.cj.exceptions.CJException;
import com.mysql.cj.exceptions.FeatureNotAvailableException;
import com.mysql.cj.exceptions.MysqlErrorNumbers;
import com.mysql.cj.exceptions.StatementIsClosedException;
import com.mysql.cj.jdbc.exceptions.MySQLStatementCancelledException;
import com.mysql.cj.jdbc.exceptions.MySQLTimeoutException;
import com.mysql.cj.jdbc.exceptions.SQLError;
import com.mysql.cj.jdbc.exceptions.SQLExceptionsMapping;
import com.mysql.cj.jdbc.result.CachedResultSetMetaData;
import com.mysql.cj.jdbc.result.ResultSetInternalMethods;
import com.mysql.cj.jdbc.result.ResultSetMetaData;
import com.mysql.cj.log.ProfilerEvent;
import com.mysql.cj.protocol.ColumnDefinition;
import com.mysql.cj.protocol.Message;
import com.mysql.cj.protocol.a.NativePacketPayload;
import com.mysql.cj.result.Field;
import com.mysql.cj.util.Util;

/**
 * A SQL Statement is pre-compiled and stored in a PreparedStatement object. This object can then be used to efficiently execute this statement multiple times.
 * 
 * <p>
 * <B>Note:</B> The setXXX methods for setting IN parameter values must specify types that are compatible with the defined SQL type of the input parameter. For
 * instance, if the IN parameter has SQL type Integer, then setInt should be used.
 * </p>
 * 
 * <p>
 * If arbitrary parameter type conversions are required, then the setObject method should be used with a target SQL type.
 * </p>
 */
public class ClientPreparedStatement extends com.mysql.cj.jdbc.StatementImpl implements JdbcPreparedStatement {

    /**
     * Does the batch (if any) contain "plain" statements added by
     * Statement.addBatch(String)?
     * 
     * If so, we can't re-write it to use multi-value or multi-queries.
     */
    protected boolean batchHasPlainStatements = false;

    protected MysqlParameterMetadata parameterMetaData;

    private java.sql.ResultSetMetaData pstmtResultMetaData;

    protected String batchedValuesClause;

    private boolean doPingInstead;

    private boolean compensateForOnDuplicateKeyUpdate = false;

    protected int rewrittenBatchSize = 0;

    /**
     * Creates a prepared statement instance
     * 
     * @param conn
     *            the connection creating this statement
     * @param sql
     *            the SQL for this statement
     * @param db
     *            the database this statement should be issued against
     * @return ClientPreparedStatement
     * @throws SQLException
     *             if a database access error occurs
     */
    protected static ClientPreparedStatement getInstance(JdbcConnection conn, String sql, String db) throws SQLException {
        return new ClientPreparedStatement(conn, sql, db);
    }

    /**
     * Creates a prepared statement instance
     * 
     * @param conn
     *            the connection creating this statement
     * @param sql
     *            the SQL for this statement
     * @param db
     *            the database this statement should be issued against
     * @param cachedParseInfo
     *            already created parseInfo or null.
     * @return ClientPreparedStatement instance
     * @throws SQLException
     *             if a database access error occurs
     */
    protected static ClientPreparedStatement getInstance(JdbcConnection conn, String sql, String db, ParseInfo cachedParseInfo) throws SQLException {
        return new ClientPreparedStatement(conn, sql, db, cachedParseInfo);
    }

    @Override
    protected void initQuery() {
        this.query = new ClientPreparedQuery(this.session);
    }

    /**
     * Constructor used by server-side prepared statements
     * 
     * @param conn
     *            the connection that created us
     * @param db
     *            the database in use when we were created
     * 
     * @throws SQLException
     *             if an error occurs
     */
    protected ClientPreparedStatement(JdbcConnection conn, String db) throws SQLException {
        super(conn, db);

        setPoolable(true);
        this.compensateForOnDuplicateKeyUpdate = this.session.getPropertySet().getBooleanProperty(PropertyKey.compensateOnDuplicateKeyUpdateCounts).getValue();
    }

    /**
     * Constructor for the PreparedStatement class.
     * 
     * @param conn
     *            the connection creating this statement
     * @param sql
     *            the SQL for this statement
     * @param db
     *            the database this statement should be issued against
     * 
     * @throws SQLException
     *             if a database error occurs.
     */
    public ClientPreparedStatement(JdbcConnection conn, String sql, String db) throws SQLException {
        this(conn, sql, db, null);
    }

    /**
     * Creates a new PreparedStatement object.
     * 
     * @param conn
     *            the connection creating this statement
     * @param sql
     *            the SQL for this statement
     * @param db
     *            the database this statement should be issued against
     * @param cachedParseInfo
     *            already created parseInfo or null.
     * 
     * @throws SQLException
     *             if a database access error occurs
     */
    public ClientPreparedStatement(JdbcConnection conn, String sql, String db, ParseInfo cachedParseInfo) throws SQLException {
        this(conn, db);

        try {
            ((PreparedQuery<?>) this.query).checkNullOrEmptyQuery(sql);
            ((PreparedQuery<?>) this.query).setOriginalSql(sql);
            ((PreparedQuery<?>) this.query).setParseInfo(cachedParseInfo != null ? cachedParseInfo : new ParseInfo(sql, this.session, this.charEncoding));
        } catch (CJException e) {
            throw SQLExceptionsMapping.translateException(e, this.exceptionInterceptor);
        }

        this.doPingInstead = sql.startsWith(PING_MARKER);

        initializeFromParseInfo();
    }

    @Override
    public QueryBindings<?> getQueryBindings() {
        return ((PreparedQuery<?>) this.query).getQueryBindings();
    }

    /**
     * Returns this PreparedStatement represented as a string.
     * 
     * @return this PreparedStatement represented as a string.
     */
    @Override
    public String toString() {
        StringBuilder buf = new StringBuilder();
        buf.append(this.getClass().getName());
        buf.append(": ");

        try {
            buf.append(asSql());
        } catch (SQLException sqlEx) {
            buf.append("EXCEPTION: " + sqlEx.toString());
        }

        return buf.toString();
    }

    @Override
    public void addBatch() throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            QueryBindings<?> queryBindings = ((PreparedQuery<?>) this.query).getQueryBindings();
            queryBindings.checkAllParametersSet();
            this.query.addBatch(queryBindings.clone());
        }
    }

    @Override
    public void addBatch(String sql) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            this.batchHasPlainStatements = true;

            super.addBatch(sql);
        }
    }

    public String asSql() throws SQLException {
        return ((PreparedQuery<?>) this.query).asSql(false);
    }

    public String asSql(boolean quoteStreamsAndUnknowns) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            return ((PreparedQuery<?>) this.query).asSql(quoteStreamsAndUnknowns);
        }
    }

    @Override
    public void clearBatch() throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            this.batchHasPlainStatements = false;

            super.clearBatch();
        }
    }

    @Override
    public void clearParameters() throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            for (BindValue bv : ((PreparedQuery<?>) this.query).getQueryBindings().getBindValues()) {
                bv.reset();
            }
        }
    }

    /**
     * Check to see if the statement is safe for read-only replicas after failover.
     * 
     * @return true if safe for read-only.
     * @throws SQLException
     *             if a database access error occurs or this method is called on a closed PreparedStatement
     */
    protected boolean checkReadOnlySafeStatement() throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            return ParseInfo.isReadOnlySafeQuery(((PreparedQuery<?>) this.query).getOriginalSql(), this.session.getServerSession().isNoBackslashEscapesSet())
                    || !this.connection.isReadOnly();
        }
    }

    @Override
    public boolean execute() throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {

            JdbcConnection locallyScopedConn = this.connection;

            if (!this.doPingInstead && !checkReadOnlySafeStatement()) {
                throw SQLError.createSQLException(Messages.getString("PreparedStatement.20") + Messages.getString("PreparedStatement.21"),
                        MysqlErrorNumbers.SQL_STATE_ILLEGAL_ARGUMENT, this.exceptionInterceptor);
            }

            ResultSetInternalMethods rs = null;

            this.lastQueryIsOnDupKeyUpdate = false;

            if (this.retrieveGeneratedKeys) {
                this.lastQueryIsOnDupKeyUpdate = containsOnDuplicateKeyUpdateInSQL();
            }

            this.batchedGeneratedKeys = null;

            resetCancelledState();

            implicitlyCloseAllOpenResults();

            clearWarnings();

            if (this.doPingInstead) {
                doPingInstead();

                return true;
            }

            setupStreamingTimeout(locallyScopedConn);

            Message sendPacket = ((PreparedQuery<?>) this.query).fillSendPacket();

            String oldDb = null;

            if (!locallyScopedConn.getDatabase().equals(this.getCurrentDatabase())) {
                oldDb = locallyScopedConn.getDatabase();
                locallyScopedConn.setDatabase(this.getCurrentDatabase());
            }

            //
            // Check if we have cached metadata for this query...
            //
            CachedResultSetMetaData cachedMetadata = null;

            boolean cacheResultSetMetadata = locallyScopedConn.getPropertySet().getBooleanProperty(PropertyKey.cacheResultSetMetadata).getValue();
            if (cacheResultSetMetadata) {
                cachedMetadata = locallyScopedConn.getCachedMetaData(((PreparedQuery<?>) this.query).getOriginalSql());
            }

            //
            // Only apply max_rows to selects
            //
            locallyScopedConn.setSessionMaxRows(getParseInfo().getFirstStmtChar() == 'S' ? this.maxRows : -1);

            rs = executeInternal(this.maxRows, sendPacket, createStreamingResultSet(), (getParseInfo().getFirstStmtChar() == 'S'), cachedMetadata, false);

            if (cachedMetadata != null) {
                locallyScopedConn.initializeResultsMetadataFromCache(((PreparedQuery<?>) this.query).getOriginalSql(), cachedMetadata, rs);
            } else {
                if (rs.hasRows() && cacheResultSetMetadata) {
                    locallyScopedConn.initializeResultsMetadataFromCache(((PreparedQuery<?>) this.query).getOriginalSql(), null /* will be created */, rs);
                }
            }

            if (this.retrieveGeneratedKeys) {
                rs.setFirstCharOfQuery(getParseInfo().getFirstStmtChar());
            }

            if (oldDb != null) {
                locallyScopedConn.setDatabase(oldDb);
            }

            if (rs != null) {
                this.lastInsertId = rs.getUpdateID();

                this.results = rs;
            }

            return ((rs != null) && rs.hasRows());
        }
    }

    @Override
    protected long[] executeBatchInternal() throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {

            if (this.connection.isReadOnly()) {
                throw new SQLException(Messages.getString("PreparedStatement.25") + Messages.getString("PreparedStatement.26"),
                        MysqlErrorNumbers.SQL_STATE_ILLEGAL_ARGUMENT);
            }

            if (this.query.getBatchedArgs() == null || this.query.getBatchedArgs().size() == 0) {
                return new long[0];
            }

            // we timeout the entire batch, not individual statements
            int batchTimeout = getTimeoutInMillis();
            setTimeoutInMillis(0);

            resetCancelledState();

            try {
                statementBegins();

                clearWarnings();

                if (!this.batchHasPlainStatements && this.rewriteBatchedStatements.getValue()) {

                    if (getParseInfo().canRewriteAsMultiValueInsertAtSqlLevel()) {
                        return executeBatchedInserts(batchTimeout);
                    }

                    if (!this.batchHasPlainStatements && this.query.getBatchedArgs() != null
                            && this.query.getBatchedArgs().size() > 3 /* cost of option setting rt-wise */) {
                        return executePreparedBatchAsMultiStatement(batchTimeout);
                    }
                }

                return executeBatchSerially(batchTimeout);
            } finally {
                this.query.getStatementExecuting().set(false);

                clearBatch();
            }
        }
    }

    /**
     * Rewrites the already prepared statement into a multi-statement
     * query of 'statementsPerBatch' values and executes the entire batch
     * using this new statement.
     * 
     * @param batchTimeout
     *            timeout for the batch execution
     * @return update counts in the same fashion as executeBatch()
     * 
     * @throws SQLException
     *             if a database access error occurs or this method is called on a closed PreparedStatement
     */
    protected long[] executePreparedBatchAsMultiStatement(int batchTimeout) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            // This is kind of an abuse, but it gets the job done
            if (this.batchedValuesClause == null) {
                this.batchedValuesClause = ((PreparedQuery<?>) this.query).getOriginalSql() + ";";
            }

            JdbcConnection locallyScopedConn = this.connection;

            boolean multiQueriesEnabled = locallyScopedConn.getPropertySet().getBooleanProperty(PropertyKey.allowMultiQueries).getValue();
            CancelQueryTask timeoutTask = null;

            try {
                clearWarnings();

                int numBatchedArgs = this.query.getBatchedArgs().size();

                if (this.retrieveGeneratedKeys) {
                    this.batchedGeneratedKeys = new ArrayList<>(numBatchedArgs);
                }

                int numValuesPerBatch = ((PreparedQuery<?>) this.query).computeBatchSize(numBatchedArgs);

                if (numBatchedArgs < numValuesPerBatch) {
                    numValuesPerBatch = numBatchedArgs;
                }

                java.sql.PreparedStatement batchedStatement = null;

                int batchedParamIndex = 1;
                int numberToExecuteAsMultiValue = 0;
                int batchCounter = 0;
                int updateCountCounter = 0;
                long[] updateCounts = new long[numBatchedArgs * getParseInfo().getNumberOfQueries()];
                SQLException sqlEx = null;

                try {
                    if (!multiQueriesEnabled) {
                        ((NativeSession) locallyScopedConn.getSession()).enableMultiQueries();
                    }

                    batchedStatement = this.retrieveGeneratedKeys
                            ? ((Wrapper) locallyScopedConn.prepareStatement(generateMultiStatementForBatch(numValuesPerBatch), RETURN_GENERATED_KEYS))
                                    .unwrap(java.sql.PreparedStatement.class)
                            : ((Wrapper) locallyScopedConn.prepareStatement(generateMultiStatementForBatch(numValuesPerBatch)))
                                    .unwrap(java.sql.PreparedStatement.class);

                    timeoutTask = startQueryTimer((StatementImpl) batchedStatement, batchTimeout);

                    numberToExecuteAsMultiValue = numBatchedArgs < numValuesPerBatch ? numBatchedArgs : numBatchedArgs / numValuesPerBatch;

                    int numberArgsToExecute = numberToExecuteAsMultiValue * numValuesPerBatch;

                    for (int i = 0; i < numberArgsToExecute; i++) {
                        if (i != 0 && i % numValuesPerBatch == 0) {
                            try {
                                batchedStatement.execute();
                            } catch (SQLException ex) {
                                sqlEx = handleExceptionForBatch(batchCounter, numValuesPerBatch, updateCounts, ex);
                            }

                            updateCountCounter = processMultiCountsAndKeys((StatementImpl) batchedStatement, updateCountCounter, updateCounts);

                            batchedStatement.clearParameters();
                            batchedParamIndex = 1;
                        }

                        batchedParamIndex = setOneBatchedParameterSet(batchedStatement, batchedParamIndex, this.query.getBatchedArgs().get(batchCounter++));
                    }

                    try {
                        batchedStatement.execute();
                    } catch (SQLException ex) {
                        sqlEx = handleExceptionForBatch(batchCounter - 1, numValuesPerBatch, updateCounts, ex);
                    }

                    updateCountCounter = processMultiCountsAndKeys((StatementImpl) batchedStatement, updateCountCounter, updateCounts);

                    batchedStatement.clearParameters();

                    numValuesPerBatch = numBatchedArgs - batchCounter;

                    if (timeoutTask != null) {
                        // we need to check the cancel state now because we loose if after the following batchedStatement.close()
                        ((JdbcPreparedStatement) batchedStatement).checkCancelTimeout();
                    }

                } finally {
                    if (batchedStatement != null) {
                        batchedStatement.close();
                        batchedStatement = null;
                    }
                }

                try {
                    if (numValuesPerBatch > 0) {

                        batchedStatement = this.retrieveGeneratedKeys
                                ? locallyScopedConn.prepareStatement(generateMultiStatementForBatch(numValuesPerBatch), RETURN_GENERATED_KEYS)
                                : locallyScopedConn.prepareStatement(generateMultiStatementForBatch(numValuesPerBatch));

                        if (timeoutTask != null) {
                            timeoutTask.setQueryToCancel((Query) batchedStatement);
                        }

                        batchedParamIndex = 1;

                        while (batchCounter < numBatchedArgs) {
                            batchedParamIndex = setOneBatchedParameterSet(batchedStatement, batchedParamIndex, this.query.getBatchedArgs().get(batchCounter++));
                        }

                        try {
                            batchedStatement.execute();
                        } catch (SQLException ex) {
                            sqlEx = handleExceptionForBatch(batchCounter - 1, numValuesPerBatch, updateCounts, ex);
                        }

                        updateCountCounter = processMultiCountsAndKeys((StatementImpl) batchedStatement, updateCountCounter, updateCounts);

                        batchedStatement.clearParameters();
                    }

                    if (timeoutTask != null) {
                        stopQueryTimer(timeoutTask, true, true);
                        timeoutTask = null;
                    }

                    if (sqlEx != null) {
                        throw SQLError.createBatchUpdateException(sqlEx, updateCounts, this.exceptionInterceptor);
                    }

                    return updateCounts;
                } finally {
                    if (batchedStatement != null) {
                        batchedStatement.close();
                    }
                }
            } finally {
                stopQueryTimer(timeoutTask, false, false);
                resetCancelledState();

                if (!multiQueriesEnabled) {
                    ((NativeSession) locallyScopedConn.getSession()).disableMultiQueries();
                }

                clearBatch();
            }
        }
    }

    protected int setOneBatchedParameterSet(java.sql.PreparedStatement batchedStatement, int batchedParamIndex, Object paramSet) throws SQLException {
        QueryBindings<?> paramArg = (QueryBindings<?>) paramSet;

        BindValue[] bindValues = paramArg.getBindValues();

        for (int j = 0; j < bindValues.length; j++) {
            if (bindValues[j].isNull()) {
                batchedStatement.setNull(batchedParamIndex++, MysqlType.NULL.getJdbcType());
            } else {
                if (bindValues[j].isStream()) {
                    batchedStatement.setBinaryStream(batchedParamIndex++, bindValues[j].getStreamValue(), bindValues[j].getStreamLength());
                } else {
                    ((JdbcPreparedStatement) batchedStatement).setBytesNoEscapeNoQuotes(batchedParamIndex++, bindValues[j].getByteValue());
                }
            }
        }

        return batchedParamIndex;
    }

    private String generateMultiStatementForBatch(int numBatches) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            String origSql = ((PreparedQuery<?>) this.query).getOriginalSql();
            StringBuilder newStatementSql = new StringBuilder((origSql.length() + 1) * numBatches);

            newStatementSql.append(origSql);

            for (int i = 0; i < numBatches - 1; i++) {
                newStatementSql.append(';');
                newStatementSql.append(origSql);
            }

            return newStatementSql.toString();
        }
    }

    /**
     * Rewrites the already prepared statement into a multi-value insert
     * statement of 'statementsPerBatch' values and executes the entire batch
     * using this new statement.
     * 
     * @param batchTimeout
     *            timeout for the batch execution
     * @return update counts in the same fashion as executeBatch()
     * 
     * @throws SQLException
     *             if a database access error occurs or this method is called on a closed PreparedStatement
     */
    protected long[] executeBatchedInserts(int batchTimeout) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            String valuesClause = getParseInfo().getValuesClause();

            JdbcConnection locallyScopedConn = this.connection;

            if (valuesClause == null) {
                return executeBatchSerially(batchTimeout);
            }

            int numBatchedArgs = this.query.getBatchedArgs().size();

            if (this.retrieveGeneratedKeys) {
                this.batchedGeneratedKeys = new ArrayList<>(numBatchedArgs);
            }

            int numValuesPerBatch = ((PreparedQuery<?>) this.query).computeBatchSize(numBatchedArgs);

            if (numBatchedArgs < numValuesPerBatch) {
                numValuesPerBatch = numBatchedArgs;
            }

            JdbcPreparedStatement batchedStatement = null;

            int batchedParamIndex = 1;
            long updateCountRunningTotal = 0;
            int numberToExecuteAsMultiValue = 0;
            int batchCounter = 0;
            CancelQueryTask timeoutTask = null;
            SQLException sqlEx = null;

            long[] updateCounts = new long[numBatchedArgs];

            try {
                try {
                    batchedStatement = /* FIXME -if we ever care about folks proxying our JdbcConnection */
                            prepareBatchedInsertSQL(locallyScopedConn, numValuesPerBatch);

                    timeoutTask = startQueryTimer(batchedStatement, batchTimeout);

                    numberToExecuteAsMultiValue = numBatchedArgs < numValuesPerBatch ? numBatchedArgs : numBatchedArgs / numValuesPerBatch;

                    int numberArgsToExecute = numberToExecuteAsMultiValue * numValuesPerBatch;

                    for (int i = 0; i < numberArgsToExecute; i++) {
                        if (i != 0 && i % numValuesPerBatch == 0) {
                            try {
                                updateCountRunningTotal += batchedStatement.executeLargeUpdate();
                            } catch (SQLException ex) {
                                sqlEx = handleExceptionForBatch(batchCounter - 1, numValuesPerBatch, updateCounts, ex);
                            }

                            getBatchedGeneratedKeys(batchedStatement);
                            batchedStatement.clearParameters();
                            batchedParamIndex = 1;

                        }

                        batchedParamIndex = setOneBatchedParameterSet(batchedStatement, batchedParamIndex, this.query.getBatchedArgs().get(batchCounter++));
                    }

                    try {
                        updateCountRunningTotal += batchedStatement.executeLargeUpdate();
                    } catch (SQLException ex) {
                        sqlEx = handleExceptionForBatch(batchCounter - 1, numValuesPerBatch, updateCounts, ex);
                    }

                    getBatchedGeneratedKeys(batchedStatement);

                    numValuesPerBatch = numBatchedArgs - batchCounter;
                } finally {
                    if (batchedStatement != null) {
                        batchedStatement.close();
                        batchedStatement = null;
                    }
                }

                try {
                    if (numValuesPerBatch > 0) {
                        batchedStatement = prepareBatchedInsertSQL(locallyScopedConn, numValuesPerBatch);

                        if (timeoutTask != null) {
                            timeoutTask.setQueryToCancel(batchedStatement);
                        }

                        batchedParamIndex = 1;

                        while (batchCounter < numBatchedArgs) {
                            batchedParamIndex = setOneBatchedParameterSet(batchedStatement, batchedParamIndex, this.query.getBatchedArgs().get(batchCounter++));
                        }

                        try {
                            updateCountRunningTotal += batchedStatement.executeLargeUpdate();
                        } catch (SQLException ex) {
                            sqlEx = handleExceptionForBatch(batchCounter - 1, numValuesPerBatch, updateCounts, ex);
                        }

                        getBatchedGeneratedKeys(batchedStatement);
                    }

                    if (sqlEx != null) {
                        throw SQLError.createBatchUpdateException(sqlEx, updateCounts, this.exceptionInterceptor);
                    }

                    if (numBatchedArgs > 1) {
                        long updCount = updateCountRunningTotal > 0 ? java.sql.Statement.SUCCESS_NO_INFO : 0;
                        for (int j = 0; j < numBatchedArgs; j++) {
                            updateCounts[j] = updCount;
                        }
                    } else {
                        updateCounts[0] = updateCountRunningTotal;
                    }
                    return updateCounts;
                } finally {
                    if (batchedStatement != null) {
                        batchedStatement.close();
                    }
                }
            } finally {
                stopQueryTimer(timeoutTask, false, false);
                resetCancelledState();
            }
        }
    }

    /**
     * Executes the current batch of statements by executing them one-by-one.
     * 
     * @param batchTimeout
     *            timeout for the batch execution
     * @return a list of update counts
     * @throws SQLException
     *             if an error occurs
     */
    protected long[] executeBatchSerially(int batchTimeout) throws SQLException {

        synchronized (checkClosed().getConnectionMutex()) {
            if (this.connection == null) {
                checkClosed();
            }

            long[] updateCounts = null;

            if (this.query.getBatchedArgs() != null) {
                int nbrCommands = this.query.getBatchedArgs().size();
                updateCounts = new long[nbrCommands];

                for (int i = 0; i < nbrCommands; i++) {
                    updateCounts[i] = -3;
                }

                SQLException sqlEx = null;

                CancelQueryTask timeoutTask = null;

                try {
                    timeoutTask = startQueryTimer(this, batchTimeout);

                    if (this.retrieveGeneratedKeys) {
                        this.batchedGeneratedKeys = new ArrayList<>(nbrCommands);
                    }

                    int batchCommandIndex = ((PreparedQuery<?>) this.query).getBatchCommandIndex();

                    for (batchCommandIndex = 0; batchCommandIndex < nbrCommands; batchCommandIndex++) {

                        ((PreparedQuery<?>) this.query).setBatchCommandIndex(batchCommandIndex);

                        Object arg = this.query.getBatchedArgs().get(batchCommandIndex);

                        try {
                            if (arg instanceof String) {
                                updateCounts[batchCommandIndex] = executeUpdateInternal((String) arg, true, this.retrieveGeneratedKeys);

                                // limit one generated key per OnDuplicateKey statement
                                getBatchedGeneratedKeys(this.results.getFirstCharOfQuery() == 'I' && containsOnDuplicateKeyInString((String) arg) ? 1 : 0);
                            } else {
                                QueryBindings<?> queryBindings = (QueryBindings<?>) arg;
                                updateCounts[batchCommandIndex] = executeUpdateInternal(queryBindings, true);

                                // limit one generated key per OnDuplicateKey statement
                                getBatchedGeneratedKeys(containsOnDuplicateKeyUpdateInSQL() ? 1 : 0);
                            }
                        } catch (SQLException ex) {
                            updateCounts[batchCommandIndex] = EXECUTE_FAILED;

                            if (this.continueBatchOnError && !(ex instanceof MySQLTimeoutException) && !(ex instanceof MySQLStatementCancelledException)
                                    && !hasDeadlockOrTimeoutRolledBackTx(ex)) {
                                sqlEx = ex;
                            } else {
                                long[] newUpdateCounts = new long[batchCommandIndex];
                                System.arraycopy(updateCounts, 0, newUpdateCounts, 0, batchCommandIndex);

                                throw SQLError.createBatchUpdateException(ex, newUpdateCounts, this.exceptionInterceptor);
                            }
                        }
                    }

                    if (sqlEx != null) {
                        throw SQLError.createBatchUpdateException(sqlEx, updateCounts, this.exceptionInterceptor);
                    }
                } catch (NullPointerException npe) {
                    try {
                        checkClosed();
                    } catch (StatementIsClosedException connectionClosedEx) {
                        int batchCommandIndex = ((PreparedQuery<?>) this.query).getBatchCommandIndex();
                        updateCounts[batchCommandIndex] = EXECUTE_FAILED;

                        long[] newUpdateCounts = new long[batchCommandIndex];

                        System.arraycopy(updateCounts, 0, newUpdateCounts, 0, batchCommandIndex);

                        throw SQLError.createBatchUpdateException(SQLExceptionsMapping.translateException(connectionClosedEx), newUpdateCounts,
                                this.exceptionInterceptor);
                    }

                    throw npe; // we don't know why this happened, punt
                } finally {
                    ((PreparedQuery<?>) this.query).setBatchCommandIndex(-1);

                    stopQueryTimer(timeoutTask, false, false);
                    resetCancelledState();
                }
            }

            return (updateCounts != null) ? updateCounts : new long[0];
        }

    }

    /**
     * Actually execute the prepared statement. This is here so server-side
     * PreparedStatements can re-use most of the code from this class.
     * 
     * @param <M>
     *            extends {@link Message}
     * 
     * @param maxRowsToRetrieve
     *            the max number of rows to return
     * @param sendPacket
     *            the packet to send
     * @param createStreamingResultSet
     *            should a 'streaming' result set be created?
     * @param queryIsSelectOnly
     *            is this query doing a SELECT?
     * @param metadata
     *            use this metadata instead of the one provided on wire
     * @param isBatch
     *            is this a batch query?
     * 
     * @return the results as a ResultSet
     * 
     * @throws SQLException
     *             if an error occurs.
     */
    protected <M extends Message> ResultSetInternalMethods executeInternal(int maxRowsToRetrieve, M sendPacket, boolean createStreamingResultSet,
            boolean queryIsSelectOnly, ColumnDefinition metadata, boolean isBatch) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            try {

                JdbcConnection locallyScopedConnection = this.connection;

                ((PreparedQuery<?>) this.query).getQueryBindings()
                        .setNumberOfExecutions(((PreparedQuery<?>) this.query).getQueryBindings().getNumberOfExecutions() + 1);

                ResultSetInternalMethods rs;

                CancelQueryTask timeoutTask = null;

                try {
                    timeoutTask = startQueryTimer(this, getTimeoutInMillis());

                    if (!isBatch) {
                        statementBegins();
                    }

                    rs = ((NativeSession) locallyScopedConnection.getSession()).execSQL(this, null, maxRowsToRetrieve, (NativePacketPayload) sendPacket,
                            createStreamingResultSet, getResultSetFactory(), metadata, isBatch);

                    if (timeoutTask != null) {
                        stopQueryTimer(timeoutTask, true, true);
                        timeoutTask = null;
                    }

                } finally {
                    if (!isBatch) {
                        this.query.getStatementExecuting().set(false);
                    }

                    stopQueryTimer(timeoutTask, false, false);
                }

                return rs;
            } catch (NullPointerException npe) {
                checkClosed(); // we can't synchronize ourselves against async connection-close due to deadlock issues, so this is the next best thing for
                              // this particular corner case.

                throw npe;
            }
        }
    }

    @Override
    public java.sql.ResultSet executeQuery() throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {

            JdbcConnection locallyScopedConn = this.connection;

            if (!this.doPingInstead) {
                QueryReturnType queryReturnType = getParseInfo().getQueryReturnType();
                if (queryReturnType != QueryReturnType.PRODUCES_RESULT_SET && queryReturnType != QueryReturnType.MAY_PRODUCE_RESULT_SET) {
                    throw SQLError.createSQLException(Messages.getString("Statement.57"), MysqlErrorNumbers.SQL_STATE_ILLEGAL_ARGUMENT,
                            getExceptionInterceptor());
                }
            }

            this.batchedGeneratedKeys = null;

            resetCancelledState();

            implicitlyCloseAllOpenResults();

            clearWarnings();

            if (this.doPingInstead) {
                doPingInstead();

                return this.results;
            }

            setupStreamingTimeout(locallyScopedConn);

            Message sendPacket = ((PreparedQuery<?>) this.query).fillSendPacket();

            String oldDb = null;

            if (!locallyScopedConn.getDatabase().equals(this.getCurrentDatabase())) {
                oldDb = locallyScopedConn.getDatabase();
                locallyScopedConn.setDatabase(this.getCurrentDatabase());
            }

            //
            // Check if we have cached metadata for this query...
            //
            CachedResultSetMetaData cachedMetadata = null;
            boolean cacheResultSetMetadata = locallyScopedConn.getPropertySet().getBooleanProperty(PropertyKey.cacheResultSetMetadata).getValue();

            String origSql = ((PreparedQuery<?>) this.query).getOriginalSql();

            if (cacheResultSetMetadata) {
                cachedMetadata = locallyScopedConn.getCachedMetaData(origSql);
            }

            locallyScopedConn.setSessionMaxRows(this.maxRows);

            this.results = executeInternal(this.maxRows, sendPacket, createStreamingResultSet(), true, cachedMetadata, false);

            if (oldDb != null) {
                locallyScopedConn.setDatabase(oldDb);
            }

            if (cachedMetadata != null) {
                locallyScopedConn.initializeResultsMetadataFromCache(origSql, cachedMetadata, this.results);
            } else {
                if (cacheResultSetMetadata) {
                    locallyScopedConn.initializeResultsMetadataFromCache(origSql, null /* will be created */, this.results);
                }
            }

            this.lastInsertId = this.results.getUpdateID();

            return this.results;
        }
    }

    @Override
    public int executeUpdate() throws SQLException {
        return Util.truncateAndConvertToInt(executeLargeUpdate());
    }

    /*
     * We need this variant, because ServerPreparedStatement calls this for
     * batched updates, which will end up clobbering the warnings and generated
     * keys we need to gather for the batch.
     */
    protected long executeUpdateInternal(boolean clearBatchedGeneratedKeysAndWarnings, boolean isBatch) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            if (clearBatchedGeneratedKeysAndWarnings) {
                clearWarnings();
                this.batchedGeneratedKeys = null;
            }

            return executeUpdateInternal(((PreparedQuery<?>) this.query).getQueryBindings(), isBatch);
        }
    }

    /**
     * Added to allow batch-updates
     * 
     * @param bindings
     *            bindings object
     * @param isReallyBatch
     *            is it a batched statement?
     * 
     * @return the update count
     * 
     * @throws SQLException
     *             if a database error occurs
     */
    protected long executeUpdateInternal(QueryBindings<?> bindings, boolean isReallyBatch) throws SQLException {

        synchronized (checkClosed().getConnectionMutex()) {

            JdbcConnection locallyScopedConn = this.connection;

            if (locallyScopedConn.isReadOnly(false)) {
                throw SQLError.createSQLException(Messages.getString("PreparedStatement.34") + Messages.getString("PreparedStatement.35"),
                        MysqlErrorNumbers.SQL_STATE_ILLEGAL_ARGUMENT, this.exceptionInterceptor);
            }

            if (!isNonResultSetProducingQuery()) {
                throw SQLError.createSQLException(Messages.getString("PreparedStatement.37"), "01S03", this.exceptionInterceptor);
            }

            resetCancelledState();

            implicitlyCloseAllOpenResults();

            ResultSetInternalMethods rs = null;

            Message sendPacket = ((PreparedQuery<?>) this.query).fillSendPacket(bindings);

            String oldDb = null;

            if (!locallyScopedConn.getDatabase().equals(this.getCurrentDatabase())) {
                oldDb = locallyScopedConn.getDatabase();
                locallyScopedConn.setDatabase(this.getCurrentDatabase());
            }

            //
            // Only apply max_rows to selects
            //
            locallyScopedConn.setSessionMaxRows(-1);

            rs = executeInternal(-1, sendPacket, false, false, null, isReallyBatch);

            if (this.retrieveGeneratedKeys) {
                rs.setFirstCharOfQuery(getParseInfo().getFirstStmtChar());
            }

            if (oldDb != null) {
                locallyScopedConn.setDatabase(oldDb);
            }

            this.results = rs;

            this.updateCount = rs.getUpdateCount();

            if (containsOnDuplicateKeyUpdateInSQL() && this.compensateForOnDuplicateKeyUpdate) {
                if (this.updateCount == 2 || this.updateCount == 0) {
                    this.updateCount = 1;
                }
            }

            this.lastInsertId = rs.getUpdateID();

            return this.updateCount;
        }
    }

    protected boolean containsOnDuplicateKeyUpdateInSQL() {
        return getParseInfo().containsOnDuplicateKeyUpdateInSQL();
    }

    /**
     * Returns a prepared statement for the number of batched parameters, used when re-writing batch INSERTs.
     * 
     * @param localConn
     *            the connection creating this statement
     * @param numBatches
     *            number of entries in a batch
     * @return new ClientPreparedStatement
     * @throws SQLException
     *             if a database access error occurs or this method is called on a closed PreparedStatement
     */
    protected ClientPreparedStatement prepareBatchedInsertSQL(JdbcConnection localConn, int numBatches) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            ClientPreparedStatement pstmt = new ClientPreparedStatement(localConn, "Rewritten batch of: " + ((PreparedQuery<?>) this.query).getOriginalSql(),
                    this.getCurrentDatabase(), getParseInfo().getParseInfoForBatch(numBatches));
            pstmt.setRetrieveGeneratedKeys(this.retrieveGeneratedKeys);
            pstmt.rewrittenBatchSize = numBatches;

            getQueryAttributesBindings().runThroughAll(a -> ((JdbcStatement) pstmt).setAttribute(a.getName(), a.getValue()));

            return pstmt;
        }
    }

    protected void setRetrieveGeneratedKeys(boolean flag) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            this.retrieveGeneratedKeys = flag;
        }
    }

    @Override
    public byte[] getBytesRepresentation(int parameterIndex) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            return ((PreparedQuery<?>) this.query).getQueryBindings().getBytesRepresentation(getCoreParameterIndex(parameterIndex));
        }
    }

    @Override
    public byte[] getOrigBytes(int parameterIndex) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            return ((PreparedQuery<?>) this.query).getQueryBindings().getOrigBytes(getCoreParameterIndex(parameterIndex));
        }
    }

    @Override
    public java.sql.ResultSetMetaData getMetaData() throws SQLException {

        synchronized (checkClosed().getConnectionMutex()) {
            //
            // We could just tack on a LIMIT 0 here no matter what the  statement, and check if a result set was returned or not, but I'm not comfortable with
            // that, myself, so we take the "safer" road, and only allow metadata for _actual_ SELECTS (but not SHOWs).
            // 
            // CALL's are trapped further up and you end up with a  CallableStatement anyway.
            //

            if (!isResultSetProducingQuery()) {
                return null;
            }

            JdbcPreparedStatement mdStmt = null;
            java.sql.ResultSet mdRs = null;

            if (this.pstmtResultMetaData == null) {
                try {
                    mdStmt = new ClientPreparedStatement(this.connection, ((PreparedQuery<?>) this.query).getOriginalSql(), this.getCurrentDatabase(),
                            getParseInfo());

                    mdStmt.setMaxRows(1);

                    int paramCount = ((PreparedQuery<?>) this.query).getParameterCount();

                    for (int i = 1; i <= paramCount; i++) {
                        mdStmt.setString(i, null);
                    }

                    boolean hadResults = mdStmt.execute();

                    if (hadResults) {
                        mdRs = mdStmt.getResultSet();

                        this.pstmtResultMetaData = mdRs.getMetaData();
                    } else {
                        this.pstmtResultMetaData = new ResultSetMetaData(this.session, new Field[0],
                                this.session.getPropertySet().getBooleanProperty(PropertyKey.useOldAliasMetadataBehavior).getValue(),
                                this.session.getPropertySet().getBooleanProperty(PropertyKey.yearIsDateType).getValue(), this.exceptionInterceptor);
                    }
                } finally {
                    SQLException sqlExRethrow = null;

                    if (mdRs != null) {
                        try {
                            mdRs.close();
                        } catch (SQLException sqlEx) {
                            sqlExRethrow = sqlEx;
                        }

                        mdRs = null;
                    }

                    if (mdStmt != null) {
                        try {
                            mdStmt.close();
                        } catch (SQLException sqlEx) {
                            sqlExRethrow = sqlEx;
                        }

                        mdStmt = null;
                    }

                    if (sqlExRethrow != null) {
                        throw sqlExRethrow;
                    }
                }
            }

            return this.pstmtResultMetaData;
        }
    }

    /**
     * Checks if the given SQL query is a result set producing query.
     * 
     * @return
     *         <code>true</code> if the query produces a result set, <code>false</code> otherwise.
     */
    protected boolean isResultSetProducingQuery() {
        QueryReturnType queryReturnType = getParseInfo().getQueryReturnType();
        return queryReturnType == QueryReturnType.PRODUCES_RESULT_SET || queryReturnType == QueryReturnType.MAY_PRODUCE_RESULT_SET;
    }

    /**
     * Checks if the given SQL query does not return a result set.
     * 
     * @return
     *         <code>true</code> if the query does not produce a result set, <code>false</code> otherwise.
     */
    private boolean isNonResultSetProducingQuery() {
        QueryReturnType queryReturnType = getParseInfo().getQueryReturnType();
        return queryReturnType == QueryReturnType.DOES_NOT_PRODUCE_RESULT_SET || queryReturnType == QueryReturnType.MAY_PRODUCE_RESULT_SET;
    }

    @Override
    public ParameterMetaData getParameterMetaData() throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            if (this.parameterMetaData == null) {
                if (this.session.getPropertySet().getBooleanProperty(PropertyKey.generateSimpleParameterMetadata).getValue()) {
                    this.parameterMetaData = new MysqlParameterMetadata(((PreparedQuery<?>) this.query).getParameterCount());
                } else {
                    this.parameterMetaData = new MysqlParameterMetadata(this.session, null, ((PreparedQuery<?>) this.query).getParameterCount(),
                            this.exceptionInterceptor);
                }
            }

            return this.parameterMetaData;
        }
    }

    @Override
    public ParseInfo getParseInfo() {
        return ((PreparedQuery<?>) this.query).getParseInfo();
    }

    @SuppressWarnings("unchecked")
    private void initializeFromParseInfo() throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {

            int parameterCount = getParseInfo().getStaticSql().length - 1;
            ((PreparedQuery<?>) this.query).setParameterCount(parameterCount);
            ((PreparedQuery<ClientPreparedQueryBindings>) this.query).setQueryBindings(new ClientPreparedQueryBindings(parameterCount, this.session));
            ((ClientPreparedQuery) this.query).getQueryBindings().setLoadDataQuery(getParseInfo().isLoadData());

            clearParameters();
        }
    }

    @Override
    public boolean isNull(int paramIndex) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            return ((PreparedQuery<?>) this.query).getQueryBindings().getBindValues()[getCoreParameterIndex(paramIndex)].isNull();
        }
    }

    @Override
    public void realClose(boolean calledExplicitly, boolean closeOpenResults) throws SQLException {
        JdbcConnection locallyScopedConn = this.connection;

        if (locallyScopedConn == null) {
            return; // already closed
        }

        synchronized (locallyScopedConn.getConnectionMutex()) {

            // additional check in case Statement was closed
            // while current thread was waiting for lock
            if (this.isClosed) {
                return;
            }

            if (this.useUsageAdvisor) {
                QueryBindings<?> qb = ((PreparedQuery<?>) this.query).getQueryBindings();
                if (qb == null || qb.getNumberOfExecutions() <= 1) {
                    this.session.getProfilerEventHandler().processEvent(ProfilerEvent.TYPE_USAGE, this.session, this, null, 0, new Throwable(),
                            Messages.getString("PreparedStatement.43"));
                }
            }

            super.realClose(calledExplicitly, closeOpenResults);

            ((PreparedQuery<?>) this.query).setOriginalSql(null);
            ((PreparedQuery<?>) this.query).setQueryBindings(null);
        }
    }

    @Override
    public String getPreparedSql() {
        synchronized (checkClosed().getConnectionMutex()) {
            if (this.rewrittenBatchSize == 0) {
                return ((PreparedQuery<?>) this.query).getOriginalSql();
            }

            try {
                return getParseInfo().getSqlForBatch();
            } catch (UnsupportedEncodingException e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public int getUpdateCount() throws SQLException {
        int count = super.getUpdateCount();

        if (containsOnDuplicateKeyUpdateInSQL() && this.compensateForOnDuplicateKeyUpdate) {
            if (count == 2 || count == 0) {
                count = 1;
            }
        }

        return count;
    }

    @Override
    public long executeLargeUpdate() throws SQLException {
        return executeUpdateInternal(true, false);
    }

    public ParameterBindings getParameterBindings() throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            return new ParameterBindingsImpl((PreparedQuery<?>) this.query, this.session, this.resultSetFactory);
        }
    }

    /**
     * For calling stored functions, this will be -1 as Connector/J does not count
     * the first '?' parameter marker, but JDBC counts it * as 1, otherwise it will return 0
     * 
     * @return offset
     */
    protected int getParameterIndexOffset() {
        return 0;
    }

    protected void checkBounds(int paramIndex, int parameterIndexOffset) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            if ((paramIndex < 1)) {
                throw SQLError.createSQLException(Messages.getString("PreparedStatement.49") + paramIndex + Messages.getString("PreparedStatement.50"),
                        MysqlErrorNumbers.SQL_STATE_ILLEGAL_ARGUMENT, this.exceptionInterceptor);
            } else if (paramIndex > ((PreparedQuery<?>) this.query).getParameterCount()) {
                throw SQLError.createSQLException(
                        Messages.getString("PreparedStatement.51") + paramIndex + Messages.getString("PreparedStatement.52")
                                + ((PreparedQuery<?>) this.query).getParameterCount() + Messages.getString("PreparedStatement.53"),
                        MysqlErrorNumbers.SQL_STATE_ILLEGAL_ARGUMENT, this.exceptionInterceptor);
            } else if (parameterIndexOffset == -1 && paramIndex == 1) {
                throw SQLError.createSQLException(Messages.getString("PreparedStatement.63"), MysqlErrorNumbers.SQL_STATE_ILLEGAL_ARGUMENT,
                        this.exceptionInterceptor);
            }
        }
    }

    public final int getCoreParameterIndex(int paramIndex) throws SQLException {
        int parameterIndexOffset = getParameterIndexOffset();
        checkBounds(paramIndex, parameterIndexOffset);
        return paramIndex - 1 + parameterIndexOffset;
    }

    @Override
    public void setArray(int i, Array x) throws SQLException {
        throw SQLError.createSQLFeatureNotSupportedException();
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            ((PreparedQuery<?>) this.query).getQueryBindings().setAsciiStream(getCoreParameterIndex(parameterIndex), x);
        }
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, int length) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            ((PreparedQuery<?>) this.query).getQueryBindings().setAsciiStream(getCoreParameterIndex(parameterIndex), x, length);
        }
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, long length) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            ((PreparedQuery<?>) this.query).getQueryBindings().setAsciiStream(getCoreParameterIndex(parameterIndex), x, length);
        }
    }

    @Override
    public void setBigDecimal(int parameterIndex, BigDecimal x) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            ((PreparedQuery<?>) this.query).getQueryBindings().setBigDecimal(getCoreParameterIndex(parameterIndex), x);
        }
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            ((PreparedQuery<?>) this.query).getQueryBindings().setBinaryStream(getCoreParameterIndex(parameterIndex), x);
        }
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, int length) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            ((PreparedQuery<?>) this.query).getQueryBindings().setBinaryStream(getCoreParameterIndex(parameterIndex), x, length);
        }
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, long length) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            ((PreparedQuery<?>) this.query).getQueryBindings().setBinaryStream(getCoreParameterIndex(parameterIndex), x, length);
        }
    }

    @Override
    public void setBlob(int i, java.sql.Blob x) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            ((PreparedQuery<?>) this.query).getQueryBindings().setBlob(getCoreParameterIndex(i), x);
        }
    }

    @Override
    public void setBlob(int parameterIndex, InputStream inputStream) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            ((PreparedQuery<?>) this.query).getQueryBindings().setBlob(getCoreParameterIndex(parameterIndex), inputStream);
        }
    }

    @Override
    public void setBlob(int parameterIndex, InputStream inputStream, long length) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            ((PreparedQuery<?>) this.query).getQueryBindings().setBlob(getCoreParameterIndex(parameterIndex), inputStream, length);
        }
    }

    @Override
    public void setBoolean(int parameterIndex, boolean x) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            ((PreparedQuery<?>) this.query).getQueryBindings().setBoolean(getCoreParameterIndex(parameterIndex), x);
        }
    }

    @Override
    public void setByte(int parameterIndex, byte x) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            ((PreparedQuery<?>) this.query).getQueryBindings().setByte(getCoreParameterIndex(parameterIndex), x);
        }
    }

    @Override
    public void setBytes(int parameterIndex, byte[] x) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            ((PreparedQuery<?>) this.query).getQueryBindings().setBytes(getCoreParameterIndex(parameterIndex), x);
        }
    }

    @Override
    public void setBytes(int parameterIndex, byte[] x, boolean checkForIntroducer, boolean escapeForMBChars) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            ((PreparedQuery<?>) this.query).getQueryBindings().setBytes(getCoreParameterIndex(parameterIndex), x, checkForIntroducer, escapeForMBChars);
        }
    }

    @Override
    public void setBytesNoEscape(int parameterIndex, byte[] parameterAsBytes) throws SQLException {
        ((PreparedQuery<?>) this.query).getQueryBindings().setBytesNoEscape(getCoreParameterIndex(parameterIndex), parameterAsBytes);
    }

    @Override
    public void setBytesNoEscapeNoQuotes(int parameterIndex, byte[] parameterAsBytes) throws SQLException {
        ((PreparedQuery<?>) this.query).getQueryBindings().setBytesNoEscapeNoQuotes(getCoreParameterIndex(parameterIndex), parameterAsBytes);
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            ((PreparedQuery<?>) this.query).getQueryBindings().setCharacterStream(getCoreParameterIndex(parameterIndex), reader);
        }
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, int length) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            ((PreparedQuery<?>) this.query).getQueryBindings().setCharacterStream(getCoreParameterIndex(parameterIndex), reader, length);
        }
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, long length) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            ((PreparedQuery<?>) this.query).getQueryBindings().setCharacterStream(getCoreParameterIndex(parameterIndex), reader, length);
        }
    }

    @Override
    public void setClob(int parameterIndex, Reader reader) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            ((PreparedQuery<?>) this.query).getQueryBindings().setCharacterStream(getCoreParameterIndex(parameterIndex), reader);
        }
    }

    @Override
    public void setClob(int parameterIndex, Reader reader, long length) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            ((PreparedQuery<?>) this.query).getQueryBindings().setCharacterStream(getCoreParameterIndex(parameterIndex), reader, length);
        }
    }

    @Override
    public void setClob(int i, Clob x) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            ((PreparedQuery<?>) this.query).getQueryBindings().setClob(getCoreParameterIndex(i), x);
        }
    }

    @Override
    public void setDate(int parameterIndex, Date x) throws java.sql.SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            ((PreparedQuery<?>) this.query).getQueryBindings().setDate(getCoreParameterIndex(parameterIndex), x);
        }
    }

    @Override
    public void setDate(int parameterIndex, Date x, Calendar cal) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            ((PreparedQuery<?>) this.query).getQueryBindings().setDate(getCoreParameterIndex(parameterIndex), x, cal);
        }
    }

    @Override
    public void setDouble(int parameterIndex, double x) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            ((PreparedQuery<?>) this.query).getQueryBindings().setDouble(getCoreParameterIndex(parameterIndex), x);
        }
    }

    @Override
    public void setFloat(int parameterIndex, float x) throws SQLException {
        ((PreparedQuery<?>) this.query).getQueryBindings().setFloat(getCoreParameterIndex(parameterIndex), x);
    }

    @Override
    public void setInt(int parameterIndex, int x) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            ((PreparedQuery<?>) this.query).getQueryBindings().setInt(getCoreParameterIndex(parameterIndex), x);
        }
    }

    @Override
    public void setLong(int parameterIndex, long x) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            ((PreparedQuery<?>) this.query).getQueryBindings().setLong(getCoreParameterIndex(parameterIndex), x);
        }
    }

    @Override
    public void setBigInteger(int parameterIndex, BigInteger x) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            ((PreparedQuery<?>) this.query).getQueryBindings().setBigInteger(getCoreParameterIndex(parameterIndex), x);
        }
    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader value) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            ((PreparedQuery<?>) this.query).getQueryBindings().setNCharacterStream(getCoreParameterIndex(parameterIndex), value);
        }
    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader reader, long length) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            ((PreparedQuery<?>) this.query).getQueryBindings().setNCharacterStream(getCoreParameterIndex(parameterIndex), reader, length);
        }
    }

    @Override
    public void setNClob(int parameterIndex, Reader reader) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            ((PreparedQuery<?>) this.query).getQueryBindings().setNClob(getCoreParameterIndex(parameterIndex), reader);
        }
    }

    @Override
    public void setNClob(int parameterIndex, Reader reader, long length) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            ((PreparedQuery<?>) this.query).getQueryBindings().setNClob(getCoreParameterIndex(parameterIndex), reader, length);
        }
    }

    @Override
    public void setNClob(int parameterIndex, NClob value) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            ((PreparedQuery<?>) this.query).getQueryBindings().setNClob(getCoreParameterIndex(parameterIndex), value);
        }
    }

    /**
     * Set a parameter to a Java String value. The driver converts this to a SQL
     * VARCHAR or LONGVARCHAR value with introducer _utf8 (depending on the
     * arguments size relative to the driver's limits on VARCHARs) when it sends
     * it to the database. If charset is set as utf8, this method just call setString.
     * 
     * @param parameterIndex
     *            the first parameter is 1...
     * @param x
     *            the parameter value
     * 
     * @exception SQLException
     *                if a database access error occurs
     */
    @Override
    public void setNString(int parameterIndex, String x) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            ((PreparedQuery<?>) this.query).getQueryBindings().setNString(getCoreParameterIndex(parameterIndex), x);
        }
    }

    @Override
    public void setNull(int parameterIndex, int sqlType) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            ((PreparedQuery<?>) this.query).getQueryBindings().setNull(getCoreParameterIndex(parameterIndex)); // MySQL ignores sqlType
        }
    }

    @Override
    public void setNull(int parameterIndex, int sqlType, String typeName) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            ((PreparedQuery<?>) this.query).getQueryBindings().setNull(getCoreParameterIndex(parameterIndex));
        }
    }

    @Override
    public void setNull(int parameterIndex, MysqlType mysqlType) throws SQLException {
        setNull(parameterIndex, mysqlType.getJdbcType());
    }

    @Override
    public void setObject(int parameterIndex, Object parameterObj) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            ((PreparedQuery<?>) this.query).getQueryBindings().setObject(getCoreParameterIndex(parameterIndex), parameterObj);
        }
    }

    @Override
    public void setObject(int parameterIndex, Object parameterObj, int targetSqlType) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            try {
                ((PreparedQuery<?>) this.query).getQueryBindings().setObject(getCoreParameterIndex(parameterIndex), parameterObj,
                        MysqlType.getByJdbcType(targetSqlType));
            } catch (FeatureNotAvailableException nae) {
                throw SQLError.createSQLFeatureNotSupportedException(Messages.getString("Statement.UnsupportedSQLType") + JDBCType.valueOf(targetSqlType),
                        MysqlErrorNumbers.SQL_STATE_DRIVER_NOT_CAPABLE, this.exceptionInterceptor);
            }
        }
    }

    @Override
    public void setObject(int parameterIndex, Object parameterObj, SQLType targetSqlType) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            if (targetSqlType instanceof MysqlType) {
                ((PreparedQuery<?>) this.query).getQueryBindings().setObject(getCoreParameterIndex(parameterIndex), parameterObj, (MysqlType) targetSqlType);
            } else {
                setObject(parameterIndex, parameterObj, targetSqlType.getVendorTypeNumber());
            }
        }
    }

    @Override
    public void setObject(int parameterIndex, Object parameterObj, int targetSqlType, int scale) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            try {
                ((PreparedQuery<?>) this.query).getQueryBindings().setObject(getCoreParameterIndex(parameterIndex), parameterObj,
                        MysqlType.getByJdbcType(targetSqlType), scale);
            } catch (FeatureNotAvailableException nae) {
                throw SQLError.createSQLFeatureNotSupportedException(Messages.getString("Statement.UnsupportedSQLType") + JDBCType.valueOf(targetSqlType),
                        MysqlErrorNumbers.SQL_STATE_DRIVER_NOT_CAPABLE, this.exceptionInterceptor);
            }
        }
    }

    @Override
    public void setObject(int parameterIndex, Object x, SQLType targetSqlType, int scaleOrLength) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            if (targetSqlType instanceof MysqlType) {
                ((PreparedQuery<?>) this.query).getQueryBindings().setObject(getCoreParameterIndex(parameterIndex), x, (MysqlType) targetSqlType,
                        scaleOrLength);
            } else {
                setObject(parameterIndex, x, targetSqlType.getVendorTypeNumber(), scaleOrLength);
            }
        }
    }

    @Override
    public void setRef(int i, Ref x) throws SQLException {
        throw SQLError.createSQLFeatureNotSupportedException();
    }

    @Override
    public void setRowId(int parameterIndex, RowId x) throws SQLException {
        throw SQLError.createSQLFeatureNotSupportedException();
    }

    @Override
    public void setShort(int parameterIndex, short x) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            ((PreparedQuery<?>) this.query).getQueryBindings().setShort(getCoreParameterIndex(parameterIndex), x);
        }
    }

    @Override
    public void setSQLXML(int parameterIndex, SQLXML xmlObject) throws SQLException {
        if (xmlObject == null) {
            setNull(parameterIndex, MysqlType.VARCHAR);
        } else {
            // FIXME: Won't work for Non-MYSQL SQLXMLs
            setCharacterStream(parameterIndex, ((MysqlSQLXML) xmlObject).serializeAsCharacterStream());
        }
    }

    @Override
    public void setString(int parameterIndex, String x) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            ((PreparedQuery<?>) this.query).getQueryBindings().setString(getCoreParameterIndex(parameterIndex), x);
        }
    }

    @Override
    public void setTime(int parameterIndex, Time x) throws java.sql.SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            ((PreparedQuery<?>) this.query).getQueryBindings().setTime(getCoreParameterIndex(parameterIndex), x);
        }
    }

    @Override
    public void setTime(int parameterIndex, java.sql.Time x, Calendar cal) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            ((PreparedQuery<?>) this.query).getQueryBindings().setTime(getCoreParameterIndex(parameterIndex), x, cal);
        }
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x) throws java.sql.SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            ((PreparedQuery<?>) this.query).getQueryBindings().setTimestamp(getCoreParameterIndex(parameterIndex), x, MysqlType.TIMESTAMP);
        }
    }

    @Override
    public void setTimestamp(int parameterIndex, java.sql.Timestamp x, Calendar cal) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            ((PreparedQuery<?>) this.query).getQueryBindings().setTimestamp(getCoreParameterIndex(parameterIndex), x, cal, MysqlType.TIMESTAMP);
        }
    }

    public void setTimestamp(int parameterIndex, Timestamp x, Calendar targetCalendar, int fractionalLength) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            ((PreparedQuery<?>) this.query).getQueryBindings().setTimestamp(getCoreParameterIndex(parameterIndex), x, targetCalendar, fractionalLength,
                    MysqlType.TIMESTAMP);
        }
    }

    @Deprecated
    @Override
    public void setUnicodeStream(int parameterIndex, InputStream x, int length) throws SQLException {
        setBinaryStream(parameterIndex, x, length);
        ((PreparedQuery<?>) this.query).getQueryBindings().getBindValues()[getCoreParameterIndex(parameterIndex)].setMysqlType(MysqlType.TEXT); // TODO was Types.CLOB
    }

    @Override
    public void setURL(int parameterIndex, URL arg) throws SQLException {
        if (arg == null) {
            setNull(parameterIndex, MysqlType.VARCHAR);
        } else {
            setString(parameterIndex, arg.toString());
            ((PreparedQuery<?>) this.query).getQueryBindings().getBindValues()[getCoreParameterIndex(parameterIndex)].setMysqlType(MysqlType.VARCHAR); // TODO was Types.DATALINK
        }
    }
}
