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

package com.mysql.jdbc;

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
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.TimeZone;

import com.mysql.cj.api.ProfilerEvent;
import com.mysql.cj.core.Messages;
import com.mysql.cj.core.exception.CJException;
import com.mysql.cj.core.exception.ExceptionFactory;
import com.mysql.cj.core.io.Buffer;
import com.mysql.cj.core.profiler.ProfilerEventHandlerFactory;
import com.mysql.cj.core.profiler.ProfilerEventImpl;
import com.mysql.cj.core.util.LogUtils;
import com.mysql.cj.core.util.StringUtils;
import com.mysql.cj.core.util.TestUtils;
import com.mysql.jdbc.exceptions.MySQLStatementCancelledException;
import com.mysql.jdbc.exceptions.MySQLTimeoutException;
import com.mysql.jdbc.exceptions.SQLError;

/**
 * JDBC Interface for MySQL-4.1 and newer server-side PreparedStatements.
 */
public class ServerPreparedStatement extends PreparedStatement {

    protected static final int BLOB_STREAM_READ_BUF_SIZE = 8192;

    public static class BatchedBindValues {
        public BindValue[] batchedParameterValues;

        BatchedBindValues(BindValue[] paramVals) {
            int numParams = paramVals.length;

            this.batchedParameterValues = new BindValue[numParams];

            for (int i = 0; i < numParams; i++) {
                this.batchedParameterValues[i] = new BindValue(paramVals[i]);
            }
        }
    }

    public static class BindValue {

        public long boundBeforeExecutionNum = 0;

        public long bindLength; /* Default length of data */

        public int bufferType; /* buffer type */

        public double doubleBinding;

        public float floatBinding;

        public boolean isLongData; /* long data indicator */

        public boolean isNull; /* NULL indicator */

        public boolean isSet = false; /* has this parameter been set? */

        public long longBinding; /* all integral values are stored here */

        public Object value; /* The value to store */

        public TimeZone tz; /* The TimeZone for date/time types */

        BindValue() {
        }

        BindValue(BindValue copyMe) {
            this.value = copyMe.value;
            this.isSet = copyMe.isSet;
            this.isLongData = copyMe.isLongData;
            this.isNull = copyMe.isNull;
            this.bufferType = copyMe.bufferType;
            this.bindLength = copyMe.bindLength;
            this.longBinding = copyMe.longBinding;
            this.floatBinding = copyMe.floatBinding;
            this.doubleBinding = copyMe.doubleBinding;
            this.tz = copyMe.tz;
        }

        void reset() {
            this.isSet = false;
            this.value = null;
            this.isLongData = false;

            this.longBinding = 0L;
            this.floatBinding = 0;
            this.doubleBinding = 0D;
            this.tz = null;
        }

        @Override
        public String toString() {
            return toString(false);
        }

        public String toString(boolean quoteIfNeeded) {
            if (this.isLongData) {
                return "' STREAM DATA '";
            }

            switch (this.bufferType) {
                case MysqlDefs.FIELD_TYPE_TINY:
                case MysqlDefs.FIELD_TYPE_SHORT:
                case MysqlDefs.FIELD_TYPE_LONG:
                case MysqlDefs.FIELD_TYPE_LONGLONG:
                    return String.valueOf(this.longBinding);
                case MysqlDefs.FIELD_TYPE_FLOAT:
                    return String.valueOf(this.floatBinding);
                case MysqlDefs.FIELD_TYPE_DOUBLE:
                    return String.valueOf(this.doubleBinding);
                case MysqlDefs.FIELD_TYPE_TIME:
                case MysqlDefs.FIELD_TYPE_DATE:
                case MysqlDefs.FIELD_TYPE_DATETIME:
                case MysqlDefs.FIELD_TYPE_TIMESTAMP:
                case MysqlDefs.FIELD_TYPE_VAR_STRING:
                case MysqlDefs.FIELD_TYPE_STRING:
                case MysqlDefs.FIELD_TYPE_VARCHAR:
                    if (quoteIfNeeded) {
                        return "'" + String.valueOf(this.value) + "'";
                    }
                    return String.valueOf(this.value);

                default:
                    if (this.value instanceof byte[]) {
                        return "byte data";
                    }
                    if (quoteIfNeeded) {
                        return "'" + String.valueOf(this.value) + "'";
                    }
                    return String.valueOf(this.value);
            }
        }

        long getBoundLength() {
            if (this.isNull) {
                return 0;
            }

            if (this.isLongData) {
                return this.bindLength;
            }

            switch (this.bufferType) {

                case MysqlDefs.FIELD_TYPE_TINY:
                    return 1;
                case MysqlDefs.FIELD_TYPE_SHORT:
                    return 2;
                case MysqlDefs.FIELD_TYPE_LONG:
                    return 4;
                case MysqlDefs.FIELD_TYPE_LONGLONG:
                    return 8;
                case MysqlDefs.FIELD_TYPE_FLOAT:
                    return 4;
                case MysqlDefs.FIELD_TYPE_DOUBLE:
                    return 8;
                case MysqlDefs.FIELD_TYPE_TIME:
                    return 9;
                case MysqlDefs.FIELD_TYPE_DATE:
                    return 7;
                case MysqlDefs.FIELD_TYPE_DATETIME:
                case MysqlDefs.FIELD_TYPE_TIMESTAMP:
                    return 11;
                case MysqlDefs.FIELD_TYPE_VAR_STRING:
                case MysqlDefs.FIELD_TYPE_STRING:
                case MysqlDefs.FIELD_TYPE_VARCHAR:
                case MysqlDefs.FIELD_TYPE_DECIMAL:
                case MysqlDefs.FIELD_TYPE_NEW_DECIMAL:
                    if (this.value instanceof byte[]) {
                        return ((byte[]) this.value).length;
                    }
                    return ((String) this.value).length();

                default:
                    return 0;
            }
        }
    }

    private boolean hasOnDuplicateKeyUpdate = false;

    private void storeTime(Buffer intoBuf, Time tm, TimeZone tz) throws SQLException {

        intoBuf.ensureCapacity(9);
        intoBuf.writeByte((byte) 8); // length
        intoBuf.writeByte((byte) 0); // neg flag
        intoBuf.writeLong(0); // tm->day, not used

        Calendar cal = Calendar.getInstance(tz);

        cal.setTime(tm);
        intoBuf.writeByte((byte) cal.get(Calendar.HOUR_OF_DAY));
        intoBuf.writeByte((byte) cal.get(Calendar.MINUTE));
        intoBuf.writeByte((byte) cal.get(Calendar.SECOND));
    }

    /**
     * Flag indicating whether or not the long parameters have been 'switched'
     * back to normal parameters. We can not execute() if clearParameters()
     * hasn't been called in this case.
     */
    private boolean detectedLongParameterSwitch = false;

    /**
     * The number of fields in the result set (if any) for this
     * PreparedStatement.
     */
    private int fieldCount;

    /** Has this prepared statement been marked invalid? */
    private boolean invalid = false;

    /** If this statement has been marked invalid, what was the reason? */
    private CJException invalidationException;

    private Buffer outByteBuffer;

    /** Bind values for individual fields */
    private BindValue[] parameterBindings;

    /** Field-level metadata for parameters */
    private Field[] parameterFields;

    /** Field-level metadata for result sets. */
    private Field[] resultFields;

    /** Do we need to send/resend types to the server? */
    private boolean sendTypesToServer = false;

    /** The ID that the server uses to identify this PreparedStatement */
    private long serverStatementId;

    /**
     * Creates a prepared statement instance
     */

    protected static ServerPreparedStatement getInstance(MysqlJdbcConnection conn, String sql, String catalog, int resultSetType, int resultSetConcurrency)
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
    protected ServerPreparedStatement(MysqlJdbcConnection conn, String sql, String catalog, int resultSetType, int resultSetConcurrency) throws SQLException {
        super(conn, catalog);

        checkNullOrEmptyQuery(sql);

        int startOfStatement = findStartOfStatement(sql);

        this.firstCharOfStmt = StringUtils.firstAlphaCharUc(sql, startOfStatement);

        this.hasOnDuplicateKeyUpdate = this.firstCharOfStmt == 'I' && containsOnDuplicateKeyInString(sql);

        this.useAutoSlowLog = this.connection.getAutoSlowLog();

        String statementComment = this.connection.getStatementComment();

        this.originalSql = (statementComment == null) ? sql : "/* " + statementComment + " */ " + sql;

        try {
            serverPrepare(sql);
        } catch (SQLException sqlEx) {
            realClose(false, true);
            // don't wrap SQLExceptions
            throw sqlEx;
        } catch (CJException ex) {
            realClose(false, true);

            SQLException sqlEx = SQLError.createSQLException(ex.toString(), SQLError.SQL_STATE_GENERAL_ERROR, getExceptionInterceptor());
            sqlEx.initCause(ex);

            throw sqlEx;
        }

        setResultSetType(resultSetType);
        setResultSetConcurrency(resultSetConcurrency);

        this.parameterTypes = new int[this.parameterCount];
    }

    /**
     * JDBC 2.0 Add a set of parameters to the batch.
     * 
     * @exception SQLException
     *                if a database-access error occurs.
     * 
     * @see StatementImpl#addBatch
     */
    @Override
    public void addBatch() throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {

            if (this.batchedArgs == null) {
                this.batchedArgs = new ArrayList<Object>();
            }

            this.batchedArgs.add(new BatchedBindValues(this.parameterBindings));
        }
    }

    @Override
    public String asSql(boolean quoteStreamsAndUnknowns) throws SQLException {

        synchronized (checkClosed().getConnectionMutex()) {

            PreparedStatement pStmtForSub = null;

            try {
                pStmtForSub = PreparedStatement.getInstance(this.connection, this.originalSql, this.currentCatalog);

                int numParameters = pStmtForSub.parameterCount;
                int ourNumParameters = this.parameterCount;

                for (int i = 0; (i < numParameters) && (i < ourNumParameters); i++) {
                    if (this.parameterBindings[i] != null) {
                        if (this.parameterBindings[i].isNull) {
                            pStmtForSub.setNull(i + 1, Types.NULL);
                        } else {
                            BindValue bindValue = this.parameterBindings[i];

                            //
                            // Handle primitives first
                            //
                            switch (bindValue.bufferType) {

                                case MysqlDefs.FIELD_TYPE_TINY:
                                    pStmtForSub.setByte(i + 1, (byte) bindValue.longBinding);
                                    break;
                                case MysqlDefs.FIELD_TYPE_SHORT:
                                    pStmtForSub.setShort(i + 1, (short) bindValue.longBinding);
                                    break;
                                case MysqlDefs.FIELD_TYPE_LONG:
                                    pStmtForSub.setInt(i + 1, (int) bindValue.longBinding);
                                    break;
                                case MysqlDefs.FIELD_TYPE_LONGLONG:
                                    pStmtForSub.setLong(i + 1, bindValue.longBinding);
                                    break;
                                case MysqlDefs.FIELD_TYPE_FLOAT:
                                    pStmtForSub.setFloat(i + 1, bindValue.floatBinding);
                                    break;
                                case MysqlDefs.FIELD_TYPE_DOUBLE:
                                    pStmtForSub.setDouble(i + 1, bindValue.doubleBinding);
                                    break;
                                default:
                                    pStmtForSub.setObject(i + 1, this.parameterBindings[i].value);
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
    protected MysqlJdbcConnection checkClosed() {
        if (this.invalid) {
            throw this.invalidationException;
        }

        return super.checkClosed();
    }

    /**
     * @see java.sql.PreparedStatement#clearParameters()
     */
    @Override
    public void clearParameters() throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            clearParametersInternal(true);
        }
    }

    private void clearParametersInternal(boolean clearServerParameters) throws SQLException {
        boolean hadLongData = false;

        if (this.parameterBindings != null) {
            for (int i = 0; i < this.parameterCount; i++) {
                if ((this.parameterBindings[i] != null) && this.parameterBindings[i].isLongData) {
                    hadLongData = true;
                }

                this.parameterBindings[i].reset();
            }
        }

        if (clearServerParameters && hadLongData) {
            serverResetStatement();

            this.detectedLongParameterSwitch = false;
        }
    }

    protected boolean isCached = false;

    private boolean useAutoSlowLog;

    protected void setClosed(boolean flag) {
        this.isClosed = flag;
    }

    /**
     * @see java.sql.Statement#close()
     */
    @Override
    public void close() throws SQLException {
        MysqlJdbcConnection locallyScopedConn = this.connection;

        if (locallyScopedConn == null) {
            return; // already closed
        }

        synchronized (locallyScopedConn.getConnectionMutex()) {

            if (this.isCached && !this.isClosed) {
                clearParameters();

                this.isClosed = true;

                this.connection.recachePreparedStatement(this);
                return;
            }

            realClose(true, true);
        }
    }

    private void dumpCloseForTestcase() throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            StringBuilder buf = new StringBuilder();
            this.connection.generateConnectionCommentBlock(buf);
            buf.append("DEALLOCATE PREPARE debug_stmt_");
            buf.append(this.statementId);
            buf.append(";\n");

            TestUtils.dumpTestcaseQuery(buf.toString());
        }
    }

    private void dumpExecuteForTestcase() throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            StringBuilder buf = new StringBuilder();

            for (int i = 0; i < this.parameterCount; i++) {
                this.connection.generateConnectionCommentBlock(buf);

                buf.append("SET @debug_stmt_param");
                buf.append(this.statementId);
                buf.append("_");
                buf.append(i);
                buf.append("=");

                if (this.parameterBindings[i].isNull) {
                    buf.append("NULL");
                } else {
                    buf.append(this.parameterBindings[i].toString(true));
                }

                buf.append(";\n");
            }

            this.connection.generateConnectionCommentBlock(buf);

            buf.append("EXECUTE debug_stmt_");
            buf.append(this.statementId);

            if (this.parameterCount > 0) {
                buf.append(" USING ");
                for (int i = 0; i < this.parameterCount; i++) {
                    if (i > 0) {
                        buf.append(", ");
                    }

                    buf.append("@debug_stmt_param");
                    buf.append(this.statementId);
                    buf.append("_");
                    buf.append(i);

                }
            }

            buf.append(";\n");

            TestUtils.dumpTestcaseQuery(buf.toString());
        }
    }

    private void dumpPrepareForTestcase() throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            StringBuilder buf = new StringBuilder(this.originalSql.length() + 64);

            this.connection.generateConnectionCommentBlock(buf);

            buf.append("PREPARE debug_stmt_");
            buf.append(this.statementId);
            buf.append(" FROM \"");
            buf.append(this.originalSql);
            buf.append("\";\n");

            TestUtils.dumpTestcaseQuery(buf.toString());
        }
    }

    @Override
    protected int[] executeBatchSerially(int batchTimeout) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            MysqlJdbcConnection locallyScopedConn = this.connection;

            if (locallyScopedConn.isReadOnly()) {
                throw SQLError.createSQLException(Messages.getString("ServerPreparedStatement.2") + Messages.getString("ServerPreparedStatement.3"),
                        SQLError.SQL_STATE_ILLEGAL_ARGUMENT, getExceptionInterceptor());
            }

            clearWarnings();

            // Store this for later, we're going to 'swap' them out
            // as we execute each batched statement...
            BindValue[] oldBindValues = this.parameterBindings;

            try {
                int[] updateCounts = null;

                if (this.batchedArgs != null) {
                    int nbrCommands = this.batchedArgs.size();
                    updateCounts = new int[nbrCommands];

                    if (this.retrieveGeneratedKeys) {
                        this.batchedGeneratedKeys = new ArrayList<ResultSetRow>(nbrCommands);
                    }

                    for (int i = 0; i < nbrCommands; i++) {
                        updateCounts[i] = -3;
                    }

                    SQLException sqlEx = null;

                    int commandIndex = 0;

                    BindValue[] previousBindValuesForBatch = null;

                    CancelTask timeoutTask = null;

                    try {
                        if (locallyScopedConn.getEnableQueryTimeouts() && batchTimeout != 0) {
                            timeoutTask = new CancelTask(this);
                            locallyScopedConn.getCancelTimer().schedule(timeoutTask, batchTimeout);
                        }

                        for (commandIndex = 0; commandIndex < nbrCommands; commandIndex++) {
                            Object arg = this.batchedArgs.get(commandIndex);

                            if (arg instanceof String) {
                                updateCounts[commandIndex] = executeUpdate((String) arg);
                            } else {
                                this.parameterBindings = ((BatchedBindValues) arg).batchedParameterValues;

                                try {
                                    // We need to check types each time, as
                                    // the user might have bound different
                                    // types in each addBatch()

                                    if (previousBindValuesForBatch != null) {
                                        for (int j = 0; j < this.parameterBindings.length; j++) {
                                            if (this.parameterBindings[j].bufferType != previousBindValuesForBatch[j].bufferType) {
                                                this.sendTypesToServer = true;

                                                break;
                                            }
                                        }
                                    }

                                    try {
                                        updateCounts[commandIndex] = executeUpdate(false, true);
                                    } finally {
                                        previousBindValuesForBatch = this.parameterBindings;
                                    }

                                    if (this.retrieveGeneratedKeys) {
                                        java.sql.ResultSet rs = null;

                                        try {
                                            // we don't want to use our version, because we've altered the behavior of ours to support batch updates
                                            // (catch-22) Ideally, what we need here is super.super.getGeneratedKeys() but that construct doesn't exist in
                                            // Java, so that's why there's this kludge.
                                            rs = getGeneratedKeysInternal();

                                            while (rs.next()) {
                                                this.batchedGeneratedKeys.add(new ByteArrayRow(new byte[][] { rs.getBytes(1) }, getExceptionInterceptor()));
                                            }
                                        } finally {
                                            if (rs != null) {
                                                rs.close();
                                            }
                                        }
                                    }
                                } catch (SQLException ex) {
                                    updateCounts[commandIndex] = EXECUTE_FAILED;

                                    if (this.continueBatchOnError && !(ex instanceof MySQLTimeoutException)
                                            && !(ex instanceof MySQLStatementCancelledException) && !hasDeadlockOrTimeoutRolledBackTx(ex)) {
                                        sqlEx = ex;
                                    } else {
                                        int[] newUpdateCounts = new int[commandIndex];
                                        System.arraycopy(updateCounts, 0, newUpdateCounts, 0, commandIndex);

                                        throw new java.sql.BatchUpdateException(ex.getMessage(), ex.getSQLState(), ex.getErrorCode(), newUpdateCounts);
                                    }
                                }
                            }
                        }
                    } finally {
                        if (timeoutTask != null) {
                            timeoutTask.cancel();

                            locallyScopedConn.getCancelTimer().purge();
                        }

                        resetCancelledState();
                    }

                    if (sqlEx != null) {
                        throw new java.sql.BatchUpdateException(sqlEx.getMessage(), sqlEx.getSQLState(), sqlEx.getErrorCode(), updateCounts);
                    }
                }

                return (updateCounts != null) ? updateCounts : new int[0];
            } finally {
                this.parameterBindings = oldBindValues;
                this.sendTypesToServer = true;

                clearBatch();
            }
        }
    }

    @Override
    protected com.mysql.jdbc.ResultSetInternalMethods executeInternal(int maxRowsToRetrieve, Buffer sendPacket, boolean createStreamingResultSet,
            boolean queryIsSelectOnly, Field[] metadataFromCache, boolean isBatch) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            this.numberOfExecutions++;

            // We defer to server-side execution
            try {
                return serverExecute(maxRowsToRetrieve, createStreamingResultSet, metadataFromCache);
            } catch (SQLException sqlEx) {
                // don't wrap SQLExceptions
                if (this.connection.getEnablePacketDebug()) {
                    this.connection.getIO().dumpPacketRingBuffer();
                }

                if (this.connection.getDumpQueriesOnException()) {
                    String extractedSql = toString();
                    StringBuilder messageBuf = new StringBuilder(extractedSql.length() + 32);
                    messageBuf.append("\n\nQuery being executed when exception was thrown:\n");
                    messageBuf.append(extractedSql);
                    messageBuf.append("\n\n");

                    sqlEx = ConnectionImpl.appendMessageToException(sqlEx, messageBuf.toString(), getExceptionInterceptor());
                }

                throw sqlEx;
            } catch (Exception ex) {
                if (this.connection.getEnablePacketDebug()) {
                    this.connection.getIO().dumpPacketRingBuffer();
                }

                SQLException sqlEx = SQLError.createSQLException(ex.toString(), SQLError.SQL_STATE_GENERAL_ERROR, ex, getExceptionInterceptor());

                if (this.connection.getDumpQueriesOnException()) {
                    String extractedSql = toString();
                    StringBuilder messageBuf = new StringBuilder(extractedSql.length() + 32);
                    messageBuf.append("\n\nQuery being executed when exception was thrown:\n");
                    messageBuf.append(extractedSql);
                    messageBuf.append("\n\n");

                    sqlEx = ConnectionImpl.appendMessageToException(sqlEx, messageBuf.toString(), getExceptionInterceptor());
                }

                throw sqlEx;
            }
        }
    }

    /**
     * @see com.mysql.jdbc.PreparedStatement#fillSendPacket()
     */
    @Override
    protected Buffer fillSendPacket() throws SQLException {
        return null; // we don't use this type of packet
    }

    /**
     * @see com.mysql.jdbc.PreparedStatement#fillSendPacket(byte, java.io.InputStream, boolean, int)
     */
    @Override
    protected Buffer fillSendPacket(byte[][] batchedParameterStrings, InputStream[] batchedParameterStreams, boolean[] batchedIsStream,
            int[] batchedStreamLengths) throws SQLException {
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
    protected BindValue getBinding(int parameterIndex, boolean forLongData) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {

            if (this.parameterBindings.length == 0) {
                throw SQLError.createSQLException(Messages.getString("ServerPreparedStatement.8"), SQLError.SQL_STATE_ILLEGAL_ARGUMENT,
                        getExceptionInterceptor());
            }

            parameterIndex--;

            if ((parameterIndex < 0) || (parameterIndex >= this.parameterBindings.length)) {
                throw SQLError.createSQLException(
                        Messages.getString("ServerPreparedStatement.9") + (parameterIndex + 1) + Messages.getString("ServerPreparedStatement.10")
                                + this.parameterBindings.length, SQLError.SQL_STATE_ILLEGAL_ARGUMENT, getExceptionInterceptor());
            }

            if (this.parameterBindings[parameterIndex] == null) {
                this.parameterBindings[parameterIndex] = new BindValue();
            } else {
                if (this.parameterBindings[parameterIndex].isLongData && !forLongData) {
                    this.detectedLongParameterSwitch = true;
                }
            }

            this.parameterBindings[parameterIndex].isSet = true;
            this.parameterBindings[parameterIndex].boundBeforeExecutionNum = this.numberOfExecutions;

            return this.parameterBindings[parameterIndex];
        }
    }

    /**
     * Return current bind values for use by Statement Interceptors.
     * 
     * @return the bind values as set by setXXX and stored by addBatch
     * @see #executeBatch()
     * @see #addBatch()
     */
    public BindValue[] getParameterBindValues() {
        return this.parameterBindings;
    }

    /**
     * @see com.mysql.jdbc.PreparedStatement#getBytes(int)
     */
    byte[] getBytes(int parameterIndex) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            BindValue bindValue = getBinding(parameterIndex, false);

            if (bindValue.isNull) {
                return null;
            } else if (bindValue.isLongData) {
                throw SQLError.notImplemented();
            } else {
                if (this.outByteBuffer == null) {
                    this.outByteBuffer = new Buffer(this.connection.getNetBufferLength());
                }

                this.outByteBuffer.clear();

                int originalPosition = this.outByteBuffer.getPosition();

                storeBinding(this.outByteBuffer, bindValue, this.connection.getIO());

                int newPosition = this.outByteBuffer.getPosition();

                int length = newPosition - originalPosition;

                byte[] valueAsBytes = new byte[length];

                System.arraycopy(this.outByteBuffer.getByteBuffer(), originalPosition, valueAsBytes, 0, length);

                return valueAsBytes;
            }
        }
    }

    /**
     * @see java.sql.PreparedStatement#getMetaData()
     */
    @Override
    public java.sql.ResultSetMetaData getMetaData() throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {

            if (this.resultFields == null) {
                return null;
            }

            return new ResultSetMetaData(this.resultFields, this.connection.getUseOldAliasMetadataBehavior(), this.connection.getYearIsDateType(),
                    getExceptionInterceptor());
        }
    }

    /**
     * @see java.sql.PreparedStatement#getParameterMetaData()
     */
    @Override
    public ParameterMetaData getParameterMetaData() throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {

            if (this.parameterMetaData == null) {
                this.parameterMetaData = new MysqlParameterMetadata(this.parameterFields, this.parameterCount, getExceptionInterceptor());
            }

            return this.parameterMetaData;
        }
    }

    /**
     * @see com.mysql.jdbc.PreparedStatement#isNull(int)
     */
    @Override
    boolean isNull(int paramIndex) {
        throw new IllegalArgumentException(Messages.getString("ServerPreparedStatement.7"));
    }

    /**
     * Closes this connection and frees all resources.
     * 
     * @param calledExplicitly
     *            was this called from close()?
     * 
     * @throws SQLException
     *             if an error occurs
     */
    @Override
    protected void realClose(boolean calledExplicitly, boolean closeOpenResults) throws SQLException {
        MysqlJdbcConnection locallyScopedConn = this.connection;

        if (locallyScopedConn == null) {
            return; // already closed
        }

        synchronized (locallyScopedConn.getConnectionMutex()) {

            if (this.connection != null) {
                if (this.connection.getAutoGenerateTestcaseScript()) {
                    dumpCloseForTestcase();
                }

                //
                // Don't communicate with the server if we're being called from the finalizer...
                // 
                // This will leak server resources, but if we don't do this, we'll deadlock (potentially, because there's no guarantee when, what order, and
                // what concurrency finalizers will be called with). Well-behaved programs won't rely on finalizers to clean up their statements.
                //

                SQLException exceptionDuringClose = null;

                if (calledExplicitly && !this.connection.isClosed()) {
                    synchronized (this.connection.getConnectionMutex()) {
                        try {

                            MysqlIO mysql = this.connection.getIO();

                            Buffer packet = mysql.getSharedSendPacket();

                            packet.writeByte((byte) MysqlDefs.COM_CLOSE_STATEMENT);
                            packet.writeLong(this.serverStatementId);

                            mysql.sendCommand(MysqlDefs.COM_CLOSE_STATEMENT, null, packet, true, null, 0);
                        } catch (SQLException sqlEx) {
                            exceptionDuringClose = sqlEx;
                        }
                    }
                }

                if (this.isCached) {
                    this.connection.decachePreparedStatement(this);
                }
                super.realClose(calledExplicitly, closeOpenResults);

                clearParametersInternal(false);
                this.parameterBindings = null;

                this.parameterFields = null;
                this.resultFields = null;

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
                serverPrepare(this.originalSql);
            } catch (SQLException sqlEx) {
                this.invalidationException = ExceptionFactory.createException(sqlEx.getMessage(), sqlEx);
            } catch (Exception ex) {
                this.invalidationException = ExceptionFactory.createException(ex.getMessage(), ex);
            }

            if (this.invalidationException != null) {
                this.invalid = true;

                this.parameterBindings = null;

                this.parameterFields = null;
                this.resultFields = null;

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

                if (this.connection != null) {
                    if (!this.connection.getDontTrackOpenResources()) {
                        this.connection.unregisterStatement(this);
                    }
                }
            }
        }
    }

    /**
     * Tells the server to execute this prepared statement with the current
     * parameter bindings.
     * 
     * <pre>
     * 
     * 
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
     * 
     * 
     * </pre>
     * 
     * @param maxRowsToRetrieve
     * @param createStreamingResultSet
     * 
     * @throws SQLException
     */
    private com.mysql.jdbc.ResultSetInternalMethods serverExecute(int maxRowsToRetrieve, boolean createStreamingResultSet, Field[] metadataFromCache)
            throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            MysqlIO mysql = this.connection.getIO();

            if (mysql.shouldIntercept()) {
                ResultSetInternalMethods interceptedResults = mysql.invokeStatementInterceptorsPre(this.originalSql, this, true);

                if (interceptedResults != null) {
                    return interceptedResults;
                }
            }

            if (this.detectedLongParameterSwitch) {
                // Check when values were bound
                boolean firstFound = false;
                long boundTimeToCheck = 0;

                for (int i = 0; i < this.parameterCount - 1; i++) {
                    if (this.parameterBindings[i].isLongData) {
                        if (firstFound && boundTimeToCheck != this.parameterBindings[i].boundBeforeExecutionNum) {
                            throw SQLError.createSQLException(
                                    Messages.getString("ServerPreparedStatement.11") + Messages.getString("ServerPreparedStatement.12"),
                                    SQLError.SQL_STATE_DRIVER_NOT_CAPABLE, getExceptionInterceptor());
                        }
                        firstFound = true;
                        boundTimeToCheck = this.parameterBindings[i].boundBeforeExecutionNum;
                    }
                }

                // Okay, we've got all "newly"-bound streams, so reset server-side state to clear out previous bindings

                serverResetStatement();
            }

            // Check bindings
            for (int i = 0; i < this.parameterCount; i++) {
                if (!this.parameterBindings[i].isSet) {
                    throw SQLError.createSQLException(
                            Messages.getString("ServerPreparedStatement.13") + (i + 1) + Messages.getString("ServerPreparedStatement.14"),
                            SQLError.SQL_STATE_ILLEGAL_ARGUMENT, getExceptionInterceptor());
                }
            }

            //
            // Send all long data
            //
            for (int i = 0; i < this.parameterCount; i++) {
                if (this.parameterBindings[i].isLongData) {
                    serverLongData(i, this.parameterBindings[i]);
                }
            }

            if (this.connection.getAutoGenerateTestcaseScript()) {
                dumpExecuteForTestcase();
            }

            //
            // store the parameter values
            //

            Buffer packet = mysql.getSharedSendPacket();
            packet.writeByte((byte) MysqlDefs.COM_EXECUTE);
            packet.writeLong(this.serverStatementId);

            //			boolean usingCursor = false;

            // we only create cursor-backed result sets if
            // a) The query is a SELECT
            // b) The server supports it
            // c) We know it is forward-only (note this doesn't preclude updatable result sets)
            // d) The user has set a fetch size
            if (this.resultFields != null && this.connection.getUseCursorFetch() && getResultSetType() == ResultSet.TYPE_FORWARD_ONLY
                    && getResultSetConcurrency() == ResultSet.CONCUR_READ_ONLY && getFetchSize() > 0) {
                packet.writeByte(MysqlDefs.OPEN_CURSOR_FLAG);
                //                  usingCursor = true;
            } else {
                packet.writeByte((byte) 0); // placeholder for flags
            }

            packet.writeLong(1); // placeholder for parameter iterations

            /* Reserve place for null-marker bytes */
            int nullCount = (this.parameterCount + 7) / 8;

            int nullBitsPosition = packet.getPosition();

            for (int i = 0; i < nullCount; i++) {
                packet.writeByte((byte) 0);
            }

            byte[] nullBitsBuffer = new byte[nullCount];

            /* In case if buffers (type) altered, indicate to server */
            packet.writeByte(this.sendTypesToServer ? (byte) 1 : (byte) 0);

            if (this.sendTypesToServer) {
                /*
                 * Store types of parameters in first in first package that is sent to the server.
                 */
                for (int i = 0; i < this.parameterCount; i++) {
                    packet.writeInt(this.parameterBindings[i].bufferType);
                }
            }

            //
            // store the parameter values
            //
            for (int i = 0; i < this.parameterCount; i++) {
                if (!this.parameterBindings[i].isLongData) {
                    if (!this.parameterBindings[i].isNull) {
                        storeBinding(packet, this.parameterBindings[i], mysql);
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
            packet.writeBytesNoNull(nullBitsBuffer);
            packet.setPosition(endPosition);

            long begin = 0;

            boolean logSlowQueries = this.connection.getLogSlowQueries();
            boolean gatherPerformanceMetrics = this.connection.getGatherPerfMetrics();

            if (this.profileSQL || logSlowQueries || gatherPerformanceMetrics) {
                begin = mysql.getCurrentTimeNanosOrMillis();
            }

            resetCancelledState();

            CancelTask timeoutTask = null;

            try {
                if (this.connection.getEnableQueryTimeouts() && this.timeoutInMillis != 0) {
                    timeoutTask = new CancelTask(this);
                    this.connection.getCancelTimer().schedule(timeoutTask, this.timeoutInMillis);
                }

                statementBegins();

                Buffer resultPacket = mysql.sendCommand(MysqlDefs.COM_EXECUTE, null, packet, false, null, 0);

                long queryEndTime = 0L;

                if (logSlowQueries || gatherPerformanceMetrics || this.profileSQL) {
                    queryEndTime = mysql.getCurrentTimeNanosOrMillis();
                }

                if (timeoutTask != null) {
                    timeoutTask.cancel();

                    this.connection.getCancelTimer().purge();

                    if (timeoutTask.caughtWhileCancelling != null) {
                        throw timeoutTask.caughtWhileCancelling;
                    }

                    timeoutTask = null;
                }

                synchronized (this.cancelTimeoutMutex) {
                    if (this.wasCancelled) {
                        SQLException cause = null;

                        if (this.wasCancelledByTimeout) {
                            cause = new MySQLTimeoutException();
                        } else {
                            cause = new MySQLStatementCancelledException();
                        }

                        resetCancelledState();

                        throw cause;
                    }
                }

                boolean queryWasSlow = false;

                if (logSlowQueries || gatherPerformanceMetrics) {
                    long elapsedTime = queryEndTime - begin;

                    if (logSlowQueries) {
                        if (this.useAutoSlowLog) {
                            queryWasSlow = elapsedTime > this.connection.getSlowQueryThresholdMillis();
                        } else {
                            queryWasSlow = this.connection.isAbonormallyLongQuery(elapsedTime);

                            this.connection.reportQueryTime(elapsedTime);
                        }
                    }

                    if (queryWasSlow) {

                        StringBuilder mesgBuf = new StringBuilder(48 + this.originalSql.length());
                        mesgBuf.append(Messages.getString("ServerPreparedStatement.15"));
                        mesgBuf.append(mysql.getSlowQueryThreshold());
                        mesgBuf.append(Messages.getString("ServerPreparedStatement.15a"));
                        mesgBuf.append(elapsedTime);
                        mesgBuf.append(Messages.getString("ServerPreparedStatement.16"));

                        mesgBuf.append("as prepared: ");
                        mesgBuf.append(this.originalSql);
                        mesgBuf.append("\n\n with parameters bound:\n\n");
                        mesgBuf.append(asSql(true));

                        this.eventSink.consumeEvent(new ProfilerEventImpl(ProfilerEvent.TYPE_SLOW_QUERY, "", this.currentCatalog, this.connection.getId(),
                                getId(), 0, System.currentTimeMillis(), elapsedTime, mysql.getQueryTimingUnits(), null, LogUtils
                                        .findCallingClassAndMethod(new Throwable()), mesgBuf.toString()));
                    }

                    if (gatherPerformanceMetrics) {
                        this.connection.registerQueryExecutionTime(elapsedTime);
                    }
                }

                this.connection.incrementNumberOfPreparedExecutes();

                if (this.profileSQL) {
                    this.eventSink = ProfilerEventHandlerFactory.getInstance(this.connection);

                    this.eventSink.consumeEvent(new ProfilerEventImpl(ProfilerEvent.TYPE_EXECUTE, "", this.currentCatalog, this.connectionId, this.statementId,
                            -1, System.currentTimeMillis(), mysql.getCurrentTimeNanosOrMillis() - begin, mysql.getQueryTimingUnits(), null, LogUtils
                                    .findCallingClassAndMethod(new Throwable()), truncateQueryToLog(asSql(true))));
                }

                com.mysql.jdbc.ResultSetInternalMethods rs = mysql.readAllResults(this, maxRowsToRetrieve, this.resultSetType, this.resultSetConcurrency,
                        createStreamingResultSet, this.currentCatalog, resultPacket, true, this.fieldCount, metadataFromCache);

                if (mysql.shouldIntercept()) {
                    ResultSetInternalMethods interceptedResults = mysql.invokeStatementInterceptorsPost(this.originalSql, this, rs, true, null);

                    if (interceptedResults != null) {
                        rs = interceptedResults;
                    }
                }

                if (this.profileSQL) {
                    long fetchEndTime = mysql.getCurrentTimeNanosOrMillis();

                    this.eventSink.consumeEvent(new ProfilerEventImpl(ProfilerEvent.TYPE_FETCH, "", this.currentCatalog, this.connection.getId(), getId(),
                            0 /*
                               * FIXME
                               * rs.
                               * resultId
                               */, System.currentTimeMillis(), (fetchEndTime - queryEndTime), mysql.getQueryTimingUnits(), null, LogUtils
                                    .findCallingClassAndMethod(new Throwable()), null));
                }

                if (queryWasSlow && this.connection.getExplainSlowQueries()) {
                    String queryAsString = asSql(true);

                    mysql.explainSlowQuery(StringUtils.getBytes(queryAsString), queryAsString);
                }

                this.sendTypesToServer = false;
                this.results = rs;

                if (mysql.hadWarnings()) {
                    mysql.scanForAndThrowDataTruncation();
                }

                return rs;
            } catch (SQLException sqlEx) {
                if (mysql.shouldIntercept()) {
                    mysql.invokeStatementInterceptorsPost(this.originalSql, this, null, true, sqlEx);
                }

                throw sqlEx;
            } finally {
                this.statementExecuting.set(false);

                if (timeoutTask != null) {
                    timeoutTask.cancel();
                    this.connection.getCancelTimer().purge();
                }
            }
        }
    }

    /**
     * Sends stream-type data parameters to the server.
     * 
     * <pre>
     * 
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
     * 
     * </pre>
     * 
     * @param parameterIndex
     * @param longData
     * 
     * @throws SQLException
     *             if an error occurs.
     */
    private void serverLongData(int parameterIndex, BindValue longData) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            MysqlIO mysql = this.connection.getIO();

            Buffer packet = mysql.getSharedSendPacket();

            Object value = longData.value;

            if (value instanceof byte[]) {
                packet.writeByte((byte) MysqlDefs.COM_LONG_DATA);
                packet.writeLong(this.serverStatementId);
                packet.writeInt((parameterIndex));

                packet.writeBytesNoNull((byte[]) longData.value);

                mysql.sendCommand(MysqlDefs.COM_LONG_DATA, null, packet, true, null, 0);
            } else if (value instanceof InputStream) {
                storeStream(mysql, parameterIndex, packet, (InputStream) value);
            } else if (value instanceof java.sql.Blob) {
                storeStream(mysql, parameterIndex, packet, ((java.sql.Blob) value).getBinaryStream());
            } else if (value instanceof Reader) {
                storeReader(mysql, parameterIndex, packet, (Reader) value);
            } else {
                throw SQLError.createSQLException(Messages.getString("ServerPreparedStatement.18") + value.getClass().getName() + "'",
                        SQLError.SQL_STATE_ILLEGAL_ARGUMENT, getExceptionInterceptor());
            }
        }
    }

    private void serverPrepare(String sql) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            MysqlIO mysql = this.connection.getIO();

            if (this.connection.getAutoGenerateTestcaseScript()) {
                dumpPrepareForTestcase();
            }

            try {
                long begin = 0;

                if (StringUtils.startsWithIgnoreCaseAndWs(sql, "LOAD DATA")) {
                    this.isLoadDataQuery = true;
                } else {
                    this.isLoadDataQuery = false;
                }

                if (this.connection.getProfileSQL()) {
                    begin = System.currentTimeMillis();
                }

                String characterEncoding = null;
                String connectionEncoding = this.connection.getCharacterEncoding();

                if (!this.isLoadDataQuery && (connectionEncoding != null)) {
                    characterEncoding = connectionEncoding;
                }

                Buffer prepareResultPacket = mysql.sendCommand(MysqlDefs.COM_PREPARE, sql, null, false, characterEncoding, 0);

                // 4.1.1 and newer use the first byte as an 'ok' or 'error' flag, so move the buffer pointer past it to start reading the statement id.
                prepareResultPacket.setPosition(1);

                this.serverStatementId = prepareResultPacket.readLong();
                this.fieldCount = prepareResultPacket.readInt();
                this.parameterCount = prepareResultPacket.readInt();
                this.parameterBindings = new BindValue[this.parameterCount];

                for (int i = 0; i < this.parameterCount; i++) {
                    this.parameterBindings[i] = new BindValue();
                }

                this.connection.incrementNumberOfPrepares();

                if (this.profileSQL) {
                    this.eventSink.consumeEvent(new ProfilerEventImpl(ProfilerEvent.TYPE_PREPARE, "", this.currentCatalog, this.connectionId, this.statementId,
                            -1, System.currentTimeMillis(), mysql.getCurrentTimeNanosOrMillis() - begin, mysql.getQueryTimingUnits(), null, LogUtils
                                    .findCallingClassAndMethod(new Throwable()), truncateQueryToLog(sql)));
                }

                if (this.parameterCount > 0) {
                    this.parameterFields = new Field[this.parameterCount];

                    Buffer metaDataPacket = mysql.readPacket();

                    int i = 0;

                    while (!metaDataPacket.isLastDataPacket() && (i < this.parameterCount)) {
                        this.parameterFields[i++] = mysql.unpackField(metaDataPacket, false);
                        metaDataPacket = mysql.readPacket();
                    }
                }

                if (this.fieldCount > 0) {
                    this.resultFields = new Field[this.fieldCount];

                    Buffer fieldPacket = mysql.readPacket();

                    int i = 0;

                    // Read in the result set column information
                    while (!fieldPacket.isLastDataPacket() && (i < this.fieldCount)) {
                        this.resultFields[i++] = mysql.unpackField(fieldPacket, false);
                        fieldPacket = mysql.readPacket();
                    }
                }
            } catch (SQLException sqlEx) {
                if (this.connection.getDumpQueriesOnException()) {
                    StringBuilder messageBuf = new StringBuilder(this.originalSql.length() + 32);
                    messageBuf.append("\n\nQuery being prepared when exception was thrown:\n\n");
                    messageBuf.append(this.originalSql);

                    sqlEx = ConnectionImpl.appendMessageToException(sqlEx, messageBuf.toString(), getExceptionInterceptor());
                }

                throw sqlEx;
            } finally {
                // Leave the I/O channel in a known state...there might be packets out there that we're not interested in
                this.connection.getIO().clearInputStream();
            }
        }
    }

    private String truncateQueryToLog(String sql) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            String query = null;

            if (sql.length() > this.connection.getMaxQuerySizeToLog()) {
                StringBuilder queryBuf = new StringBuilder(this.connection.getMaxQuerySizeToLog() + 12);
                queryBuf.append(sql.substring(0, this.connection.getMaxQuerySizeToLog()));
                queryBuf.append(Messages.getString("MysqlIO.25"));

                query = queryBuf.toString();
            } else {
                query = sql;
            }

            return query;
        }
    }

    private void serverResetStatement() throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {

            MysqlIO mysql = this.connection.getIO();

            Buffer packet = mysql.getSharedSendPacket();

            packet.writeByte((byte) MysqlDefs.COM_RESET_STMT);
            packet.writeLong(this.serverStatementId);

            try {
                mysql.sendCommand(MysqlDefs.COM_RESET_STMT, null, packet, false, null, 0);
            } catch (SQLException sqlEx) {
                throw sqlEx;
            } catch (CJException ex) {
                SQLException sqlEx = SQLError.createSQLException(ex.toString(), SQLError.SQL_STATE_GENERAL_ERROR, getExceptionInterceptor());
                sqlEx.initCause(ex);

                throw sqlEx;
            } finally {
                mysql.clearInputStream();
            }
        }
    }

    /**
     * @see java.sql.PreparedStatement#setArray(int, java.sql.Array)
     */
    @Override
    public void setArray(int i, Array x) throws SQLException {
        throw SQLError.notImplemented();
    }

    /**
     * @see java.sql.PreparedStatement#setAsciiStream(int, java.io.InputStream, int)
     */
    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, int length) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            if (x == null) {
                setNull(parameterIndex, java.sql.Types.BINARY);
            } else {
                BindValue binding = getBinding(parameterIndex, true);
                setType(binding, MysqlDefs.FIELD_TYPE_BLOB);

                binding.value = x;
                binding.isNull = false;
                binding.isLongData = true;

                if (this.connection.getUseStreamLengthsInPrepStmts()) {
                    binding.bindLength = length;
                } else {
                    binding.bindLength = -1;
                }
            }
        }
    }

    /**
     * @see java.sql.PreparedStatement#setBigDecimal(int, java.math.BigDecimal)
     */
    @Override
    public void setBigDecimal(int parameterIndex, BigDecimal x) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {

            if (x == null) {
                setNull(parameterIndex, java.sql.Types.DECIMAL);
            } else {

                BindValue binding = getBinding(parameterIndex, false);
                setType(binding, MysqlDefs.FIELD_TYPE_NEW_DECIMAL);

                binding.value = StringUtils.fixDecimalExponent(x.toPlainString());
                binding.isNull = false;
                binding.isLongData = false;
            }
        }
    }

    /**
     * @see java.sql.PreparedStatement#setBinaryStream(int, java.io.InputStream, int)
     */
    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, int length) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {

            if (x == null) {
                setNull(parameterIndex, java.sql.Types.BINARY);
            } else {
                BindValue binding = getBinding(parameterIndex, true);
                setType(binding, MysqlDefs.FIELD_TYPE_BLOB);

                binding.value = x;
                binding.isNull = false;
                binding.isLongData = true;

                if (this.connection.getUseStreamLengthsInPrepStmts()) {
                    binding.bindLength = length;
                } else {
                    binding.bindLength = -1;
                }
            }
        }
    }

    /**
     * @see java.sql.PreparedStatement#setBlob(int, java.sql.Blob)
     */
    @Override
    public void setBlob(int parameterIndex, Blob x) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {

            if (x == null) {
                setNull(parameterIndex, java.sql.Types.BINARY);
            } else {
                BindValue binding = getBinding(parameterIndex, true);
                setType(binding, MysqlDefs.FIELD_TYPE_BLOB);

                binding.value = x;
                binding.isNull = false;
                binding.isLongData = true;

                if (this.connection.getUseStreamLengthsInPrepStmts()) {
                    binding.bindLength = x.length();
                } else {
                    binding.bindLength = -1;
                }
            }
        }
    }

    /**
     * @see java.sql.PreparedStatement#setBoolean(int, boolean)
     */
    @Override
    public void setBoolean(int parameterIndex, boolean x) throws SQLException {
        setByte(parameterIndex, (x ? (byte) 1 : (byte) 0));
    }

    /**
     * @see java.sql.PreparedStatement#setByte(int, byte)
     */
    @Override
    public void setByte(int parameterIndex, byte x) throws SQLException {
        checkClosed();

        BindValue binding = getBinding(parameterIndex, false);
        setType(binding, MysqlDefs.FIELD_TYPE_TINY);

        binding.value = null;
        binding.longBinding = x;
        binding.isNull = false;
        binding.isLongData = false;
    }

    /**
     * @see java.sql.PreparedStatement#setBytes(int, byte)
     */
    @Override
    public void setBytes(int parameterIndex, byte[] x) throws SQLException {
        checkClosed();

        if (x == null) {
            setNull(parameterIndex, java.sql.Types.BINARY);
        } else {
            BindValue binding = getBinding(parameterIndex, false);
            setType(binding, MysqlDefs.FIELD_TYPE_VAR_STRING);

            binding.value = x;
            binding.isNull = false;
            binding.isLongData = false;
        }
    }

    /**
     * @see java.sql.PreparedStatement#setCharacterStream(int, java.io.Reader, int)
     */
    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, int length) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {

            if (reader == null) {
                setNull(parameterIndex, java.sql.Types.BINARY);
            } else {
                BindValue binding = getBinding(parameterIndex, true);
                setType(binding, MysqlDefs.FIELD_TYPE_BLOB);

                binding.value = reader;
                binding.isNull = false;
                binding.isLongData = true;

                if (this.connection.getUseStreamLengthsInPrepStmts()) {
                    binding.bindLength = length;
                } else {
                    binding.bindLength = -1;
                }
            }
        }
    }

    /**
     * @see java.sql.PreparedStatement#setClob(int, java.sql.Clob)
     */
    @Override
    public void setClob(int parameterIndex, Clob x) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {

            if (x == null) {
                setNull(parameterIndex, java.sql.Types.BINARY);
            } else {
                BindValue binding = getBinding(parameterIndex, true);
                setType(binding, MysqlDefs.FIELD_TYPE_BLOB);

                binding.value = x.getCharacterStream();
                binding.isNull = false;
                binding.isLongData = true;

                if (this.connection.getUseStreamLengthsInPrepStmts()) {
                    binding.bindLength = x.length();
                } else {
                    binding.bindLength = -1;
                }
            }
        }
    }

    /**
     * Set a parameter to a java.sql.Date value. The driver converts this to a
     * SQL DATE value when it sends it to the database.
     * 
     * @param parameterIndex
     *            the first parameter is 1, the second is 2, ...
     * @param x
     *            the parameter value
     * 
     * @exception SQLException
     *                if a database-access error occurs.
     */
    @Override
    public void setDate(int parameterIndex, Date x) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            setDateInternal(parameterIndex, x, this.connection.getDefaultTimeZone());
        }
    }

    /**
     * Set a parameter to a java.sql.Date value. The driver converts this to a
     * SQL DATE value when it sends it to the database.
     * 
     * @param parameterIndex
     *            the first parameter is 1, the second is 2, ...
     * @param x
     *            the parameter value
     * @param cal
     *            the calendar to interpret the date with
     * 
     * @exception SQLException
     *                if a database-access error occurs.
     */
    @Override
    public void setDate(int parameterIndex, Date x, Calendar cal) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            setDateInternal(parameterIndex, x, cal.getTimeZone());
        }
    }

    private void setDateInternal(int parameterIndex, Date x, TimeZone tz) throws SQLException {
        if (x == null) {
            setNull(parameterIndex, java.sql.Types.DATE);
        } else {
            BindValue binding = getBinding(parameterIndex, false);
            setType(binding, MysqlDefs.FIELD_TYPE_DATE);

            binding.value = x;
            binding.tz = tz;
            binding.isNull = false;
            binding.isLongData = false;
        }
    }

    /**
     * @see java.sql.PreparedStatement#setDouble(int, double)
     */
    @Override
    public void setDouble(int parameterIndex, double x) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {

            if (!this.connection.getAllowNanAndInf() && (x == Double.POSITIVE_INFINITY || x == Double.NEGATIVE_INFINITY || Double.isNaN(x))) {
                throw SQLError.createSQLException(Messages.getString("PreparedStatement.64", new Object[] { x }), SQLError.SQL_STATE_ILLEGAL_ARGUMENT,
                        getExceptionInterceptor());

            }

            BindValue binding = getBinding(parameterIndex, false);
            setType(binding, MysqlDefs.FIELD_TYPE_DOUBLE);

            binding.value = null;
            binding.doubleBinding = x;
            binding.isNull = false;
            binding.isLongData = false;
        }
    }

    /**
     * @see java.sql.PreparedStatement#setFloat(int, float)
     */
    @Override
    public void setFloat(int parameterIndex, float x) throws SQLException {
        checkClosed();

        BindValue binding = getBinding(parameterIndex, false);
        setType(binding, MysqlDefs.FIELD_TYPE_FLOAT);

        binding.value = null;
        binding.floatBinding = x;
        binding.isNull = false;
        binding.isLongData = false;
    }

    /**
     * @see java.sql.PreparedStatement#setInt(int, int)
     */
    @Override
    public void setInt(int parameterIndex, int x) throws SQLException {
        checkClosed();

        BindValue binding = getBinding(parameterIndex, false);
        setType(binding, MysqlDefs.FIELD_TYPE_LONG);

        binding.value = null;
        binding.longBinding = x;
        binding.isNull = false;
        binding.isLongData = false;
    }

    /**
     * @see java.sql.PreparedStatement#setLong(int, long)
     */
    @Override
    public void setLong(int parameterIndex, long x) throws SQLException {
        checkClosed();

        BindValue binding = getBinding(parameterIndex, false);
        setType(binding, MysqlDefs.FIELD_TYPE_LONGLONG);

        binding.value = null;
        binding.longBinding = x;
        binding.isNull = false;
        binding.isLongData = false;
    }

    /**
     * @see java.sql.PreparedStatement#setNull(int, int)
     */
    @Override
    public void setNull(int parameterIndex, int sqlType) throws SQLException {
        checkClosed();

        BindValue binding = getBinding(parameterIndex, false);

        //
        // Don't re-set types, but use something if this
        // parameter was never specified
        //
        if (binding.bufferType == 0) {
            setType(binding, MysqlDefs.FIELD_TYPE_NULL);
        }

        binding.value = null;
        binding.isNull = true;
        binding.isLongData = false;
    }

    /**
     * @see java.sql.PreparedStatement#setNull(int, int, java.lang.String)
     */
    @Override
    public void setNull(int parameterIndex, int sqlType, String typeName) throws SQLException {
        checkClosed();

        BindValue binding = getBinding(parameterIndex, false);

        //
        // Don't re-set types, but use something if this parameter was never specified
        //
        if (binding.bufferType == 0) {
            setType(binding, MysqlDefs.FIELD_TYPE_NULL);
        }

        binding.value = null;
        binding.isNull = true;
        binding.isLongData = false;
    }

    /**
     * @see java.sql.PreparedStatement#setRef(int, java.sql.Ref)
     */
    @Override
    public void setRef(int i, Ref x) throws SQLException {
        throw SQLError.notImplemented();
    }

    /**
     * @see java.sql.PreparedStatement#setShort(int, short)
     */
    @Override
    public void setShort(int parameterIndex, short x) throws SQLException {
        checkClosed();

        BindValue binding = getBinding(parameterIndex, false);
        setType(binding, MysqlDefs.FIELD_TYPE_SHORT);

        binding.value = null;
        binding.longBinding = x;
        binding.isNull = false;
        binding.isLongData = false;
    }

    /**
     * @see java.sql.PreparedStatement#setString(int, java.lang.String)
     */
    @Override
    public void setString(int parameterIndex, String x) throws SQLException {
        checkClosed();

        if (x == null) {
            setNull(parameterIndex, java.sql.Types.CHAR);
        } else {
            BindValue binding = getBinding(parameterIndex, false);
            setType(binding, MysqlDefs.FIELD_TYPE_VAR_STRING);

            binding.value = x;
            binding.isNull = false;
            binding.isLongData = false;
        }
    }

    /**
     * Set a parameter to a java.sql.Time value.
     * 
     * @param parameterIndex
     *            the first parameter is 1...));
     * @param x
     *            the parameter value
     * 
     * @throws SQLException
     *             if a database access error occurs
     */
    @Override
    public void setTime(int parameterIndex, Time x) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            setTimeInternal(parameterIndex, x, this.connection.getDefaultTimeZone());
        }
    }

    /**
     * Set a parameter to a java.sql.Time value. The driver converts this to a
     * SQL TIME value when it sends it to the database, using the given
     * timezone.
     * 
     * @param parameterIndex
     *            the first parameter is 1...));
     * @param x
     *            the parameter value
     * @param cal
     *            the timezone to use
     * 
     * @throws SQLException
     *             if a database access error occurs
     */
    @Override
    public void setTime(int parameterIndex, Time x, Calendar cal) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            setTimeInternal(parameterIndex, x, cal.getTimeZone());
        }
    }

    /**
     * Set a parameter to a java.sql.Time value. The driver converts this to a
     * SQL TIME value when it sends it to the database, using the given
     * timezone.
     * 
     * @param parameterIndex
     *            the first parameter is 1...));
     * @param x
     *            the parameter value
     * @param tz
     *            the timezone to use
     * 
     * @throws SQLException
     *             if a database access error occurs
     */
    private void setTimeInternal(int parameterIndex, Time x, TimeZone tz) throws SQLException {
        if (x == null) {
            setNull(parameterIndex, java.sql.Types.TIME);
        } else {
            BindValue binding = getBinding(parameterIndex, false);
            setType(binding, MysqlDefs.FIELD_TYPE_TIME);

            binding.value = x;
            binding.tz = tz;

            binding.isNull = false;
            binding.isLongData = false;
        }
    }

    /**
     * Set a parameter to a java.sql.Timestamp value. The driver converts this
     * to a SQL TIMESTAMP value when it sends it to the database.
     * 
     * @param parameterIndex
     *            the first parameter is 1, the second is 2, ...
     * @param x
     *            the parameter value
     * 
     * @throws SQLException
     *             if a database-access error occurs.
     */
    @Override
    public void setTimestamp(int parameterIndex, java.sql.Timestamp x) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            setTimestampInternal(parameterIndex, x, this.connection.getDefaultTimeZone());
        }
    }

    /**
     * Set a parameter to a java.sql.Timestamp value. The driver converts this
     * to a SQL TIMESTAMP value when it sends it to the database.
     * 
     * @param parameterIndex
     *            the first parameter is 1, the second is 2, ...
     * @param x
     *            the parameter value
     * @param cal
     *            the timezone to use
     * 
     * @throws SQLException
     *             if a database-access error occurs.
     */
    @Override
    public void setTimestamp(int parameterIndex, java.sql.Timestamp x, Calendar cal) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            setTimestampInternal(parameterIndex, x, cal.getTimeZone());
        }
    }

    private void setTimestampInternal(int parameterIndex, java.sql.Timestamp x, TimeZone tz) throws SQLException {
        if (x == null) {
            setNull(parameterIndex, java.sql.Types.TIMESTAMP);
        } else {
            BindValue binding = getBinding(parameterIndex, false);
            setType(binding, MysqlDefs.FIELD_TYPE_DATETIME);

            binding.value = x;
            binding.tz = tz;

            binding.isNull = false;
            binding.isLongData = false;
        }
    }

    protected void setType(BindValue oldValue, int bufferType) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            if (oldValue.bufferType != bufferType) {
                this.sendTypesToServer = true;
            }

            oldValue.bufferType = bufferType;
        }
    }

    /**
     * @param parameterIndex
     * @param x
     * @param length
     * 
     * @throws SQLException
     * @throws SQLFeatureNotSupportedException
     * 
     * @see java.sql.PreparedStatement#setUnicodeStream(int, java.io.InputStream, int)
     * @deprecated
     */
    @Deprecated
    @Override
    public void setUnicodeStream(int parameterIndex, InputStream x, int length) throws SQLException {
        checkClosed();

        throw SQLError.notImplemented();
    }

    /**
     * @see java.sql.PreparedStatement#setURL(int, java.net.URL)
     */
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
    private void storeBinding(Buffer packet, BindValue bindValue, MysqlIO mysql) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            try {
                Object value = bindValue.value;

                //
                // Handle primitives first
                //
                switch (bindValue.bufferType) {

                    case MysqlDefs.FIELD_TYPE_TINY:
                        packet.writeByte((byte) bindValue.longBinding);
                        return;
                    case MysqlDefs.FIELD_TYPE_SHORT:
                        packet.ensureCapacity(2);
                        packet.writeInt((int) bindValue.longBinding);
                        return;
                    case MysqlDefs.FIELD_TYPE_LONG:
                        packet.ensureCapacity(4);
                        packet.writeLong((int) bindValue.longBinding);
                        return;
                    case MysqlDefs.FIELD_TYPE_LONGLONG:
                        packet.ensureCapacity(8);
                        packet.writeLongLong(bindValue.longBinding);
                        return;
                    case MysqlDefs.FIELD_TYPE_FLOAT:
                        packet.ensureCapacity(4);
                        packet.writeFloat(bindValue.floatBinding);
                        return;
                    case MysqlDefs.FIELD_TYPE_DOUBLE:
                        packet.ensureCapacity(8);
                        packet.writeDouble(bindValue.doubleBinding);
                        return;
                    case MysqlDefs.FIELD_TYPE_TIME:
                        storeTime(packet, (Time) value, bindValue.tz);
                        return;
                    case MysqlDefs.FIELD_TYPE_DATE:
                    case MysqlDefs.FIELD_TYPE_DATETIME:
                    case MysqlDefs.FIELD_TYPE_TIMESTAMP:
                        storeDateTime(packet, (java.util.Date) value, bindValue.tz, mysql, bindValue.bufferType);
                        return;
                    case MysqlDefs.FIELD_TYPE_VAR_STRING:
                    case MysqlDefs.FIELD_TYPE_STRING:
                    case MysqlDefs.FIELD_TYPE_VARCHAR:
                    case MysqlDefs.FIELD_TYPE_DECIMAL:
                    case MysqlDefs.FIELD_TYPE_NEW_DECIMAL:
                        if (value instanceof byte[]) {
                            packet.writeLenBytes((byte[]) value);
                        } else if (!this.isLoadDataQuery) {
                            packet.writeLenString((String) value, this.charEncoding);
                        } else {
                            packet.writeLenBytes(StringUtils.getBytes((String) value));
                        }

                        return;
                }

            } catch (SQLException | CJException uEE) {
                throw SQLError.createSQLException(Messages.getString("ServerPreparedStatement.22") + this.connection.getCharacterEncoding() + "'",
                        SQLError.SQL_STATE_GENERAL_ERROR, uEE, getExceptionInterceptor());
            }
        }
    }

    /**
     * @param intoBuf
     * @param dt
     * @param mysql
     * @param bufferType
     * @throws SQLException
     */
    private void storeDateTime(Buffer intoBuf, java.util.Date dt, TimeZone tz, MysqlIO mysql, int bufferType) throws SQLException {
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

            intoBuf.writeByte(length); // length

            int year = cal.get(Calendar.YEAR);
            int month = cal.get(Calendar.MONTH) + 1;
            int date = cal.get(Calendar.DAY_OF_MONTH);

            intoBuf.writeInt(year);
            intoBuf.writeByte((byte) month);
            intoBuf.writeByte((byte) date);

            if (dt instanceof java.sql.Date) {
                intoBuf.writeByte((byte) 0);
                intoBuf.writeByte((byte) 0);
                intoBuf.writeByte((byte) 0);
            } else {
                intoBuf.writeByte((byte) cal.get(Calendar.HOUR_OF_DAY));
                intoBuf.writeByte((byte) cal.get(Calendar.MINUTE));
                intoBuf.writeByte((byte) cal.get(Calendar.SECOND));
            }

            if (length == 11) {
                //  MySQL expects microseconds, not nanos
                intoBuf.writeLong(((java.sql.Timestamp) dt).getNanos() / 1000);
            }
        }
    }

    //
    // TO DO: Investigate using NIO to do this faster
    //
    private void storeReader(MysqlIO mysql, int parameterIndex, Buffer packet, Reader inStream) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            String forcedEncoding = this.connection.getClobCharacterEncoding();

            String clobEncoding = (forcedEncoding == null ? this.connection.getCharacterEncoding() : forcedEncoding);

            int maxBytesChar = 2;

            if (clobEncoding != null) {
                if (!clobEncoding.equals("UTF-16")) {
                    maxBytesChar = this.connection.getMaxBytesPerChar(clobEncoding);

                    if (maxBytesChar == 1) {
                        maxBytesChar = 2; // for safety
                    }
                } else {
                    maxBytesChar = 4;
                }
            }

            char[] buf = new char[BLOB_STREAM_READ_BUF_SIZE / maxBytesChar];

            int numRead = 0;

            int bytesInPacket = 0;
            int totalBytesRead = 0;
            int bytesReadAtLastSend = 0;
            int packetIsFullAt = this.connection.getBlobSendChunkSize();

            try {
                packet.clear();
                packet.setPosition(0);
                packet.writeByte((byte) MysqlDefs.COM_LONG_DATA);
                packet.writeLong(this.serverStatementId);
                packet.writeInt((parameterIndex));

                boolean readAny = false;

                while ((numRead = inStream.read(buf)) != -1) {
                    readAny = true;

                    byte[] valueAsBytes = StringUtils.getBytes(buf, 0, numRead, clobEncoding);

                    packet.writeBytesNoNull(valueAsBytes, 0, valueAsBytes.length);

                    bytesInPacket += valueAsBytes.length;
                    totalBytesRead += valueAsBytes.length;

                    if (bytesInPacket >= packetIsFullAt) {
                        bytesReadAtLastSend = totalBytesRead;

                        mysql.sendCommand(MysqlDefs.COM_LONG_DATA, null, packet, true, null, 0);

                        bytesInPacket = 0;
                        packet.clear();
                        packet.setPosition(0);
                        packet.writeByte((byte) MysqlDefs.COM_LONG_DATA);
                        packet.writeLong(this.serverStatementId);
                        packet.writeInt((parameterIndex));
                    }
                }

                if (totalBytesRead != bytesReadAtLastSend) {
                    mysql.sendCommand(MysqlDefs.COM_LONG_DATA, null, packet, true, null, 0);
                }

                if (!readAny) {
                    mysql.sendCommand(MysqlDefs.COM_LONG_DATA, null, packet, true, null, 0);
                }
            } catch (IOException ioEx) {
                SQLException sqlEx = SQLError.createSQLException(Messages.getString("ServerPreparedStatement.24") + ioEx.toString(),
                        SQLError.SQL_STATE_GENERAL_ERROR, getExceptionInterceptor());
                sqlEx.initCause(ioEx);

                throw sqlEx;
            } finally {
                if (this.connection.getAutoClosePStmtStreams()) {
                    if (inStream != null) {
                        try {
                            inStream.close();
                        } catch (IOException ioEx) {
                            // ignore
                        }
                    }
                }
            }
        }
    }

    private void storeStream(MysqlIO mysql, int parameterIndex, Buffer packet, InputStream inStream) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            byte[] buf = new byte[BLOB_STREAM_READ_BUF_SIZE];

            int numRead = 0;

            try {
                int bytesInPacket = 0;
                int totalBytesRead = 0;
                int bytesReadAtLastSend = 0;
                int packetIsFullAt = this.connection.getBlobSendChunkSize();

                packet.clear();
                packet.setPosition(0);
                packet.writeByte((byte) MysqlDefs.COM_LONG_DATA);
                packet.writeLong(this.serverStatementId);
                packet.writeInt((parameterIndex));

                boolean readAny = false;

                while ((numRead = inStream.read(buf)) != -1) {

                    readAny = true;

                    packet.writeBytesNoNull(buf, 0, numRead);
                    bytesInPacket += numRead;
                    totalBytesRead += numRead;

                    if (bytesInPacket >= packetIsFullAt) {
                        bytesReadAtLastSend = totalBytesRead;

                        mysql.sendCommand(MysqlDefs.COM_LONG_DATA, null, packet, true, null, 0);

                        bytesInPacket = 0;
                        packet.clear();
                        packet.setPosition(0);
                        packet.writeByte((byte) MysqlDefs.COM_LONG_DATA);
                        packet.writeLong(this.serverStatementId);
                        packet.writeInt((parameterIndex));
                    }
                }

                if (totalBytesRead != bytesReadAtLastSend) {
                    mysql.sendCommand(MysqlDefs.COM_LONG_DATA, null, packet, true, null, 0);
                }

                if (!readAny) {
                    mysql.sendCommand(MysqlDefs.COM_LONG_DATA, null, packet, true, null, 0);
                }
            } catch (IOException ioEx) {
                SQLException sqlEx = SQLError.createSQLException(Messages.getString("ServerPreparedStatement.25") + ioEx.toString(),
                        SQLError.SQL_STATE_GENERAL_ERROR, getExceptionInterceptor());
                sqlEx.initCause(ioEx);

                throw sqlEx;
            } finally {
                if (this.connection.getAutoClosePStmtStreams()) {
                    if (inStream != null) {
                        try {
                            inStream.close();
                        } catch (IOException ioEx) {
                            // ignore
                        }
                    }
                }
            }
        }
    }

    /**
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
        StringBuilder toStringBuf = new StringBuilder();

        toStringBuf.append("com.mysql.jdbc.ServerPreparedStatement[");
        toStringBuf.append(this.serverStatementId);
        toStringBuf.append("] - ");

        try {
            toStringBuf.append(asSql());
        } catch (SQLException sqlEx) {
            toStringBuf.append(Messages.getString("ServerPreparedStatement.6"));
            toStringBuf.append(sqlEx);
        }

        return toStringBuf.toString();
    }

    protected long getServerStatementId() {
        return this.serverStatementId;
    }

    private boolean hasCheckedRewrite = false;
    private boolean canRewrite = false;

    @Override
    public boolean canRewriteAsMultiValueInsertAtSqlLevel() throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            if (!this.hasCheckedRewrite) {
                this.hasCheckedRewrite = true;
                this.canRewrite = canRewrite(this.originalSql, isOnDuplicateKeyUpdate(), getLocationOfOnDuplicateKeyUpdate(), 0);
                // We need to client-side parse this to get the VALUES clause, etc.
                this.parseInfo = new ParseInfo(this.originalSql, this.connection, this.connection.getMetaData(), this.charEncoding);
            }

            return this.canRewrite;
        }
    }

    public boolean canRewriteAsMultivalueInsertStatement() throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            if (!canRewriteAsMultiValueInsertAtSqlLevel()) {
                return false;
            }

            BindValue[] currentBindValues = null;
            BindValue[] previousBindValues = null;

            int nbrCommands = this.batchedArgs.size();

            // Can't have type changes between sets of bindings for this to work...

            for (int commandIndex = 0; commandIndex < nbrCommands; commandIndex++) {
                Object arg = this.batchedArgs.get(commandIndex);

                if (!(arg instanceof String)) {

                    currentBindValues = ((BatchedBindValues) arg).batchedParameterValues;

                    // We need to check types each time, as the user might have bound different types in each addBatch()

                    if (previousBindValues != null) {
                        for (int j = 0; j < this.parameterBindings.length; j++) {
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
                this.locationOfOnDuplicateKeyUpdate = getOnDuplicateKeyLocation(this.originalSql, this.connection.getDontCheckOnDuplicateKeyUpdateInSQL(),
                        this.connection.getRewriteBatchedStatements(), this.connection.isNoBackslashEscapesSet());
            }

            return this.locationOfOnDuplicateKeyUpdate;
        }
    }

    protected boolean isOnDuplicateKeyUpdate() throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            return getLocationOfOnDuplicateKeyUpdate() != -1;
        }
    }

    /**
     * Computes the maximum parameter set size, and entire batch size given
     * the number of arguments in the batch.
     * 
     * @throws SQLException
     */
    @Override
    protected long[] computeMaxParameterSetSizeAndBatchSize(int numBatchedArgs) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            long sizeOfEntireBatch = 1 + /* com_execute */+4 /* stmt id */+ 1 /* flags */+ 4 /* batch count padding */;
            long maxSizeOfParameterSet = 0;

            for (int i = 0; i < numBatchedArgs; i++) {
                BindValue[] paramArg = ((BatchedBindValues) this.batchedArgs.get(i)).batchedParameterValues;

                long sizeOfParameterSet = 0;

                sizeOfParameterSet += (this.parameterCount + 7) / 8; // for isNull

                sizeOfParameterSet += this.parameterCount * 2; // have to send types

                for (int j = 0; j < this.parameterBindings.length; j++) {
                    if (!paramArg[j].isNull) {

                        long size = paramArg[j].getBoundLength();

                        if (paramArg[j].isLongData) {
                            if (size != -1) {
                                sizeOfParameterSet += size;
                            }
                        } else {
                            sizeOfParameterSet += size;
                        }
                    }
                }

                sizeOfEntireBatch += sizeOfParameterSet;

                if (sizeOfParameterSet > maxSizeOfParameterSet) {
                    maxSizeOfParameterSet = sizeOfParameterSet;
                }
            }

            return new long[] { maxSizeOfParameterSet, sizeOfEntireBatch };
        }
    }

    @Override
    protected int setOneBatchedParameterSet(java.sql.PreparedStatement batchedStatement, int batchedParamIndex, Object paramSet) throws SQLException {
        BindValue[] paramArg = ((BatchedBindValues) paramSet).batchedParameterValues;

        for (int j = 0; j < paramArg.length; j++) {
            if (paramArg[j].isNull) {
                batchedStatement.setNull(batchedParamIndex++, Types.NULL);
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

                        case MysqlDefs.FIELD_TYPE_TINY:
                            batchedStatement.setByte(batchedParamIndex++, (byte) paramArg[j].longBinding);
                            break;
                        case MysqlDefs.FIELD_TYPE_SHORT:
                            batchedStatement.setShort(batchedParamIndex++, (short) paramArg[j].longBinding);
                            break;
                        case MysqlDefs.FIELD_TYPE_LONG:
                            batchedStatement.setInt(batchedParamIndex++, (int) paramArg[j].longBinding);
                            break;
                        case MysqlDefs.FIELD_TYPE_LONGLONG:
                            batchedStatement.setLong(batchedParamIndex++, paramArg[j].longBinding);
                            break;
                        case MysqlDefs.FIELD_TYPE_FLOAT:
                            batchedStatement.setFloat(batchedParamIndex++, paramArg[j].floatBinding);
                            break;
                        case MysqlDefs.FIELD_TYPE_DOUBLE:
                            batchedStatement.setDouble(batchedParamIndex++, paramArg[j].doubleBinding);
                            break;
                        case MysqlDefs.FIELD_TYPE_TIME:
                            batchedStatement.setTime(batchedParamIndex++, (Time) paramArg[j].value);
                            break;
                        case MysqlDefs.FIELD_TYPE_DATE:
                            batchedStatement.setDate(batchedParamIndex++, (Date) paramArg[j].value);
                            break;
                        case MysqlDefs.FIELD_TYPE_DATETIME:
                        case MysqlDefs.FIELD_TYPE_TIMESTAMP:
                            batchedStatement.setTimestamp(batchedParamIndex++, (Timestamp) paramArg[j].value);
                            break;
                        case MysqlDefs.FIELD_TYPE_VAR_STRING:
                        case MysqlDefs.FIELD_TYPE_STRING:
                        case MysqlDefs.FIELD_TYPE_VARCHAR:
                        case MysqlDefs.FIELD_TYPE_DECIMAL:
                        case MysqlDefs.FIELD_TYPE_NEW_DECIMAL:
                            Object value = paramArg[j].value;

                            if (value instanceof byte[]) {
                                batchedStatement.setBytes(batchedParamIndex, (byte[]) value);
                            } else {
                                batchedStatement.setString(batchedParamIndex, (String) value);
                            }

                            // If we ended up here as a multi-statement, we're not working with a server prepared statement

                            if (batchedStatement instanceof ServerPreparedStatement) {
                                BindValue asBound = ((ServerPreparedStatement) batchedStatement).getBinding(batchedParamIndex, false);
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
    protected PreparedStatement prepareBatchedInsertSQL(MysqlJdbcConnection localConn, int numBatches) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            try {
                PreparedStatement pstmt = new ServerPreparedStatement(localConn, this.parseInfo.getSqlForBatch(numBatches), this.currentCatalog,
                        this.resultSetConcurrency, this.resultSetType);
                pstmt.setRetrieveGeneratedKeys(this.retrieveGeneratedKeys);

                return pstmt;
            } catch (UnsupportedEncodingException e) {
                SQLException sqlEx = SQLError.createSQLException(Messages.getString("ServerPreparedStatement.27"), SQLError.SQL_STATE_GENERAL_ERROR,
                        getExceptionInterceptor());
                sqlEx.initCause(e);

                throw sqlEx;
            }
        }
    }

    /**
     * @see java.sql.PreparedStatement#setNCharacterStream(int, java.io.Reader, long)
     */
    @Override
    public void setNCharacterStream(int parameterIndex, Reader reader, long length) throws SQLException {
        // can't take if characterEncoding isn't utf8
        if (!this.charEncoding.equalsIgnoreCase("UTF-8") && !this.charEncoding.equalsIgnoreCase("utf8")) {
            throw SQLError.createSQLException(Messages.getString("ServerPreparedStatement.28"), getExceptionInterceptor());
        }

        checkClosed();

        if (reader == null) {
            setNull(parameterIndex, java.sql.Types.BINARY);
        } else {
            BindValue binding = getBinding(parameterIndex, true);
            setType(binding, MysqlDefs.FIELD_TYPE_BLOB);

            binding.value = reader;
            binding.isNull = false;
            binding.isLongData = true;

            if (this.connection.getUseStreamLengthsInPrepStmts()) {
                binding.bindLength = length;
            } else {
                binding.bindLength = -1;
            }
        }
    }

    /**
     * @see java.sql.PreparedStatement#setNClob(int, java.sql.NClob)
     */
    @Override
    public void setNClob(int parameterIndex, NClob x) throws SQLException {
        setNClob(parameterIndex, x.getCharacterStream(), this.connection.getUseStreamLengthsInPrepStmts() ? x.length() : -1);
    }

    /**
     * Set a NCLOB parameter.
     * 
     * @param parameterIndex
     *            the first parameter is 1, the second is 2, ...
     * @param reader
     *            the java reader which contains the UNICODE data
     * @param length
     *            the number of characters in the stream
     * 
     * @throws SQLException
     *             if a database error occurs
     */
    @Override
    public void setNClob(int parameterIndex, Reader reader, long length) throws SQLException {
        // can't take if characterEncoding isn't utf8
        if (!this.charEncoding.equalsIgnoreCase("UTF-8") && !this.charEncoding.equalsIgnoreCase("utf8")) {
            throw SQLError.createSQLException(Messages.getString("ServerPreparedStatement.29"), getExceptionInterceptor());
        }

        checkClosed();

        if (reader == null) {
            setNull(parameterIndex, java.sql.Types.NCLOB);
        } else {
            BindValue binding = getBinding(parameterIndex, true);
            setType(binding, MysqlDefs.FIELD_TYPE_BLOB);

            binding.value = reader;
            binding.isNull = false;
            binding.isLongData = true;

            if (this.connection.getUseStreamLengthsInPrepStmts()) {
                binding.bindLength = length;
            } else {
                binding.bindLength = -1;
            }
        }
    }

    /**
     * @see java.sql.PreparedStatement#setNString(int, java.lang.String)
     */
    @Override
    public void setNString(int parameterIndex, String x) throws SQLException {
        if (this.charEncoding.equalsIgnoreCase("UTF-8") || this.charEncoding.equalsIgnoreCase("utf8")) {
            setString(parameterIndex, x);
        } else {
            throw SQLError.createSQLException(Messages.getString("ServerPreparedStatement.30"), getExceptionInterceptor());
        }
    }

    @Override
    public void setRowId(int parameterIndex, RowId x) throws SQLException {
        PreparedStatementHelper.setRowId(this, parameterIndex, x);
    }

    @Override
    public void setSQLXML(int parameterIndex, SQLXML xmlObject) throws SQLException {
        PreparedStatementHelper.setSQLXML(this, parameterIndex, xmlObject);
    }

}
