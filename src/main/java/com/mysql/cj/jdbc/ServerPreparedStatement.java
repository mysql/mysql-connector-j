/*
  Copyright (c) 2002, 2016, Oracle and/or its affiliates. All rights reserved.

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

import com.mysql.cj.api.ProfilerEvent;
import com.mysql.cj.api.jdbc.JdbcConnection;
import com.mysql.cj.api.jdbc.result.ResultSetInternalMethods;
import com.mysql.cj.api.mysqla.io.NativeProtocol.IntegerDataType;
import com.mysql.cj.api.mysqla.io.NativeProtocol.StringLengthDataType;
import com.mysql.cj.api.mysqla.io.NativeProtocol.StringSelfDataType;
import com.mysql.cj.api.mysqla.io.PacketPayload;
import com.mysql.cj.api.mysqla.result.ColumnDefinition;
import com.mysql.cj.api.result.Row;
import com.mysql.cj.core.Messages;
import com.mysql.cj.core.MysqlType;
import com.mysql.cj.core.conf.PropertyDefinitions;
import com.mysql.cj.core.exceptions.CJException;
import com.mysql.cj.core.exceptions.ExceptionFactory;
import com.mysql.cj.core.profiler.ProfilerEventHandlerFactory;
import com.mysql.cj.core.profiler.ProfilerEventImpl;
import com.mysql.cj.core.result.Field;
import com.mysql.cj.core.util.LogUtils;
import com.mysql.cj.core.util.StringUtils;
import com.mysql.cj.core.util.TestUtils;
import com.mysql.cj.jdbc.exceptions.MySQLStatementCancelledException;
import com.mysql.cj.jdbc.exceptions.MySQLTimeoutException;
import com.mysql.cj.jdbc.exceptions.SQLError;
import com.mysql.cj.jdbc.exceptions.SQLExceptionsMapping;
import com.mysql.cj.jdbc.result.ResultSetMetaData;
import com.mysql.cj.jdbc.util.TimeUtil;
import com.mysql.cj.mysqla.MysqlaConstants;
import com.mysql.cj.mysqla.io.Buffer;
import com.mysql.cj.mysqla.io.ColumnDefinitionFactory;

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
            this.isNull = false;
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

            if (this.isNull) {
                return "NULL";
            }

            switch (this.bufferType) {
                case MysqlaConstants.FIELD_TYPE_TINY:
                case MysqlaConstants.FIELD_TYPE_SHORT:
                case MysqlaConstants.FIELD_TYPE_LONG:
                case MysqlaConstants.FIELD_TYPE_LONGLONG:
                    return String.valueOf(this.longBinding);
                case MysqlaConstants.FIELD_TYPE_FLOAT:
                    return String.valueOf(this.floatBinding);
                case MysqlaConstants.FIELD_TYPE_DOUBLE:
                    return String.valueOf(this.doubleBinding);
                case MysqlaConstants.FIELD_TYPE_TIME:
                case MysqlaConstants.FIELD_TYPE_DATE:
                case MysqlaConstants.FIELD_TYPE_DATETIME:
                case MysqlaConstants.FIELD_TYPE_TIMESTAMP:
                case MysqlaConstants.FIELD_TYPE_VAR_STRING:
                case MysqlaConstants.FIELD_TYPE_STRING:
                case MysqlaConstants.FIELD_TYPE_VARCHAR:
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

                case MysqlaConstants.FIELD_TYPE_TINY:
                    return 1;
                case MysqlaConstants.FIELD_TYPE_SHORT:
                    return 2;
                case MysqlaConstants.FIELD_TYPE_LONG:
                    return 4;
                case MysqlaConstants.FIELD_TYPE_LONGLONG:
                    return 8;
                case MysqlaConstants.FIELD_TYPE_FLOAT:
                    return 4;
                case MysqlaConstants.FIELD_TYPE_DOUBLE:
                    return 8;
                case MysqlaConstants.FIELD_TYPE_TIME:
                    return 9;
                case MysqlaConstants.FIELD_TYPE_DATE:
                    return 7;
                case MysqlaConstants.FIELD_TYPE_DATETIME:
                case MysqlaConstants.FIELD_TYPE_TIMESTAMP:
                    return 11;
                case MysqlaConstants.FIELD_TYPE_VAR_STRING:
                case MysqlaConstants.FIELD_TYPE_STRING:
                case MysqlaConstants.FIELD_TYPE_VARCHAR:
                case MysqlaConstants.FIELD_TYPE_DECIMAL:
                case MysqlaConstants.FIELD_TYPE_NEWDECIMAL:
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

    private PacketPayload outByteBuffer;

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

    /** The packet buffer size the MySQL server reported upon connection */
    private int netBufferLength = 16384;

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

        checkNullOrEmptyQuery(sql);

        int startOfStatement = findStartOfStatement(sql);

        this.firstCharOfStmt = StringUtils.firstAlphaCharUc(sql, startOfStatement);

        this.hasOnDuplicateKeyUpdate = this.firstCharOfStmt == 'I' && containsOnDuplicateKeyInString(sql);

        this.useAutoSlowLog = this.session.getPropertySet().getBooleanReadableProperty(PropertyDefinitions.PNAME_autoSlowLog).getValue();

        this.netBufferLength = this.session.getServerVariable("net_buffer_length", 16 * 1024);

        String statementComment = this.connection.getStatementComment();

        this.originalSql = (statementComment == null) ? sql : "/* " + statementComment + " */ " + sql;

        try {
            serverPrepare(sql);
        } catch (CJException | SQLException sqlEx) {
            realClose(false, true);

            throw SQLExceptionsMapping.translateException(sqlEx, getExceptionInterceptor());
        }

        setResultSetType(resultSetType);
        setResultSetConcurrency(resultSetConcurrency);

        this.parameterTypes = new MysqlType[this.parameterCount];
    }

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
                pStmtForSub = PreparedStatement.getInstance(this.connection, this.originalSql, this.getCurrentCatalog());

                int numParameters = pStmtForSub.parameterCount;
                int ourNumParameters = this.parameterCount;

                for (int i = 0; (i < numParameters) && (i < ourNumParameters); i++) {
                    if (this.parameterBindings[i] != null) {
                        if (this.parameterBindings[i].isNull) {
                            pStmtForSub.setNull(i + 1, MysqlType.NULL);
                        } else {
                            BindValue bindValue = this.parameterBindings[i];

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
    protected JdbcConnection checkClosed() {
        if (this.invalid) {
            throw this.invalidationException;
        }

        return super.checkClosed();
    }

    @Override
    public void clearParameters() {
        synchronized (checkClosed().getConnectionMutex()) {
            clearParametersInternal(true);
        }
    }

    private void clearParametersInternal(boolean clearServerParameters) {
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
    protected long[] executeBatchSerially(int batchTimeout) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            JdbcConnection locallyScopedConn = this.connection;

            if (locallyScopedConn.isReadOnly()) {
                throw SQLError.createSQLException(Messages.getString("ServerPreparedStatement.2") + Messages.getString("ServerPreparedStatement.3"),
                        SQLError.SQL_STATE_ILLEGAL_ARGUMENT, getExceptionInterceptor());
            }

            clearWarnings();

            // Store this for later, we're going to 'swap' them out
            // as we execute each batched statement...
            BindValue[] oldBindValues = this.parameterBindings;

            try {
                long[] updateCounts = null;

                if (this.batchedArgs != null) {
                    int nbrCommands = this.batchedArgs.size();
                    updateCounts = new long[nbrCommands];

                    if (this.retrieveGeneratedKeys) {
                        this.batchedGeneratedKeys = new ArrayList<Row>(nbrCommands);
                    }

                    for (int i = 0; i < nbrCommands; i++) {
                        updateCounts[i] = -3;
                    }

                    SQLException sqlEx = null;

                    int commandIndex = 0;

                    BindValue[] previousBindValuesForBatch = null;

                    CancelTask timeoutTask = null;

                    try {
                        if (locallyScopedConn.getPropertySet().getBooleanReadableProperty(PropertyDefinitions.PNAME_enableQueryTimeouts).getValue()
                                && batchTimeout != 0) {
                            timeoutTask = new CancelTask(this);
                            locallyScopedConn.getCancelTimer().schedule(timeoutTask, batchTimeout);
                        }

                        for (commandIndex = 0; commandIndex < nbrCommands; commandIndex++) {
                            Object arg = this.batchedArgs.get(commandIndex);

                            try {
                                if (arg instanceof String) {
                                    updateCounts[commandIndex] = executeUpdateInternal((String) arg, true, this.retrieveGeneratedKeys);

                                    // limit one generated key per OnDuplicateKey statement
                                    getBatchedGeneratedKeys(this.results.getFirstCharOfQuery() == 'I' && containsOnDuplicateKeyInString((String) arg) ? 1 : 0);
                                } else {
                                    this.parameterBindings = ((BatchedBindValues) arg).batchedParameterValues;

                                    // We need to check types each time, as the user might have bound different types in each addBatch()

                                    if (previousBindValuesForBatch != null) {
                                        for (int j = 0; j < this.parameterBindings.length; j++) {
                                            if (this.parameterBindings[j].bufferType != previousBindValuesForBatch[j].bufferType) {
                                                this.sendTypesToServer = true;

                                                break;
                                            }
                                        }
                                    }

                                    try {
                                        updateCounts[commandIndex] = executeUpdateInternal(false, true);
                                    } finally {
                                        previousBindValuesForBatch = this.parameterBindings;
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

                                    throw SQLError.createBatchUpdateException(ex, newUpdateCounts, getExceptionInterceptor());
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
                        throw SQLError.createBatchUpdateException(sqlEx, updateCounts, getExceptionInterceptor());
                    }
                }

                return (updateCounts != null) ? updateCounts : new long[0];
            } finally {
                this.parameterBindings = oldBindValues;
                this.sendTypesToServer = true;

                clearBatch();
            }
        }
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

                    sqlEx = ConnectionImpl.appendMessageToException(sqlEx, messageBuf.toString(), getExceptionInterceptor());
                }

                throw sqlEx;
            } catch (Exception ex) {
                if (this.session.getPropertySet().getBooleanReadableProperty(PropertyDefinitions.PNAME_enablePacketDebug).getValue()) {
                    this.session.dumpPacketRingBuffer();
                }

                SQLException sqlEx = SQLError.createSQLException(ex.toString(), SQLError.SQL_STATE_GENERAL_ERROR, ex, getExceptionInterceptor());

                if (this.dumpQueriesOnException.getValue()) {
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

    @Override
    protected PacketPayload fillSendPacket() throws SQLException {
        return null; // we don't use this type of packet
    }

    @Override
    protected PacketPayload fillSendPacket(byte[][] batchedParameterStrings, InputStream[] batchedParameterStreams, boolean[] batchedIsStream,
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
                throw SQLError.createSQLException(Messages.getString("ServerPreparedStatement.9") + (parameterIndex + 1)
                        + Messages.getString("ServerPreparedStatement.10") + this.parameterBindings.length, SQLError.SQL_STATE_ILLEGAL_ARGUMENT,
                        getExceptionInterceptor());
            }

            if (this.parameterBindings[parameterIndex] == null) {
                this.parameterBindings[parameterIndex] = new BindValue();
            } else {
                if (this.parameterBindings[parameterIndex].isLongData && !forLongData) {
                    this.detectedLongParameterSwitch = true;
                }
            }

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

    byte[] getBytes(int parameterIndex) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            BindValue bindValue = getBinding(parameterIndex, false);

            if (bindValue.isNull) {
                return null;
            } else if (bindValue.isLongData) {
                throw SQLError.createSQLFeatureNotSupportedException();
            } else {
                if (this.outByteBuffer == null) {
                    this.outByteBuffer = new Buffer(this.netBufferLength);
                }

                this.outByteBuffer.setPosition(MysqlaConstants.HEADER_LENGTH);

                int originalPosition = this.outByteBuffer.getPosition();

                storeBinding(this.outByteBuffer, bindValue);

                int newPosition = this.outByteBuffer.getPosition();

                int length = newPosition - originalPosition;

                byte[] valueAsBytes = new byte[length];

                System.arraycopy(this.outByteBuffer.getByteBuffer(), originalPosition, valueAsBytes, 0, length);

                return valueAsBytes;
            }
        }
    }

    @Override
    public java.sql.ResultSetMetaData getMetaData() throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {

            if (this.resultFields == null) {
                return null;
            }

            return new ResultSetMetaData(this.session, this.resultFields,
                    this.session.getPropertySet().getBooleanReadableProperty(PropertyDefinitions.PNAME_useOldAliasMetadataBehavior).getValue(),
                    this.session.getPropertySet().getBooleanReadableProperty(PropertyDefinitions.PNAME_yearIsDateType).getValue(), getExceptionInterceptor());
        }
    }

    @Override
    public ParameterMetaData getParameterMetaData() throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {

            if (this.parameterMetaData == null) {
                this.parameterMetaData = new MysqlParameterMetadata(this.session, this.parameterFields, this.parameterCount, getExceptionInterceptor());
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
                if (this.autoGenerateTestcaseScript.getValue()) {
                    dumpCloseForTestcase();
                }

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

                            PacketPayload packet = this.session.getSharedSendPacket();

                            packet.writeInteger(IntegerDataType.INT1, MysqlaConstants.COM_STMT_CLOSE);
                            packet.writeInteger(IntegerDataType.INT4, this.serverStatementId);

                            this.session.sendCommand(MysqlaConstants.COM_STMT_CLOSE, null, packet, true, null, 0);
                        } catch (CJException sqlEx) {
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
                    if (!this.dontTrackOpenResources.getValue()) {
                        this.connection.unregisterStatement(this);
                    }
                }
            }
        }
    }

    @Override
    boolean isCursorRequired() throws SQLException {
        // we only create cursor-backed result sets if
        // a) The query is a SELECT
        // b) The server supports it
        // c) We know it is forward-only (note this doesn't preclude updatable result sets)
        // d) The user has set a fetch size
        //return this.resultFields != null && this.connection.isCursorFetchEnabled() && getResultSetType() == ResultSet.TYPE_FORWARD_ONLY
        //        && getResultSetConcurrency() == ResultSet.CONCUR_READ_ONLY && getFetchSize() > 0;
        return this.resultFields != null && this.session.getPropertySet().getBooleanReadableProperty(PropertyDefinitions.PNAME_useCursorFetch).getValue()
                && getResultSetType() == ResultSet.TYPE_FORWARD_ONLY && getResultSetConcurrency() == ResultSet.CONCUR_READ_ONLY && getFetchSize() > 0;
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
    private ResultSetInternalMethods serverExecute(int maxRowsToRetrieve, boolean createStreamingResultSet, ColumnDefinition metadata) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            if (this.session.shouldIntercept()) {
                ResultSetInternalMethods interceptedResults = this.session.invokeStatementInterceptorsPre(this.originalSql, this, true);

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

            if (this.autoGenerateTestcaseScript.getValue()) {
                dumpExecuteForTestcase();
            }

            //
            // store the parameter values
            //

            PacketPayload packet = this.session.getSharedSendPacket();
            packet.writeInteger(IntegerDataType.INT1, MysqlaConstants.COM_STMT_EXECUTE);
            packet.writeInteger(IntegerDataType.INT4, this.serverStatementId);

            // we only create cursor-backed result sets if
            // a) The query is a SELECT
            // b) The server supports it
            // c) We know it is forward-only (note this doesn't preclude updatable result sets)
            // d) The user has set a fetch size
            if (this.resultFields != null && this.useCursorFetch && getResultSetType() == ResultSet.TYPE_FORWARD_ONLY && getFetchSize() > 0) {
                packet.writeInteger(IntegerDataType.INT1, OPEN_CURSOR_FLAG);
            } else {
                packet.writeInteger(IntegerDataType.INT1, 0); // placeholder for flags
            }

            packet.writeInteger(IntegerDataType.INT4, 1); // placeholder for parameter iterations

            /* Reserve place for null-marker bytes */
            int nullCount = (this.parameterCount + 7) / 8;

            int nullBitsPosition = packet.getPosition();

            for (int i = 0; i < nullCount; i++) {
                packet.writeInteger(IntegerDataType.INT1, 0);
            }

            byte[] nullBitsBuffer = new byte[nullCount];

            /* In case if buffers (type) altered, indicate to server */
            packet.writeInteger(IntegerDataType.INT1, this.sendTypesToServer ? (byte) 1 : (byte) 0);

            if (this.sendTypesToServer) {
                /*
                 * Store types of parameters in first in first package that is sent to the server.
                 */
                for (int i = 0; i < this.parameterCount; i++) {
                    packet.writeInteger(IntegerDataType.INT2, this.parameterBindings[i].bufferType);
                }
            }

            //
            // store the parameter values
            //
            for (int i = 0; i < this.parameterCount; i++) {
                if (!this.parameterBindings[i].isLongData) {
                    if (!this.parameterBindings[i].isNull) {
                        storeBinding(packet, this.parameterBindings[i]);
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

            CancelTask timeoutTask = null;

            try {
                // Get this before executing to avoid a shared packet pollution in the case some other query is issued internally, such as when using I_S.
                String queryAsString = "";
                if (this.profileSQL || this.logSlowQueries || gatherPerformanceMetrics) {
                    queryAsString = asSql(true);
                }

                if (this.connection.getPropertySet().getBooleanReadableProperty(PropertyDefinitions.PNAME_enableQueryTimeouts).getValue()
                        && this.timeoutInMillis != 0) {
                    timeoutTask = new CancelTask(this);
                    this.connection.getCancelTimer().schedule(timeoutTask, this.timeoutInMillis);
                }

                statementBegins();

                PacketPayload resultPacket = this.session.sendCommand(MysqlaConstants.COM_STMT_EXECUTE, null, packet, false, null, 0);

                long queryEndTime = 0L;

                if (this.logSlowQueries || gatherPerformanceMetrics || this.profileSQL) {
                    queryEndTime = this.session.getCurrentTimeNanosOrMillis();
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

                if (this.logSlowQueries || gatherPerformanceMetrics) {
                    long elapsedTime = queryEndTime - begin;

                    if (this.logSlowQueries) {
                        if (this.useAutoSlowLog) {
                            queryWasSlow = elapsedTime > this.slowQueryThresholdMillis.getValue();
                        } else {
                            queryWasSlow = this.connection.isAbonormallyLongQuery(elapsedTime);

                            this.connection.reportQueryTime(elapsedTime);
                        }
                    }

                    if (queryWasSlow) {

                        StringBuilder mesgBuf = new StringBuilder(48 + this.originalSql.length());
                        mesgBuf.append(Messages.getString("ServerPreparedStatement.15"));
                        mesgBuf.append(this.session.getSlowQueryThreshold());
                        mesgBuf.append(Messages.getString("ServerPreparedStatement.15a"));
                        mesgBuf.append(elapsedTime);
                        mesgBuf.append(Messages.getString("ServerPreparedStatement.16"));

                        mesgBuf.append("as prepared: ");
                        mesgBuf.append(this.originalSql);
                        mesgBuf.append("\n\n with parameters bound:\n\n");
                        mesgBuf.append(queryAsString);

                        this.eventSink.consumeEvent(new ProfilerEventImpl(ProfilerEvent.TYPE_SLOW_QUERY, "", this.getCurrentCatalog(), this.connection.getId(),
                                getId(), 0, System.currentTimeMillis(), elapsedTime, this.session.getQueryTimingUnits(), null,
                                LogUtils.findCallingClassAndMethod(new Throwable()), mesgBuf.toString()));
                    }

                    if (gatherPerformanceMetrics) {
                        this.connection.registerQueryExecutionTime(elapsedTime);
                    }
                }

                this.connection.incrementNumberOfPreparedExecutes();

                if (this.profileSQL) {
                    this.eventSink = ProfilerEventHandlerFactory.getInstance(this.session);

                    this.eventSink.consumeEvent(new ProfilerEventImpl(ProfilerEvent.TYPE_EXECUTE, "", this.getCurrentCatalog(), this.connectionId,
                            this.statementId, -1, System.currentTimeMillis(), this.session.getCurrentTimeNanosOrMillis() - begin,
                            this.session.getQueryTimingUnits(), null, LogUtils.findCallingClassAndMethod(new Throwable()), truncateQueryToLog(queryAsString)));
                }

                com.mysql.cj.api.jdbc.result.ResultSetInternalMethods rs = this.session.getProtocol().readAllResults(maxRowsToRetrieve,
                        createStreamingResultSet, resultPacket, true, metadata, this.resultSetFactory);

                if (this.session.shouldIntercept()) {
                    ResultSetInternalMethods interceptedResults = this.session.invokeStatementInterceptorsPost(this.originalSql, this, rs, true, null);

                    if (interceptedResults != null) {
                        rs = interceptedResults;
                    }
                }

                if (this.profileSQL) {
                    long fetchEndTime = this.session.getCurrentTimeNanosOrMillis();

                    this.eventSink.consumeEvent(new ProfilerEventImpl(ProfilerEvent.TYPE_FETCH, "", this.getCurrentCatalog(), this.connection.getId(), getId(),
                            0 /*
                               * FIXME
                               * rs.
                               * resultId
                               */, System.currentTimeMillis(), (fetchEndTime - queryEndTime), this.session.getQueryTimingUnits(), null,
                            LogUtils.findCallingClassAndMethod(new Throwable()), null));
                }

                if (queryWasSlow && this.explainSlowQueries.getValue()) {
                    this.session.explainSlowQuery(StringUtils.getBytes(queryAsString), queryAsString);
                }

                this.sendTypesToServer = false;
                this.results = rs;

                if (this.session.hadWarnings()) {
                    this.session.getProtocol().scanForAndThrowDataTruncation();
                }

                return rs;
            } catch (IOException ioEx) {
                throw SQLError.createCommunicationsException(this.connection, this.session.getProtocol().getPacketSentTimeHolder().getLastPacketSentTime(),
                        this.session.getProtocol().getPacketReceivedTimeHolder().getLastPacketReceivedTime(), ioEx, getExceptionInterceptor());
            } catch (SQLException | CJException sqlEx) {
                if (this.session.shouldIntercept()) {
                    this.session.invokeStatementInterceptorsPost(this.originalSql, this, null, true, sqlEx);
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
    private void serverLongData(int parameterIndex, BindValue longData) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            PacketPayload packet = this.session.getSharedSendPacket();

            Object value = longData.value;

            if (value instanceof byte[]) {
                packet.writeInteger(IntegerDataType.INT1, MysqlaConstants.COM_STMT_SEND_LONG_DATA);
                packet.writeInteger(IntegerDataType.INT4, this.serverStatementId);
                packet.writeInteger(IntegerDataType.INT2, parameterIndex);

                packet.writeBytes(StringLengthDataType.STRING_FIXED, (byte[]) longData.value);

                this.session.sendCommand(MysqlaConstants.COM_STMT_SEND_LONG_DATA, null, packet, true, null, 0);
            } else if (value instanceof InputStream) {
                storeStream(parameterIndex, packet, (InputStream) value);
            } else if (value instanceof java.sql.Blob) {
                storeStream(parameterIndex, packet, ((java.sql.Blob) value).getBinaryStream());
            } else if (value instanceof Reader) {
                storeReader(parameterIndex, packet, (Reader) value);
            } else {
                throw SQLError.createSQLException(Messages.getString("ServerPreparedStatement.18") + value.getClass().getName() + "'",
                        SQLError.SQL_STATE_ILLEGAL_ARGUMENT, getExceptionInterceptor());
            }
        }
    }

    private void serverPrepare(String sql) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            if (this.autoGenerateTestcaseScript.getValue()) {
                dumpPrepareForTestcase();
            }

            try {
                long begin = 0;

                if (StringUtils.startsWithIgnoreCaseAndWs(sql, "LOAD DATA")) {
                    this.isLoadDataQuery = true;
                } else {
                    this.isLoadDataQuery = false;
                }

                if (this.profileSQL) {
                    begin = System.currentTimeMillis();
                }

                String characterEncoding = null;
                String connectionEncoding = this.session.getPropertySet().getStringReadableProperty(PropertyDefinitions.PNAME_characterEncoding).getValue();

                if (!this.isLoadDataQuery && (connectionEncoding != null)) {
                    characterEncoding = connectionEncoding;
                }

                PacketPayload prepareResultPacket = this.session.sendCommand(MysqlaConstants.COM_STMT_PREPARE, sql, null, false, characterEncoding, 0);

                // 4.1.1 and newer use the first byte as an 'ok' or 'error' flag, so move the buffer pointer past it to start reading the statement id.
                prepareResultPacket.setPosition(1);

                this.serverStatementId = prepareResultPacket.readInteger(IntegerDataType.INT4);
                this.fieldCount = (int) prepareResultPacket.readInteger(IntegerDataType.INT2);
                this.parameterCount = (int) prepareResultPacket.readInteger(IntegerDataType.INT2);
                this.parameterBindings = new BindValue[this.parameterCount];

                for (int i = 0; i < this.parameterCount; i++) {
                    this.parameterBindings[i] = new BindValue();
                }

                this.connection.incrementNumberOfPrepares();

                if (this.profileSQL) {
                    this.eventSink.consumeEvent(new ProfilerEventImpl(ProfilerEvent.TYPE_PREPARE, "", this.getCurrentCatalog(), this.connectionId,
                            this.statementId, -1, System.currentTimeMillis(), this.session.getCurrentTimeNanosOrMillis() - begin,
                            this.session.getQueryTimingUnits(), null, LogUtils.findCallingClassAndMethod(new Throwable()), truncateQueryToLog(sql)));
                }

                boolean checkEOF = !this.session.getServerSession().isEOFDeprecated();

                if (this.parameterCount > 0) {
                    //this.parameterFields = new Field[this.parameterCount];

                    //PacketPayload metaDataPacket;
                    //for (int i = 0; i < this.parameterCount; i++) {
                    //    metaDataPacket = this.session.readPacket();
                    //    this.parameterFields[i] = this.session.getResultsHandler().unpackField(metaDataPacket, this.connection.getCharacterSetMetadata());
                    //}
                    if (checkEOF) { // Skip the following EOF packet.
                        this.session.readPacket();
                    }

                    this.parameterFields = this.session.getProtocol().read(ColumnDefinition.class, new ColumnDefinitionFactory(this.parameterCount, null))
                            .getFields();

                }

                // Read in the result set column information
                if (this.fieldCount > 0) {
                    //this.resultFields = new Field[this.fieldCount];

                    //PacketPayload fieldPacket;
                    //for (int i = 0; i < this.fieldCount; i++) {
                    //    fieldPacket = this.session.readPacket();
                    //    this.resultFields[i] = this.session.getResultsHandler().unpackField(fieldPacket, this.connection.getCharacterSetMetadata());
                    //}
                    //if (checkEOF) { // Skip the following EOF packet.
                    //    this.session.readPacket();
                    //}
                    this.resultFields = this.session.getProtocol().read(ColumnDefinition.class, new ColumnDefinitionFactory(this.fieldCount, null)).getFields();
                }
            } catch (IOException ioEx) {
                throw SQLError.createCommunicationsException(this.session.getProtocol().getConnection(),
                        this.session.getProtocol().getPacketSentTimeHolder().getLastPacketSentTime(),
                        this.session.getProtocol().getPacketReceivedTimeHolder().getLastPacketReceivedTime(), ioEx, this.session.getExceptionInterceptor());
            } catch (SQLException | CJException sqlEx) {
                SQLException ex = sqlEx instanceof SQLException ? (SQLException) sqlEx : SQLExceptionsMapping.translateException(sqlEx);

                if (this.dumpQueriesOnException.getValue()) {
                    StringBuilder messageBuf = new StringBuilder(this.originalSql.length() + 32);
                    messageBuf.append("\n\nQuery being prepared when exception was thrown:\n\n");
                    messageBuf.append(this.originalSql);

                    ex = ConnectionImpl.appendMessageToException(ex, messageBuf.toString(), getExceptionInterceptor());
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
            String query = null;

            int maxQuerySizeToLog = this.session.getPropertySet().getIntegerReadableProperty(PropertyDefinitions.PNAME_maxQuerySizeToLog).getValue();
            if (sql.length() > maxQuerySizeToLog) {
                StringBuilder queryBuf = new StringBuilder(maxQuerySizeToLog + 12);
                queryBuf.append(sql.substring(0, maxQuerySizeToLog));
                queryBuf.append(Messages.getString("MysqlIO.25"));

                query = queryBuf.toString();
            } else {
                query = sql;
            }

            return query;
        }
    }

    private void serverResetStatement() {
        synchronized (checkClosed().getConnectionMutex()) {

            PacketPayload packet = this.session.getSharedSendPacket();

            packet.writeInteger(IntegerDataType.INT1, MysqlaConstants.COM_STMT_RESET);
            packet.writeInteger(IntegerDataType.INT4, this.serverStatementId);

            try {
                this.session.sendCommand(MysqlaConstants.COM_STMT_RESET, null, packet, false, null, 0);
            } finally {
                this.session.clearInputStream();
            }
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
                BindValue binding = getBinding(parameterIndex, true);
                resetToType(binding, MysqlaConstants.FIELD_TYPE_BLOB);

                binding.value = x;
                binding.isLongData = true;

                if (this.useStreamLengthsInPrepStmts.getValue()) {
                    binding.bindLength = length;
                } else {
                    binding.bindLength = -1;
                }
            }
        }
    }

    @Override
    public void setBigDecimal(int parameterIndex, BigDecimal x) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {

            if (x == null) {
                setNull(parameterIndex, MysqlType.DECIMAL);
            } else {

                BindValue binding = getBinding(parameterIndex, false);
                resetToType(binding, MysqlaConstants.FIELD_TYPE_NEWDECIMAL);

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
                BindValue binding = getBinding(parameterIndex, true);
                resetToType(binding, MysqlaConstants.FIELD_TYPE_BLOB);

                binding.value = x;
                binding.isLongData = true;

                if (this.useStreamLengthsInPrepStmts.getValue()) {
                    binding.bindLength = length;
                } else {
                    binding.bindLength = -1;
                }
            }
        }
    }

    @Override
    public void setBlob(int parameterIndex, Blob x) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {

            if (x == null) {
                setNull(parameterIndex, MysqlType.BINARY);
            } else {
                BindValue binding = getBinding(parameterIndex, true);
                resetToType(binding, MysqlaConstants.FIELD_TYPE_BLOB);

                binding.value = x;
                binding.isLongData = true;

                if (this.useStreamLengthsInPrepStmts.getValue()) {
                    binding.bindLength = x.length();
                } else {
                    binding.bindLength = -1;
                }
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

        BindValue binding = getBinding(parameterIndex, false);
        resetToType(binding, MysqlaConstants.FIELD_TYPE_TINY);

        binding.longBinding = x;
    }

    @Override
    public void setBytes(int parameterIndex, byte[] x) throws SQLException {
        checkClosed();

        if (x == null) {
            setNull(parameterIndex, MysqlType.BINARY);
        } else {
            BindValue binding = getBinding(parameterIndex, false);
            resetToType(binding, MysqlaConstants.FIELD_TYPE_VAR_STRING);

            binding.value = x;
        }
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, int length) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {

            if (reader == null) {
                setNull(parameterIndex, MysqlType.BINARY);
            } else {
                BindValue binding = getBinding(parameterIndex, true);
                resetToType(binding, MysqlaConstants.FIELD_TYPE_BLOB);

                binding.value = reader;
                binding.isLongData = true;

                if (this.useStreamLengthsInPrepStmts.getValue()) {
                    binding.bindLength = length;
                } else {
                    binding.bindLength = -1;
                }
            }
        }
    }

    @Override
    public void setClob(int parameterIndex, Clob x) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {

            if (x == null) {
                setNull(parameterIndex, MysqlType.BINARY);
            } else {
                BindValue binding = getBinding(parameterIndex, true);
                resetToType(binding, MysqlaConstants.FIELD_TYPE_BLOB);

                binding.value = x.getCharacterStream();
                binding.isLongData = true;

                if (this.useStreamLengthsInPrepStmts.getValue()) {
                    binding.bindLength = x.length();
                } else {
                    binding.bindLength = -1;
                }
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
            BindValue binding = getBinding(parameterIndex, false);
            resetToType(binding, MysqlaConstants.FIELD_TYPE_DATE);

            binding.value = x;
            binding.tz = tz;
        }
    }

    @Override
    public void setDouble(int parameterIndex, double x) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {

            if (!this.session.getPropertySet().getBooleanReadableProperty(PropertyDefinitions.PNAME_allowNanAndInf).getValue()
                    && (x == Double.POSITIVE_INFINITY || x == Double.NEGATIVE_INFINITY || Double.isNaN(x))) {
                throw SQLError.createSQLException(Messages.getString("PreparedStatement.64", new Object[] { x }), SQLError.SQL_STATE_ILLEGAL_ARGUMENT,
                        getExceptionInterceptor());

            }

            BindValue binding = getBinding(parameterIndex, false);
            resetToType(binding, MysqlaConstants.FIELD_TYPE_DOUBLE);

            binding.doubleBinding = x;
        }
    }

    @Override
    public void setFloat(int parameterIndex, float x) throws SQLException {
        checkClosed();

        BindValue binding = getBinding(parameterIndex, false);
        resetToType(binding, MysqlaConstants.FIELD_TYPE_FLOAT);

        binding.floatBinding = x;
    }

    @Override
    public void setInt(int parameterIndex, int x) throws SQLException {
        checkClosed();

        BindValue binding = getBinding(parameterIndex, false);
        resetToType(binding, MysqlaConstants.FIELD_TYPE_LONG);

        binding.longBinding = x;
    }

    @Override
    public void setLong(int parameterIndex, long x) throws SQLException {
        checkClosed();

        BindValue binding = getBinding(parameterIndex, false);
        resetToType(binding, MysqlaConstants.FIELD_TYPE_LONGLONG);

        binding.longBinding = x;
    }

    @Override
    public void setNull(int parameterIndex, int sqlType) throws SQLException {
        checkClosed();

        BindValue binding = getBinding(parameterIndex, false);
        resetToType(binding, MysqlaConstants.FIELD_TYPE_NULL);

        binding.isNull = true;
    }

    @Override
    public void setNull(int parameterIndex, int sqlType, String typeName) throws SQLException {
        checkClosed();

        BindValue binding = getBinding(parameterIndex, false);
        resetToType(binding, MysqlaConstants.FIELD_TYPE_NULL);

        binding.isNull = true;
    }

    @Override
    public void setRef(int i, Ref x) throws SQLException {
        throw SQLError.createSQLFeatureNotSupportedException();
    }

    @Override
    public void setShort(int parameterIndex, short x) throws SQLException {
        checkClosed();

        BindValue binding = getBinding(parameterIndex, false);
        resetToType(binding, MysqlaConstants.FIELD_TYPE_SHORT);

        binding.longBinding = x;
    }

    @Override
    public void setString(int parameterIndex, String x) throws SQLException {
        checkClosed();

        if (x == null) {
            setNull(parameterIndex, MysqlType.VARCHAR);
        } else {
            BindValue binding = getBinding(parameterIndex, false);
            resetToType(binding, MysqlaConstants.FIELD_TYPE_VAR_STRING);

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
            BindValue binding = getBinding(parameterIndex, false);
            resetToType(binding, MysqlaConstants.FIELD_TYPE_TIME);

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
            BindValue binding = getBinding(parameterIndex, false);
            resetToType(binding, MysqlaConstants.FIELD_TYPE_DATETIME);

            if (!this.sendFractionalSeconds.getValue()) {
                x = TimeUtil.truncateFractionalSeconds(x);
            }

            binding.value = x;
            binding.tz = tz;
        }
    }

    /**
     * Reset a bind value to be used for a new value of the given type.
     */
    protected void resetToType(BindValue oldValue, int bufferType) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            // clear any possible old value
            oldValue.reset();

            if (bufferType == MysqlaConstants.FIELD_TYPE_NULL && oldValue.bufferType != 0) {
                // preserve the previous type to (possibly) avoid sending types at execution time
            } else if (oldValue.bufferType != bufferType) {
                this.sendTypesToServer = true;
                oldValue.bufferType = bufferType;
            }

            // setup bind value for use
            oldValue.isSet = true;
            oldValue.boundBeforeExecutionNum = this.numberOfExecutions;
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
    private void storeBinding(PacketPayload packet, BindValue bindValue) throws SQLException {
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
                        } else if (!this.isLoadDataQuery) {
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

    // TODO: Investigate using NIO to do this faster
    private void storeReader(int parameterIndex, PacketPayload packet, Reader inStream) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            String forcedEncoding = this.session.getPropertySet().getStringReadableProperty(PropertyDefinitions.PNAME_clobCharacterEncoding).getStringValue();

            String clobEncoding = (forcedEncoding == null
                    ? this.session.getPropertySet().getStringReadableProperty(PropertyDefinitions.PNAME_characterEncoding).getValue() : forcedEncoding);

            int maxBytesChar = 2;

            if (clobEncoding != null) {
                if (!clobEncoding.equals("UTF-16")) {
                    maxBytesChar = this.session.getMaxBytesPerChar(clobEncoding);

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
            int packetIsFullAt = this.session.getPropertySet().getMemorySizeReadableProperty(PropertyDefinitions.PNAME_blobSendChunkSize).getValue();

            try {
                packet.setPosition(0);
                packet.writeInteger(IntegerDataType.INT1, MysqlaConstants.COM_STMT_SEND_LONG_DATA);
                packet.writeInteger(IntegerDataType.INT4, this.serverStatementId);
                packet.writeInteger(IntegerDataType.INT2, parameterIndex);

                boolean readAny = false;

                while ((numRead = inStream.read(buf)) != -1) {
                    readAny = true;

                    byte[] valueAsBytes = StringUtils.getBytes(buf, 0, numRead, clobEncoding);

                    packet.writeBytes(StringSelfDataType.STRING_EOF, valueAsBytes);

                    bytesInPacket += valueAsBytes.length;
                    totalBytesRead += valueAsBytes.length;

                    if (bytesInPacket >= packetIsFullAt) {
                        bytesReadAtLastSend = totalBytesRead;

                        this.session.sendCommand(MysqlaConstants.COM_STMT_SEND_LONG_DATA, null, packet, true, null, 0);

                        bytesInPacket = 0;
                        packet.setPosition(0);
                        packet.writeInteger(IntegerDataType.INT1, MysqlaConstants.COM_STMT_SEND_LONG_DATA);
                        packet.writeInteger(IntegerDataType.INT4, this.serverStatementId);
                        packet.writeInteger(IntegerDataType.INT2, parameterIndex);
                    }
                }

                if (totalBytesRead != bytesReadAtLastSend) {
                    this.session.sendCommand(MysqlaConstants.COM_STMT_SEND_LONG_DATA, null, packet, true, null, 0);
                }

                if (!readAny) {
                    this.session.sendCommand(MysqlaConstants.COM_STMT_SEND_LONG_DATA, null, packet, true, null, 0);
                }
            } catch (IOException ioEx) {
                SQLException sqlEx = SQLError.createSQLException(Messages.getString("ServerPreparedStatement.24") + ioEx.toString(),
                        SQLError.SQL_STATE_GENERAL_ERROR, getExceptionInterceptor());
                sqlEx.initCause(ioEx);

                throw sqlEx;
            } finally {
                if (this.autoClosePStmtStreams.getValue()) {
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

    private void storeStream(int parameterIndex, PacketPayload packet, InputStream inStream) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            byte[] buf = new byte[BLOB_STREAM_READ_BUF_SIZE];

            int numRead = 0;

            try {
                int bytesInPacket = 0;
                int totalBytesRead = 0;
                int bytesReadAtLastSend = 0;
                int packetIsFullAt = this.session.getPropertySet().getMemorySizeReadableProperty(PropertyDefinitions.PNAME_blobSendChunkSize).getValue();

                packet.setPosition(0);
                packet.writeInteger(IntegerDataType.INT1, MysqlaConstants.COM_STMT_SEND_LONG_DATA);
                packet.writeInteger(IntegerDataType.INT4, this.serverStatementId);
                packet.writeInteger(IntegerDataType.INT2, parameterIndex);

                boolean readAny = false;

                while ((numRead = inStream.read(buf)) != -1) {

                    readAny = true;

                    packet.writeBytes(StringLengthDataType.STRING_FIXED, buf, 0, numRead);
                    bytesInPacket += numRead;
                    totalBytesRead += numRead;

                    if (bytesInPacket >= packetIsFullAt) {
                        bytesReadAtLastSend = totalBytesRead;

                        this.session.sendCommand(MysqlaConstants.COM_STMT_SEND_LONG_DATA, null, packet, true, null, 0);

                        bytesInPacket = 0;
                        packet.setPosition(0);
                        packet.writeInteger(IntegerDataType.INT1, MysqlaConstants.COM_STMT_SEND_LONG_DATA);
                        packet.writeInteger(IntegerDataType.INT4, this.serverStatementId);
                        packet.writeInteger(IntegerDataType.INT2, parameterIndex);
                    }
                }

                if (totalBytesRead != bytesReadAtLastSend) {
                    this.session.sendCommand(MysqlaConstants.COM_STMT_SEND_LONG_DATA, null, packet, true, null, 0);
                }

                if (!readAny) {
                    this.session.sendCommand(MysqlaConstants.COM_STMT_SEND_LONG_DATA, null, packet, true, null, 0);
                }
            } catch (IOException ioEx) {
                SQLException sqlEx = SQLError.createSQLException(Messages.getString("ServerPreparedStatement.25") + ioEx.toString(),
                        SQLError.SQL_STATE_GENERAL_ERROR, getExceptionInterceptor());
                sqlEx.initCause(ioEx);

                throw sqlEx;
            } finally {
                if (this.autoClosePStmtStreams.getValue()) {
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

    @Override
    public String toString() {
        StringBuilder toStringBuf = new StringBuilder();

        toStringBuf.append("com.mysql.cj.jdbc.ServerPreparedStatement[");
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

    @Override
    public long getServerStatementId() {
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
                this.locationOfOnDuplicateKeyUpdate = getOnDuplicateKeyLocation(this.originalSql, this.dontCheckOnDuplicateKeyUpdateInSQL,
                        this.rewriteBatchedStatements.getValue(), this.connection.isNoBackslashEscapesSet());
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
            long sizeOfEntireBatch = 1 + /* com_execute */+4 /* stmt id */ + 1 /* flags */ + 4 /* batch count padding */;
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
    protected PreparedStatement prepareBatchedInsertSQL(JdbcConnection localConn, int numBatches) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            try {
                //PreparedStatement pstmt = new ServerPreparedStatement(localConn, this.parseInfo.getSqlForBatch(numBatches), this.getCurrentCatalog(), this.resultSetConcurrency, this.resultSetType);
                PreparedStatement pstmt = ((Wrapper) localConn.prepareStatement(this.parseInfo.getSqlForBatch(numBatches), this.resultSetConcurrency,
                        this.resultSetType)).unwrap(PreparedStatement.class);
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

    @Override
    public void setNCharacterStream(int parameterIndex, Reader reader, long length) throws SQLException {
        // can't take if characterEncoding isn't utf8
        if (!this.charEncoding.equalsIgnoreCase("UTF-8") && !this.charEncoding.equalsIgnoreCase("utf8")) {
            throw SQLError.createSQLException(Messages.getString("ServerPreparedStatement.28"), getExceptionInterceptor());
        }

        checkClosed();

        if (reader == null) {
            setNull(parameterIndex, MysqlType.BINARY);
        } else {
            BindValue binding = getBinding(parameterIndex, true);
            resetToType(binding, MysqlaConstants.FIELD_TYPE_BLOB);

            binding.value = reader;
            binding.isLongData = true;

            if (this.useStreamLengthsInPrepStmts.getValue()) {
                binding.bindLength = length;
            } else {
                binding.bindLength = -1;
            }
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
            throw SQLError.createSQLException(Messages.getString("ServerPreparedStatement.29"), getExceptionInterceptor());
        }

        checkClosed();

        if (reader == null) {
            setNull(parameterIndex, MysqlType.TEXT);
        } else {
            BindValue binding = getBinding(parameterIndex, true);
            resetToType(binding, MysqlaConstants.FIELD_TYPE_BLOB);

            binding.value = reader;
            binding.isLongData = true;

            if (this.useStreamLengthsInPrepStmts.getValue()) {
                binding.bindLength = length;
            } else {
                binding.bindLength = -1;
            }
        }
    }

    @Override
    public void setNString(int parameterIndex, String x) throws SQLException {
        if (this.charEncoding.equalsIgnoreCase("UTF-8") || this.charEncoding.equalsIgnoreCase("utf8")) {
            setString(parameterIndex, x);
        } else {
            throw SQLError.createSQLException(Messages.getString("ServerPreparedStatement.30"), getExceptionInterceptor());
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
