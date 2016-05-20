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

package com.mysql.jdbc;

import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.ParameterMetaData;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.TimeZone;

import com.mysql.jdbc.exceptions.MySQLStatementCancelledException;
import com.mysql.jdbc.exceptions.MySQLTimeoutException;
import com.mysql.jdbc.log.LogUtils;
import com.mysql.jdbc.profiler.ProfilerEvent;

/**
 * JDBC Interface for MySQL-4.1 and newer server-side PreparedStatements.
 */
public class ServerPreparedStatement extends PreparedStatement {
    private static final Constructor<?> JDBC_4_SPS_CTOR;

    static {
        if (Util.isJdbc4()) {
            try {
                String jdbc4ClassName = Util.isJdbc42() ? "com.mysql.jdbc.JDBC42ServerPreparedStatement" : "com.mysql.jdbc.JDBC4ServerPreparedStatement";
                JDBC_4_SPS_CTOR = Class.forName(jdbc4ClassName)
                        .getConstructor(new Class[] { MySQLConnection.class, String.class, String.class, Integer.TYPE, Integer.TYPE });
            } catch (SecurityException e) {
                throw new RuntimeException(e);
            } catch (NoSuchMethodException e) {
                throw new RuntimeException(e);
            } catch (ClassNotFoundException e) {
                throw new RuntimeException(e);
            }
        } else {
            JDBC_4_SPS_CTOR = null;
        }
    }

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
        }

        void reset() {
            this.isNull = false;
            this.isSet = false;
            this.value = null;
            this.isLongData = false;

            this.longBinding = 0L;
            this.floatBinding = 0;
            this.doubleBinding = 0D;
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

    /* 1 (length) + 2 (year) + 1 (month) + 1 (day) */
    //private static final byte MAX_DATE_REP_LENGTH = (byte) 5;

    /*
     * 1 (length) + 2 (year) + 1 (month) + 1 (day) + 1 (hour) + 1 (minute) + 1
     * (second) + 4 (microseconds)
     */
    //private static final byte MAX_DATETIME_REP_LENGTH = 12;

    /*
     * 1 (length) + 1 (is negative) + 4 (day count) + 1 (hour) + 1 (minute) + 1
     * (seconds) + 4 (microseconds)
     */
    //private static final byte MAX_TIME_REP_LENGTH = 13;

    private boolean hasOnDuplicateKeyUpdate = false;

    private void storeTime(Buffer intoBuf, Time tm) throws SQLException {

        intoBuf.ensureCapacity(9);
        intoBuf.writeByte((byte) 8); // length
        intoBuf.writeByte((byte) 0); // neg flag
        intoBuf.writeLong(0); // tm->day, not used

        Calendar sessionCalendar = getCalendarInstanceForSessionOrNew();

        synchronized (sessionCalendar) {
            java.util.Date oldTime = sessionCalendar.getTime();
            try {
                sessionCalendar.setTime(tm);
                intoBuf.writeByte((byte) sessionCalendar.get(Calendar.HOUR_OF_DAY));
                intoBuf.writeByte((byte) sessionCalendar.get(Calendar.MINUTE));
                intoBuf.writeByte((byte) sessionCalendar.get(Calendar.SECOND));

                // intoBuf.writeLongInt(0); // tm-second_part
            } finally {
                sessionCalendar.setTime(oldTime);
            }
        }
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
    private SQLException invalidationException;

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

    /** The type used for string bindings, changes from version-to-version */
    private int stringTypeCode = MysqlDefs.FIELD_TYPE_STRING;

    private boolean serverNeedsResetBeforeEachExecution;

    /**
     * Creates a prepared statement instance -- We need to provide factory-style
     * methods so we can support both JDBC3 (and older) and JDBC4 runtimes,
     * otherwise the class verifier complains when it tries to load JDBC4-only
     * interface classes that are present in JDBC4 method signatures.
     */

    protected static ServerPreparedStatement getInstance(MySQLConnection conn, String sql, String catalog, int resultSetType, int resultSetConcurrency)
            throws SQLException {
        if (!Util.isJdbc4()) {
            return new ServerPreparedStatement(conn, sql, catalog, resultSetType, resultSetConcurrency);
        }

        try {
            return (ServerPreparedStatement) JDBC_4_SPS_CTOR
                    .newInstance(new Object[] { conn, sql, catalog, Integer.valueOf(resultSetType), Integer.valueOf(resultSetConcurrency) });
        } catch (IllegalArgumentException e) {
            throw new SQLException(e.toString(), SQLError.SQL_STATE_GENERAL_ERROR);
        } catch (InstantiationException e) {
            throw new SQLException(e.toString(), SQLError.SQL_STATE_GENERAL_ERROR);
        } catch (IllegalAccessException e) {
            throw new SQLException(e.toString(), SQLError.SQL_STATE_GENERAL_ERROR);
        } catch (InvocationTargetException e) {
            Throwable target = e.getTargetException();

            if (target instanceof SQLException) {
                throw (SQLException) target;
            }

            throw new SQLException(target.toString(), SQLError.SQL_STATE_GENERAL_ERROR);
        }
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
    protected ServerPreparedStatement(MySQLConnection conn, String sql, String catalog, int resultSetType, int resultSetConcurrency) throws SQLException {
        super(conn, catalog);

        checkNullOrEmptyQuery(sql);

        int startOfStatement = findStartOfStatement(sql);

        this.firstCharOfStmt = StringUtils.firstAlphaCharUc(sql, startOfStatement);

        this.hasOnDuplicateKeyUpdate = this.firstCharOfStmt == 'I' && containsOnDuplicateKeyInString(sql);

        if (this.connection.versionMeetsMinimum(5, 0, 0)) {
            this.serverNeedsResetBeforeEachExecution = !this.connection.versionMeetsMinimum(5, 0, 3);
        } else {
            this.serverNeedsResetBeforeEachExecution = !this.connection.versionMeetsMinimum(4, 1, 10);
        }

        this.useAutoSlowLog = this.connection.getAutoSlowLog();
        this.useTrueBoolean = this.connection.versionMeetsMinimum(3, 21, 23);

        String statementComment = this.connection.getStatementComment();

        this.originalSql = (statementComment == null) ? sql : "/* " + statementComment + " */ " + sql;

        if (this.connection.versionMeetsMinimum(4, 1, 2)) {
            this.stringTypeCode = MysqlDefs.FIELD_TYPE_VAR_STRING;
        } else {
            this.stringTypeCode = MysqlDefs.FIELD_TYPE_STRING;
        }

        try {
            serverPrepare(sql);
        } catch (SQLException sqlEx) {
            realClose(false, true);
            // don't wrap SQLExceptions
            throw sqlEx;
        } catch (Exception ex) {
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

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.Statement#checkClosed()
     */
    @Override
    protected MySQLConnection checkClosed() throws SQLException {
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

    private Calendar serverTzCalendar;

    private Calendar defaultTzCalendar;

    protected void setClosed(boolean flag) {
        this.isClosed = flag;
    }

    /**
     * @see java.sql.Statement#close()
     */
    @Override
    public void close() throws SQLException {
        MySQLConnection locallyScopedConn = this.connection;

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

            this.connection.dumpTestcaseQuery(buf.toString());
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

            this.connection.dumpTestcaseQuery(buf.toString());
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

            this.connection.dumpTestcaseQuery(buf.toString());
        }
    }

    @Override
    protected long[] executeBatchSerially(int batchTimeout) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            MySQLConnection locallyScopedConn = this.connection;

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
                        if (locallyScopedConn.getEnableQueryTimeouts() && batchTimeout != 0 && locallyScopedConn.versionMeetsMinimum(5, 0, 0)) {
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

    /**
     * @see com.mysql.jdbc.PreparedStatement#executeInternal(int, com.mysql.jdbc.Buffer, boolean, boolean)
     */
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

                SQLException sqlEx = SQLError.createSQLException(ex.toString(), SQLError.SQL_STATE_GENERAL_ERROR, getExceptionInterceptor());

                if (this.connection.getDumpQueriesOnException()) {
                    String extractedSql = toString();
                    StringBuilder messageBuf = new StringBuilder(extractedSql.length() + 32);
                    messageBuf.append("\n\nQuery being executed when exception was thrown:\n");
                    messageBuf.append(extractedSql);
                    messageBuf.append("\n\n");

                    sqlEx = ConnectionImpl.appendMessageToException(sqlEx, messageBuf.toString(), getExceptionInterceptor());
                }

                sqlEx.initCause(ex);

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

    /**
     * @see com.mysql.jdbc.PreparedStatement#getBytes(int)
     */
    byte[] getBytes(int parameterIndex) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            BindValue bindValue = getBinding(parameterIndex, false);

            if (bindValue.isNull) {
                return null;
            } else if (bindValue.isLongData) {
                throw SQLError.createSQLFeatureNotSupportedException();
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
        MySQLConnection locallyScopedConn = this.connection;

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
     * @throws SQLException
     *             if an error occurs.
     */
    protected void rePrepare() throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            this.invalidationException = null;

            try {
                serverPrepare(this.originalSql);
            } catch (SQLException sqlEx) {
                // don't wrap SQLExceptions
                this.invalidationException = sqlEx;
            } catch (Exception ex) {
                this.invalidationException = SQLError.createSQLException(ex.toString(), SQLError.SQL_STATE_GENERAL_ERROR, getExceptionInterceptor());
                this.invalidationException.initCause(ex);
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

    @Override
    boolean isCursorRequired() throws SQLException {
        // we only create cursor-backed result sets if
        // a) The query is a SELECT
        // b) The server supports it
        // c) We know it is forward-only (note this doesn't preclude updatable result sets)
        // d) The user has set a fetch size
        return this.resultFields != null && this.connection.isCursorFetchEnabled() && getResultSetType() == ResultSet.TYPE_FORWARD_ONLY
                && getResultSetConcurrency() == ResultSet.CONCUR_READ_ONLY && getFetchSize() > 0;
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

            packet.clear();
            packet.writeByte((byte) MysqlDefs.COM_EXECUTE);
            packet.writeLong(this.serverStatementId);

            if (this.connection.versionMeetsMinimum(4, 1, 2)) {
                if (isCursorRequired()) {
                    packet.writeByte(MysqlDefs.OPEN_CURSOR_FLAG);
                } else {
                    packet.writeByte((byte) 0); // placeholder for flags
                }

                packet.writeLong(1); // placeholder for parameter iterations
            }

            /* Reserve place for null-marker bytes */
            int nullCount = (this.parameterCount + 7) / 8;

            // if (mysql.versionMeetsMinimum(4, 1, 2)) {
            // nullCount = (this.parameterCount + 9) / 8;
            // }
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
            boolean gatherPerformanceMetrics = this.connection.getGatherPerformanceMetrics();

            if (this.profileSQL || logSlowQueries || gatherPerformanceMetrics) {
                begin = mysql.getCurrentTimeNanosOrMillis();
            }

            resetCancelledState();

            CancelTask timeoutTask = null;

            try {
                // Get this before executing to avoid a shared packet pollution in the case some other query is issued internally, such as when using I_S.
                String queryAsString = "";
                if (this.profileSQL || logSlowQueries || gatherPerformanceMetrics) {
                    queryAsString = asSql(true);
                }

                if (this.connection.getEnableQueryTimeouts() && this.timeoutInMillis != 0 && this.connection.versionMeetsMinimum(5, 0, 0)) {
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
                        mesgBuf.append(queryAsString);

                        this.eventSink.consumeEvent(new ProfilerEvent(ProfilerEvent.TYPE_SLOW_QUERY, "", this.currentCatalog, this.connection.getId(), getId(),
                                0, System.currentTimeMillis(), elapsedTime, mysql.getQueryTimingUnits(), null,
                                LogUtils.findCallingClassAndMethod(new Throwable()), mesgBuf.toString()));
                    }

                    if (gatherPerformanceMetrics) {
                        this.connection.registerQueryExecutionTime(elapsedTime);
                    }
                }

                this.connection.incrementNumberOfPreparedExecutes();

                if (this.profileSQL) {
                    this.eventSink = ProfilerEventHandlerFactory.getInstance(this.connection);

                    this.eventSink.consumeEvent(new ProfilerEvent(ProfilerEvent.TYPE_EXECUTE, "", this.currentCatalog, this.connectionId, this.statementId, -1,
                            System.currentTimeMillis(), mysql.getCurrentTimeNanosOrMillis() - begin, mysql.getQueryTimingUnits(), null,
                            LogUtils.findCallingClassAndMethod(new Throwable()), truncateQueryToLog(queryAsString)));
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

                    this.eventSink.consumeEvent(new ProfilerEvent(ProfilerEvent.TYPE_FETCH, "", this.currentCatalog, this.connection.getId(), getId(), 0 /*
                                                                                                                                                          * FIXME
                                                                                                                                                          * rs.
                                                                                                                                                          * resultId
                                                                                                                                                          */,
                            System.currentTimeMillis(), (fetchEndTime - queryEndTime), mysql.getQueryTimingUnits(), null,
                            LogUtils.findCallingClassAndMethod(new Throwable()), null));
                }

                if (queryWasSlow && this.connection.getExplainSlowQueries()) {
                    mysql.explainSlowQuery(StringUtils.getBytes(queryAsString), queryAsString);
                }

                if (!createStreamingResultSet && this.serverNeedsResetBeforeEachExecution) {
                    serverResetStatement(); // clear any long data...
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
                packet.clear();
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

                if (this.connection.getProfileSql()) {
                    begin = System.currentTimeMillis();
                }

                String characterEncoding = null;
                String connectionEncoding = this.connection.getEncoding();

                if (!this.isLoadDataQuery && this.connection.getUseUnicode() && (connectionEncoding != null)) {
                    characterEncoding = connectionEncoding;
                }

                Buffer prepareResultPacket = mysql.sendCommand(MysqlDefs.COM_PREPARE, sql, null, false, characterEncoding, 0);

                if (this.connection.versionMeetsMinimum(4, 1, 1)) {
                    // 4.1.1 and newer use the first byte as an 'ok' or 'error' flag, so move the buffer pointer past it to start reading the statement id.
                    prepareResultPacket.setPosition(1);
                } else {
                    // 4.1.0 doesn't use the first byte as an 'ok' or 'error' flag
                    prepareResultPacket.setPosition(0);
                }

                this.serverStatementId = prepareResultPacket.readLong();
                this.fieldCount = prepareResultPacket.readInt();
                this.parameterCount = prepareResultPacket.readInt();
                this.parameterBindings = new BindValue[this.parameterCount];

                for (int i = 0; i < this.parameterCount; i++) {
                    this.parameterBindings[i] = new BindValue();
                }

                this.connection.incrementNumberOfPrepares();

                if (this.profileSQL) {
                    this.eventSink.consumeEvent(new ProfilerEvent(ProfilerEvent.TYPE_PREPARE, "", this.currentCatalog, this.connectionId, this.statementId, -1,
                            System.currentTimeMillis(), mysql.getCurrentTimeNanosOrMillis() - begin, mysql.getQueryTimingUnits(), null,
                            LogUtils.findCallingClassAndMethod(new Throwable()), truncateQueryToLog(sql)));
                }

                boolean checkEOF = !mysql.isEOFDeprecated();

                if (this.parameterCount > 0 && this.connection.versionMeetsMinimum(4, 1, 2) && !mysql.isVersion(5, 0, 0)) {
                    this.parameterFields = new Field[this.parameterCount];

                    Buffer metaDataPacket;
                    for (int i = 0; i < this.parameterCount; i++) {
                        metaDataPacket = mysql.readPacket();
                        this.parameterFields[i] = mysql.unpackField(metaDataPacket, false);
                    }
                    if (checkEOF) { // Skip the following EOF packet.
                        mysql.readPacket();
                    }
                }

                // Read in the result set column information
                if (this.fieldCount > 0) {
                    this.resultFields = new Field[this.fieldCount];

                    Buffer fieldPacket;
                    for (int i = 0; i < this.fieldCount; i++) {
                        fieldPacket = mysql.readPacket();
                        this.resultFields[i] = mysql.unpackField(fieldPacket, false);
                    }
                    if (checkEOF) { // Skip the following EOF packet.
                        mysql.readPacket();
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

            packet.clear();
            packet.writeByte((byte) MysqlDefs.COM_RESET_STMT);
            packet.writeLong(this.serverStatementId);

            try {
                mysql.sendCommand(MysqlDefs.COM_RESET_STMT, null, packet, !this.connection.versionMeetsMinimum(4, 1, 2), null, 0);
            } catch (SQLException sqlEx) {
                throw sqlEx;
            } catch (Exception ex) {
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
        throw SQLError.createSQLFeatureNotSupportedException();
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
                resetToType(binding, MysqlDefs.FIELD_TYPE_BLOB);

                binding.value = x;
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

                if (this.connection.versionMeetsMinimum(5, 0, 3)) {
                    resetToType(binding, MysqlDefs.FIELD_TYPE_NEW_DECIMAL);
                } else {
                    resetToType(binding, this.stringTypeCode);
                }

                binding.value = StringUtils.fixDecimalExponent(StringUtils.consistentToString(x));
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
                resetToType(binding, MysqlDefs.FIELD_TYPE_BLOB);

                binding.value = x;
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
                resetToType(binding, MysqlDefs.FIELD_TYPE_BLOB);

                binding.value = x;
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
        resetToType(binding, MysqlDefs.FIELD_TYPE_TINY);

        binding.longBinding = x;
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
            resetToType(binding, MysqlDefs.FIELD_TYPE_VAR_STRING);

            binding.value = x;
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
                resetToType(binding, MysqlDefs.FIELD_TYPE_BLOB);

                binding.value = reader;
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
                resetToType(binding, MysqlDefs.FIELD_TYPE_BLOB);

                binding.value = x.getCharacterStream();
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
        setDate(parameterIndex, x, null);
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
        if (x == null) {
            setNull(parameterIndex, java.sql.Types.DATE);
        } else {
            BindValue binding = getBinding(parameterIndex, false);
            resetToType(binding, MysqlDefs.FIELD_TYPE_DATE);

            binding.value = x;
        }
    }

    /**
     * @see java.sql.PreparedStatement#setDouble(int, double)
     */
    @Override
    public void setDouble(int parameterIndex, double x) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {

            if (!this.connection.getAllowNanAndInf() && (x == Double.POSITIVE_INFINITY || x == Double.NEGATIVE_INFINITY || Double.isNaN(x))) {
                throw SQLError.createSQLException("'" + x + "' is not a valid numeric or approximate numeric value", SQLError.SQL_STATE_ILLEGAL_ARGUMENT,
                        getExceptionInterceptor());

            }

            BindValue binding = getBinding(parameterIndex, false);
            resetToType(binding, MysqlDefs.FIELD_TYPE_DOUBLE);

            binding.doubleBinding = x;
        }
    }

    /**
     * @see java.sql.PreparedStatement#setFloat(int, float)
     */
    @Override
    public void setFloat(int parameterIndex, float x) throws SQLException {
        checkClosed();

        BindValue binding = getBinding(parameterIndex, false);
        resetToType(binding, MysqlDefs.FIELD_TYPE_FLOAT);

        binding.floatBinding = x;
    }

    /**
     * @see java.sql.PreparedStatement#setInt(int, int)
     */
    @Override
    public void setInt(int parameterIndex, int x) throws SQLException {
        checkClosed();

        BindValue binding = getBinding(parameterIndex, false);
        resetToType(binding, MysqlDefs.FIELD_TYPE_LONG);

        binding.longBinding = x;
    }

    /**
     * @see java.sql.PreparedStatement#setLong(int, long)
     */
    @Override
    public void setLong(int parameterIndex, long x) throws SQLException {
        checkClosed();

        BindValue binding = getBinding(parameterIndex, false);
        resetToType(binding, MysqlDefs.FIELD_TYPE_LONGLONG);

        binding.longBinding = x;
    }

    /**
     * @see java.sql.PreparedStatement#setNull(int, int)
     */
    @Override
    public void setNull(int parameterIndex, int sqlType) throws SQLException {
        checkClosed();

        BindValue binding = getBinding(parameterIndex, false);
        resetToType(binding, MysqlDefs.FIELD_TYPE_NULL);

        binding.isNull = true;
    }

    /**
     * @see java.sql.PreparedStatement#setNull(int, int, java.lang.String)
     */
    @Override
    public void setNull(int parameterIndex, int sqlType, String typeName) throws SQLException {
        checkClosed();

        BindValue binding = getBinding(parameterIndex, false);
        resetToType(binding, MysqlDefs.FIELD_TYPE_NULL);

        binding.isNull = true;
    }

    /**
     * @see java.sql.PreparedStatement#setRef(int, java.sql.Ref)
     */
    @Override
    public void setRef(int i, Ref x) throws SQLException {
        throw SQLError.createSQLFeatureNotSupportedException();
    }

    /**
     * @see java.sql.PreparedStatement#setShort(int, short)
     */
    @Override
    public void setShort(int parameterIndex, short x) throws SQLException {
        checkClosed();

        BindValue binding = getBinding(parameterIndex, false);
        resetToType(binding, MysqlDefs.FIELD_TYPE_SHORT);

        binding.longBinding = x;
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
            resetToType(binding, this.stringTypeCode);

            binding.value = x;
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
    public void setTime(int parameterIndex, java.sql.Time x) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            setTimeInternal(parameterIndex, x, null, this.connection.getDefaultTimeZone(), false);
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
    public void setTime(int parameterIndex, java.sql.Time x, Calendar cal) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            setTimeInternal(parameterIndex, x, cal, cal.getTimeZone(), true);
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
    private void setTimeInternal(int parameterIndex, java.sql.Time x, Calendar targetCalendar, TimeZone tz, boolean rollForward) throws SQLException {
        if (x == null) {
            setNull(parameterIndex, java.sql.Types.TIME);
        } else {
            BindValue binding = getBinding(parameterIndex, false);
            resetToType(binding, MysqlDefs.FIELD_TYPE_TIME);

            if (!this.useLegacyDatetimeCode) {
                binding.value = x;
            } else {
                Calendar sessionCalendar = getCalendarInstanceForSessionOrNew();

                binding.value = TimeUtil.changeTimezone(this.connection, sessionCalendar, targetCalendar, x, tz, this.connection.getServerTimezoneTZ(),
                        rollForward);
            }
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
            setTimestampInternal(parameterIndex, x, null, this.connection.getDefaultTimeZone(), false);
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
            setTimestampInternal(parameterIndex, x, cal, cal.getTimeZone(), true);
        }
    }

    private void setTimestampInternal(int parameterIndex, java.sql.Timestamp x, Calendar targetCalendar, TimeZone tz, boolean rollForward) throws SQLException {
        if (x == null) {
            setNull(parameterIndex, java.sql.Types.TIMESTAMP);
        } else {
            BindValue binding = getBinding(parameterIndex, false);
            resetToType(binding, MysqlDefs.FIELD_TYPE_DATETIME);

            if (!this.sendFractionalSeconds) {
                x = TimeUtil.truncateFractionalSeconds(x);
            }

            if (!this.useLegacyDatetimeCode) {
                binding.value = x;
            } else {
                Calendar sessionCalendar = this.connection.getUseJDBCCompliantTimezoneShift() ? this.connection.getUtcCalendar()
                        : getCalendarInstanceForSessionOrNew();

                binding.value = TimeUtil.changeTimezone(this.connection, sessionCalendar, targetCalendar, x, tz, this.connection.getServerTimezoneTZ(),
                        rollForward);
            }
        }
    }

    /**
     * Reset a bind value to be used for a new value of the given type.
     */
    protected void resetToType(BindValue oldValue, int bufferType) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            // clear any possible old value
            oldValue.reset();

            if (bufferType == MysqlDefs.FIELD_TYPE_NULL && oldValue.bufferType != 0) {
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

    /**
     * @param parameterIndex
     * @param x
     * @param length
     * 
     * @throws SQLException
     * @throws NotImplemented
     * 
     * @see java.sql.PreparedStatement#setUnicodeStream(int, java.io.InputStream, int)
     * @deprecated
     */
    @Deprecated
    @Override
    public void setUnicodeStream(int parameterIndex, InputStream x, int length) throws SQLException {
        checkClosed();

        throw SQLError.createSQLFeatureNotSupportedException();
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
                        storeTime(packet, (Time) value);
                        return;
                    case MysqlDefs.FIELD_TYPE_DATE:
                    case MysqlDefs.FIELD_TYPE_DATETIME:
                    case MysqlDefs.FIELD_TYPE_TIMESTAMP:
                        storeDateTime(packet, (java.util.Date) value, mysql, bindValue.bufferType);
                        return;
                    case MysqlDefs.FIELD_TYPE_VAR_STRING:
                    case MysqlDefs.FIELD_TYPE_STRING:
                    case MysqlDefs.FIELD_TYPE_VARCHAR:
                    case MysqlDefs.FIELD_TYPE_DECIMAL:
                    case MysqlDefs.FIELD_TYPE_NEW_DECIMAL:
                        if (value instanceof byte[]) {
                            packet.writeLenBytes((byte[]) value);
                        } else if (!this.isLoadDataQuery) {
                            packet.writeLenString((String) value, this.charEncoding, this.connection.getServerCharset(), this.charConverter,
                                    this.connection.parserKnowsUnicode(), this.connection);
                        } else {
                            packet.writeLenBytes(StringUtils.getBytes((String) value));
                        }

                        return;
                }

            } catch (UnsupportedEncodingException uEE) {
                throw SQLError.createSQLException(Messages.getString("ServerPreparedStatement.22") + this.connection.getEncoding() + "'",
                        SQLError.SQL_STATE_GENERAL_ERROR, getExceptionInterceptor());
            }
        }
    }

    private void storeDateTime412AndOlder(Buffer intoBuf, java.util.Date dt, int bufferType) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            Calendar sessionCalendar = null;

            if (!this.useLegacyDatetimeCode) {
                if (bufferType == MysqlDefs.FIELD_TYPE_DATE) {
                    sessionCalendar = getDefaultTzCalendar();
                } else {
                    sessionCalendar = getServerTzCalendar();
                }
            } else {
                sessionCalendar = (dt instanceof Timestamp && this.connection.getUseJDBCCompliantTimezoneShift()) ? this.connection.getUtcCalendar()
                        : getCalendarInstanceForSessionOrNew();
            }

            java.util.Date oldTime = sessionCalendar.getTime();

            try {
                intoBuf.ensureCapacity(8);
                intoBuf.writeByte((byte) 7); // length

                sessionCalendar.setTime(dt);

                int year = sessionCalendar.get(Calendar.YEAR);
                int month = sessionCalendar.get(Calendar.MONTH) + 1;
                int date = sessionCalendar.get(Calendar.DATE);

                intoBuf.writeInt(year);
                intoBuf.writeByte((byte) month);
                intoBuf.writeByte((byte) date);

                if (dt instanceof java.sql.Date) {
                    intoBuf.writeByte((byte) 0);
                    intoBuf.writeByte((byte) 0);
                    intoBuf.writeByte((byte) 0);
                } else {
                    intoBuf.writeByte((byte) sessionCalendar.get(Calendar.HOUR_OF_DAY));
                    intoBuf.writeByte((byte) sessionCalendar.get(Calendar.MINUTE));
                    intoBuf.writeByte((byte) sessionCalendar.get(Calendar.SECOND));
                }
            } finally {
                sessionCalendar.setTime(oldTime);
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
    private void storeDateTime(Buffer intoBuf, java.util.Date dt, MysqlIO mysql, int bufferType) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            if (this.connection.versionMeetsMinimum(4, 1, 3)) {
                storeDateTime413AndNewer(intoBuf, dt, bufferType);
            } else {
                storeDateTime412AndOlder(intoBuf, dt, bufferType);
            }
        }
    }

    private void storeDateTime413AndNewer(Buffer intoBuf, java.util.Date dt, int bufferType) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            Calendar sessionCalendar = null;

            if (!this.useLegacyDatetimeCode) {
                if (bufferType == MysqlDefs.FIELD_TYPE_DATE) {
                    sessionCalendar = getDefaultTzCalendar();
                } else {
                    sessionCalendar = getServerTzCalendar();
                }
            } else {
                sessionCalendar = (dt instanceof Timestamp && this.connection.getUseJDBCCompliantTimezoneShift()) ? this.connection.getUtcCalendar()
                        : getCalendarInstanceForSessionOrNew();
            }

            java.util.Date oldTime = sessionCalendar.getTime();

            try {
                sessionCalendar.setTime(dt);

                if (dt instanceof java.sql.Date) {
                    sessionCalendar.set(Calendar.HOUR_OF_DAY, 0);
                    sessionCalendar.set(Calendar.MINUTE, 0);
                    sessionCalendar.set(Calendar.SECOND, 0);
                }

                byte length = (byte) 7;

                if (dt instanceof java.sql.Timestamp) {
                    length = (byte) 11;
                }

                intoBuf.ensureCapacity(length);

                intoBuf.writeByte(length); // length

                int year = sessionCalendar.get(Calendar.YEAR);
                int month = sessionCalendar.get(Calendar.MONTH) + 1;
                int date = sessionCalendar.get(Calendar.DAY_OF_MONTH);

                intoBuf.writeInt(year);
                intoBuf.writeByte((byte) month);
                intoBuf.writeByte((byte) date);

                if (dt instanceof java.sql.Date) {
                    intoBuf.writeByte((byte) 0);
                    intoBuf.writeByte((byte) 0);
                    intoBuf.writeByte((byte) 0);
                } else {
                    intoBuf.writeByte((byte) sessionCalendar.get(Calendar.HOUR_OF_DAY));
                    intoBuf.writeByte((byte) sessionCalendar.get(Calendar.MINUTE));
                    intoBuf.writeByte((byte) sessionCalendar.get(Calendar.SECOND));
                }

                if (length == 11) {
                    //	MySQL expects microseconds, not nanos
                    intoBuf.writeLong(((java.sql.Timestamp) dt).getNanos() / 1000);
                }

            } finally {
                sessionCalendar.setTime(oldTime);
            }
        }
    }

    private Calendar getServerTzCalendar() throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            if (this.serverTzCalendar == null) {
                this.serverTzCalendar = new GregorianCalendar(this.connection.getServerTimezoneTZ());
            }

            return this.serverTzCalendar;
        }
    }

    private Calendar getDefaultTzCalendar() throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            if (this.defaultTzCalendar == null) {
                this.defaultTzCalendar = new GregorianCalendar(TimeZone.getDefault());
            }

            return this.defaultTzCalendar;
        }
    }

    //
    // TO DO: Investigate using NIO to do this faster
    //
    private void storeReader(MysqlIO mysql, int parameterIndex, Buffer packet, Reader inStream) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            String forcedEncoding = this.connection.getClobCharacterEncoding();

            String clobEncoding = (forcedEncoding == null ? this.connection.getEncoding() : forcedEncoding);

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
                packet.writeByte((byte) MysqlDefs.COM_LONG_DATA);
                packet.writeLong(this.serverStatementId);
                packet.writeInt((parameterIndex));

                boolean readAny = false;

                while ((numRead = inStream.read(buf)) != -1) {
                    readAny = true;

                    byte[] valueAsBytes = StringUtils.getBytes(buf, null, clobEncoding, this.connection.getServerCharset(), 0, numRead,
                            this.connection.parserKnowsUnicode(), getExceptionInterceptor());

                    packet.writeBytesNoNull(valueAsBytes, 0, valueAsBytes.length);

                    bytesInPacket += valueAsBytes.length;
                    totalBytesRead += valueAsBytes.length;

                    if (bytesInPacket >= packetIsFullAt) {
                        bytesReadAtLastSend = totalBytesRead;

                        mysql.sendCommand(MysqlDefs.COM_LONG_DATA, null, packet, true, null, 0);

                        bytesInPacket = 0;
                        packet.clear();
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
                this.parseInfo = new ParseInfo(this.originalSql, this.connection, this.connection.getMetaData(), this.charEncoding, this.charConverter);
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
                            throw new IllegalArgumentException(
                                    "Unknown type when re-binding parameter into batched statement for parameter index " + batchedParamIndex);
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
    protected PreparedStatement prepareBatchedInsertSQL(MySQLConnection localConn, int numBatches) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            try {
                PreparedStatement pstmt = ((Wrapper) localConn.prepareStatement(this.parseInfo.getSqlForBatch(numBatches), this.resultSetConcurrency,
                        this.resultSetType)).unwrap(PreparedStatement.class);
                pstmt.setRetrieveGeneratedKeys(this.retrieveGeneratedKeys);

                return pstmt;
            } catch (UnsupportedEncodingException e) {
                SQLException sqlEx = SQLError.createSQLException("Unable to prepare batch statement", SQLError.SQL_STATE_GENERAL_ERROR,
                        getExceptionInterceptor());
                sqlEx.initCause(e);

                throw sqlEx;
            }
        }
    }

    @Override
    public void setPoolable(boolean poolable) throws SQLException {
        if (!poolable) {
            this.connection.decachePreparedStatement(this);
        }
        super.setPoolable(poolable);
    }
}
