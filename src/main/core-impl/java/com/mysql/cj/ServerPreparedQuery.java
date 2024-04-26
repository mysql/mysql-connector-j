/*
 * Copyright (c) 2017, 2024, Oracle and/or its affiliates.
 *
 * This program is free software; you can redistribute it and/or modify it under the terms of the GNU General Public License, version 2.0, as published by
 * the Free Software Foundation.
 *
 * This program is designed to work with certain software that is licensed under separate terms, as designated in a particular file or component or in
 * included license documentation. The authors of MySQL hereby grant you an additional permission to link the program and your derivative works with the
 * separately licensed software that they have either included with the program or referenced in the documentation.
 *
 * Without limiting anything contained in the foregoing, this file, which is part of MySQL Connector/J, is also subject to the Universal FOSS Exception,
 * version 1.0, a copy of which can be found at http://oss.oracle.com/licenses/universal-foss-exception.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License, version 2.0, for more details.
 *
 * You should have received a copy of the GNU General Public License along with this program; if not, write to the Free Software Foundation, Inc.,
 * 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA
 */

package com.mysql.cj;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.sql.Clob;

import com.mysql.cj.conf.PropertyKey;
import com.mysql.cj.conf.RuntimeProperty;
import com.mysql.cj.exceptions.CJException;
import com.mysql.cj.exceptions.ExceptionFactory;
import com.mysql.cj.exceptions.MysqlErrorNumbers;
import com.mysql.cj.exceptions.WrongArgumentException;
import com.mysql.cj.log.ProfilerEvent;
import com.mysql.cj.protocol.ColumnDefinition;
import com.mysql.cj.protocol.Message;
import com.mysql.cj.protocol.ProtocolEntityFactory;
import com.mysql.cj.protocol.Resultset;
import com.mysql.cj.protocol.Resultset.Type;
import com.mysql.cj.protocol.a.ColumnDefinitionFactory;
import com.mysql.cj.protocol.a.NativeConstants.IntegerDataType;
import com.mysql.cj.protocol.a.NativeConstants.StringLengthDataType;
import com.mysql.cj.protocol.a.NativeConstants.StringSelfDataType;
import com.mysql.cj.protocol.a.NativeMessageBuilder;
import com.mysql.cj.protocol.a.NativePacketPayload;
import com.mysql.cj.result.Field;
import com.mysql.cj.telemetry.TelemetryAttribute;
import com.mysql.cj.telemetry.TelemetryScope;
import com.mysql.cj.telemetry.TelemetrySpan;
import com.mysql.cj.telemetry.TelemetrySpanName;
import com.mysql.cj.util.StringUtils;

// TODO should not be protocol-specific

public class ServerPreparedQuery extends ClientPreparedQuery {

    public static final int BLOB_STREAM_READ_BUF_SIZE = 8192;
    public static final byte OPEN_CURSOR_FLAG = 0x01;
    public static final byte PARAMETER_COUNT_AVAILABLE = 0x08;

    /** The ID that the server uses to identify this PreparedStatement */
    private long serverStatementId;

    /** Field-level metadata for parameters */
    private Field[] parameterFields;

    /** Field-level metadata for result sets. From statement prepare. */
    private ColumnDefinition resultFields;

    /** The "profileSQL" connection property value */
    protected boolean profileSQL = false;

    /** The "gatherPerfMetrics" connection property value */
    protected boolean gatherPerfMetrics;

    /** The "logSlowQueries" connection property value */
    protected boolean logSlowQueries = false;

    private boolean useAutoSlowLog;

    protected RuntimeProperty<Integer> slowQueryThresholdMillis;

    protected RuntimeProperty<Boolean> explainSlowQueries;
    protected boolean useCursorFetch = false;

    protected boolean queryWasSlow = false;

    protected NativeMessageBuilder commandBuilder = null;

    public static ServerPreparedQuery getInstance(NativeSession sess) {
        if (sess.getPropertySet().getBooleanProperty(PropertyKey.autoGenerateTestcaseScript).getValue()) {
            return new ServerPreparedQueryTestcaseGenerator(sess);
        }
        return new ServerPreparedQuery(sess);
    }

    protected ServerPreparedQuery(NativeSession sess) {
        super(sess);
        this.profileSQL = sess.getPropertySet().getBooleanProperty(PropertyKey.profileSQL).getValue();
        this.gatherPerfMetrics = sess.getPropertySet().getBooleanProperty(PropertyKey.gatherPerfMetrics).getValue();
        this.logSlowQueries = sess.getPropertySet().getBooleanProperty(PropertyKey.logSlowQueries).getValue();
        this.useAutoSlowLog = sess.getPropertySet().getBooleanProperty(PropertyKey.autoSlowLog).getValue();
        this.slowQueryThresholdMillis = sess.getPropertySet().getIntegerProperty(PropertyKey.slowQueryThresholdMillis);
        this.explainSlowQueries = sess.getPropertySet().getBooleanProperty(PropertyKey.explainSlowQueries);
        this.useCursorFetch = sess.getPropertySet().getBooleanProperty(PropertyKey.useCursorFetch).getValue();
        this.commandBuilder = (NativeMessageBuilder) sess.getProtocol().getMessageBuilder();
    }

    /**
     * @param sql
     *            query string
     * @throws IOException
     *             if an i/o error occurs
     */
    public void serverPrepare(String sql) throws IOException {
        this.session.checkClosed();

        this.session.getSessionLock().lock();
        try {
            long begin = this.profileSQL ? System.currentTimeMillis() : 0;

            NativePacketPayload prepareResultPacket = this.session.getProtocol()
                    .sendCommand(this.commandBuilder.buildComStmtPrepare(this.session.getSharedSendPacket(), sql,
                            this.session.getPropertySet().getStringProperty(PropertyKey.characterEncoding).getValue()), false, 0);

            // 4.1.1 and newer use the first byte as an 'ok' or 'error' flag, so move the buffer pointer past it to start reading the statement id.
            prepareResultPacket.setPosition(1);

            this.serverStatementId = prepareResultPacket.readInteger(IntegerDataType.INT4);
            int fieldCount = (int) prepareResultPacket.readInteger(IntegerDataType.INT2);
            setParameterCount((int) prepareResultPacket.readInteger(IntegerDataType.INT2));

            this.queryBindings = new NativeQueryBindings(this.parameterCount, this.session, NativeQueryBindValue::new);

            if (this.gatherPerfMetrics) {
                this.session.getProtocol().getMetricsHolder().incrementNumberOfPrepares();
            }

            if (this.profileSQL) {
                this.session.getProfilerEventHandler().processEvent(ProfilerEvent.TYPE_PREPARE, this.session, this, null,
                        this.session.getCurrentTimeNanosOrMillis() - begin, new Throwable(), truncateQueryToLog(sql));
            }

            boolean checkEOF = !this.session.getServerSession().isEOFDeprecated();

            if (this.parameterCount > 0) {
                this.parameterFields = this.session.getProtocol().read(ColumnDefinition.class, new ColumnDefinitionFactory(this.parameterCount, null))
                        .getFields();
                if (checkEOF && this.session.getProtocol().probeMessage(null).isEOFPacket()) { // Skip the following EOF packet.
                    this.session.getProtocol().skipPacket();
                }
            }

            // Read in the result set column information
            if (fieldCount > 0) {
                this.resultFields = this.session.getProtocol().read(ColumnDefinition.class, new ColumnDefinitionFactory(fieldCount, null));
                if (checkEOF && this.session.getProtocol().probeMessage(null).isEOFPacket()) { // Skip the following EOF packet.
                    this.session.getProtocol().skipPacket();
                }
            }
        } finally {
            this.session.getSessionLock().unlock();
        }
    }

    @Override
    public void statementBegins() {
        super.statementBegins();
        this.queryWasSlow = false;
    }

    /**
     * @param <T>
     *            extends {@link Resultset}
     * @param maxRowsToRetrieve
     *            rows limit
     * @param createStreamingResultSet
     *            should c/J create a streaming result?
     * @param metadata
     *            use this metadata instead of the one provided on wire
     * @param resultSetFactory
     *            {@link ProtocolEntityFactory}
     * @return T instance
     */
    public <T extends Resultset> T serverExecute(int maxRowsToRetrieve, boolean createStreamingResultSet, ColumnDefinition metadata,
            ProtocolEntityFactory<T, NativePacketPayload> resultSetFactory) {
        if (this.session.shouldIntercept()) {
            T interceptedResults = this.session.invokeQueryInterceptorsPre(this::getOriginalSql, this, true);

            if (interceptedResults != null) {
                return interceptedResults;
            }
        }
        String queryAsString = this.profileSQL || this.logSlowQueries || this.gatherPerfMetrics ? asSql() : "";
        return readExecuteResult(sendExecutePacket(prepareExecutePacket(), queryAsString), maxRowsToRetrieve, createStreamingResultSet, metadata,
                resultSetFactory, queryAsString);
    }

    public NativePacketPayload prepareExecutePacket() {
        BindValue[] bindValues = this.queryBindings.getBindValues();

        if (this.queryBindings.isLongParameterSwitchDetected()) {
            // Check when values were bound
            boolean firstFound = false;
            long boundTimeToCheck = 0;

            for (int i = 0; i < this.parameterCount - 1; i++) {
                if (bindValues[i].isStream()) {
                    if (firstFound && boundTimeToCheck != bindValues[i].getBoundBeforeExecutionNum()) {
                        throw ExceptionFactory.createException(
                                Messages.getString("ServerPreparedStatement.11") + Messages.getString("ServerPreparedStatement.12"),
                                MysqlErrorNumbers.SQLSTATE_CONNJ_DRIVER_NOT_CAPABLE, 0, true, null, this.session.getExceptionInterceptor());
                    }
                    firstFound = true;
                    boundTimeToCheck = bindValues[i].getBoundBeforeExecutionNum();
                }
            }

            // Okay, we've got all "newly"-bound streams, so reset server-side state to clear out previous bindings
            serverResetStatement();
        }

        this.queryBindings.checkAllParametersSet();

        //
        // Send all long data
        //
        for (int i = 0; i < this.parameterCount; i++) {
            if (bindValues[i].isStream()) {
                serverLongData(i, bindValues[i]);
            }
        }

        //
        // store the parameter values
        //
        boolean sendQueryAttributes = false;
        if (this.session.getServerSession().supportsQueryAttributes()) {
            // Servers between 8.0.23 8.0.25 are affected by Bug#103102, Bug#103268 and Bug#103377. Query attributes cannot be sent to these servers.
            sendQueryAttributes = this.session.getServerSession().getServerVersion().meetsMinimum(new ServerVersion(8, 0, 26));
        } else if (this.queryAttributesBindings.getCount() > 0) {
            this.session.getLog().logWarn(Messages.getString("QueryAttributes.SetButNotSupported"));
        }

        byte flags = 0;
        if (this.resultFields != null && this.resultFields.getFields() != null && this.useCursorFetch && this.resultSetType == Type.FORWARD_ONLY
                && this.fetchSize > 0) {
            // we only create cursor-backed result sets if
            // a) The query is a SELECT
            // b) The server supports it
            // c) We know it is forward-only (note this doesn't preclude updatable result sets)
            // d) The user has set a fetch size
            flags |= OPEN_CURSOR_FLAG;
        }
        if (sendQueryAttributes) {
            flags |= PARAMETER_COUNT_AVAILABLE;
        }

        return this.commandBuilder.buildComStmtExecute(this.session.getSharedSendPacket(), this.serverStatementId, flags, sendQueryAttributes, this);
    }

    public NativePacketPayload sendExecutePacket(NativePacketPayload packet, String queryAsString) { // TODO queryAsString should be shared instead of passed
        final long begin = this.session.getCurrentTimeNanosOrMillis();
        resetCancelledState();
        CancelQueryTask timeoutTask = null;
        try {
            // Get this before executing to avoid a shared packet pollution in the case some other query is issued internally, such as when using I_S.
            timeoutTask = startQueryTimer(this, this.timeoutInMillis);

            statementBegins();

            NativePacketPayload resultPacket = this.session.getProtocol().sendCommand(packet, false, 0);

            final long queryEndTime = this.session.getCurrentTimeNanosOrMillis();

            if (timeoutTask != null) {
                stopQueryTimer(timeoutTask, true, true);
                timeoutTask = null;
            }

            final long executeTime = queryEndTime - begin;
            setExecuteTime(executeTime);

            if (this.logSlowQueries) {
                this.queryWasSlow = this.useAutoSlowLog ? //
                        this.session.getProtocol().getMetricsHolder().checkAbonormallyLongQuery(executeTime)
                        : executeTime > this.slowQueryThresholdMillis.getValue();

                if (this.queryWasSlow) {
                    this.session.getProfilerEventHandler().processEvent(ProfilerEvent.TYPE_SLOW_QUERY, this.session, this, null, executeTime, new Throwable(),
                            Messages.getString("ServerPreparedStatement.15", new String[] { String.valueOf(this.session.getSlowQueryThreshold()),
                                    String.valueOf(executeTime), this.originalSql, queryAsString }));
                }
            }

            if (this.gatherPerfMetrics) {
                this.session.getProtocol().getMetricsHolder().registerQueryExecutionTime(executeTime);
                this.session.getProtocol().getMetricsHolder().incrementNumberOfPreparedExecutes();
            }

            if (this.profileSQL) {
                this.session.getProfilerEventHandler().processEvent(ProfilerEvent.TYPE_EXECUTE, this.session, this, null, executeTime, new Throwable(),
                        truncateQueryToLog(queryAsString));
            }

            return resultPacket;

        } catch (CJException sqlEx) {
            if (this.session.shouldIntercept()) {
                this.session.invokeQueryInterceptorsPost(this::getOriginalSql, this, null, true);
            }

            throw sqlEx;
        } finally {
            this.statementExecuting.set(false);
            stopQueryTimer(timeoutTask, false, false);
        }
    }

    public <T extends Resultset> T readExecuteResult(NativePacketPayload resultPacket, int maxRowsToRetrieve, boolean createStreamingResultSet,
            ColumnDefinition metadata, ProtocolEntityFactory<T, NativePacketPayload> resultSetFactory, String queryAsString) {
        // TODO queryAsString should be shared instead of passed
        try {
            long fetchStartTime = this.profileSQL ? this.session.getCurrentTimeNanosOrMillis() : 0;

            T rs = this.session.getProtocol().readAllResults(maxRowsToRetrieve, createStreamingResultSet, resultPacket, true,
                    metadata != null ? metadata : this.resultFields, resultSetFactory);

            if (this.session.shouldIntercept()) {
                T interceptedResults = this.session.invokeQueryInterceptorsPost(this::getOriginalSql, this, rs, true);

                if (interceptedResults != null) {
                    rs = interceptedResults;
                }
            }

            if (this.profileSQL) {
                this.session.getProfilerEventHandler().processEvent(ProfilerEvent.TYPE_FETCH, this.session, this, rs,
                        this.session.getCurrentTimeNanosOrMillis() - fetchStartTime, new Throwable(), null);
            }

            if (this.queryWasSlow && this.explainSlowQueries.getValue()) {
                this.session.getProtocol().explainSlowQuery(queryAsString, queryAsString);
            }

            this.queryBindings.getSendTypesToServer().set(false);

            if (this.session.hadWarnings()) {
                this.session.getProtocol().scanForAndThrowDataTruncation();
            }

            return rs;
        } catch (IOException ioEx) {
            throw ExceptionFactory.createCommunicationsException(this.session.getPropertySet(), this.session.getServerSession(),
                    this.session.getProtocol().getPacketSentTimeHolder(), this.session.getProtocol().getPacketReceivedTimeHolder(), ioEx,
                    this.session.getExceptionInterceptor());
        } catch (CJException sqlEx) {
            if (this.session.shouldIntercept()) {
                this.session.invokeQueryInterceptorsPost(this::getOriginalSql, this, null, true);
            }

            throw sqlEx;
        }
    }

    /**
     * Sends stream-type data parameters to the server.
     *
     * <pre>
     *  Long data handling:
     *
     *  - Server gets the long data in pieces with command type 'COM_LONG_DATA'.
     *  - The packet received will have the format:
     *    [COM_LONG_DATA:     1][STMT_ID:4][parameter_number:2][type:2][data]
     *  - Checks if the type is specified by client, and if yes reads the type,
     *    and  stores the data in that format.
     *  - It is up to the client to check for read data ended. The server does not
     *    care; and also server does not notify to the client that it got the
     *    data  or not; if there is any error; then during execute; the error
     *    will  be returned
     * </pre>
     *
     * @param parameterIndex
     *            parameter index
     * @param binding
     *            {@link BindValue containing long data}
     */
    private void serverLongData(int parameterIndex, BindValue binding) {
        this.lock.lock();
        try {
            TelemetrySpan span = this.session.getTelemetryHandler().startSpan(TelemetrySpanName.STMT_SEND_LONG_DATA);
            try (TelemetryScope scope = span.makeCurrent()) {
                String dbOperation = getQueryInfo().getStatementKeyword();
                span.setAttribute(TelemetryAttribute.DB_NAME, getCurrentDatabase());
                span.setAttribute(TelemetryAttribute.DB_OPERATION, dbOperation);
                span.setAttribute(TelemetryAttribute.DB_STATEMENT, dbOperation + TelemetryAttribute.STATEMENT_SUFFIX);
                span.setAttribute(TelemetryAttribute.DB_SYSTEM, TelemetryAttribute.DB_SYSTEM_DEFAULT);
                span.setAttribute(TelemetryAttribute.DB_USER, this.session.getHostInfo().getUser());
                span.setAttribute(TelemetryAttribute.THREAD_ID, Thread.currentThread().getId());
                span.setAttribute(TelemetryAttribute.THREAD_NAME, Thread.currentThread().getName());

                NativePacketPayload packet = this.session.getSharedSendPacket();
                Object value = binding.getValue();
                if (value instanceof byte[]) {
                    this.session.getProtocol()
                            .sendCommand(this.commandBuilder.buildComStmtSendLongData(packet, this.serverStatementId, parameterIndex, (byte[]) value), true, 0);
                } else if (value instanceof InputStream) {
                    storeStreamOrReader(parameterIndex, packet, (InputStream) value);
                } else if (value instanceof java.sql.Blob) {
                    try {
                        storeStreamOrReader(parameterIndex, packet, ((java.sql.Blob) value).getBinaryStream());
                    } catch (Throwable t) {
                        throw ExceptionFactory.createException(t.getMessage(), this.session.getExceptionInterceptor());
                    }
                } else if (value instanceof Reader) {
                    if (binding.isNational() && !this.charEncoding.equalsIgnoreCase("UTF-8") && !this.charEncoding.equalsIgnoreCase("utf8")) {
                        throw ExceptionFactory.createException(Messages.getString("ServerPreparedStatement.31"), this.session.getExceptionInterceptor());
                    }
                    storeStreamOrReader(parameterIndex, packet, (Reader) value);
                } else if (value instanceof Clob) {
                    if (binding.isNational() && !this.charEncoding.equalsIgnoreCase("UTF-8") && !this.charEncoding.equalsIgnoreCase("utf8")) {
                        throw ExceptionFactory.createException(Messages.getString("ServerPreparedStatement.31"), this.session.getExceptionInterceptor());
                    }
                    try {
                        storeStreamOrReader(parameterIndex, packet, ((Clob) value).getCharacterStream());
                    } catch (Throwable t) {
                        throw ExceptionFactory.createException(t.getMessage(), t);
                    }
                } else {
                    throw ExceptionFactory.createException(WrongArgumentException.class,
                            Messages.getString("ServerPreparedStatement.18") + value.getClass().getName() + "'", this.session.getExceptionInterceptor());
                }
            } catch (Throwable t) {
                span.setError(t);
                throw t;
            } finally {
                span.end();
            }
        } finally {
            this.lock.unlock();
        }
    }

    @Override
    public void closeQuery() {
        this.queryBindings = null;
        this.parameterFields = null;
        this.resultFields = null;
        super.closeQuery();
    }

    public long getServerStatementId() {
        return this.serverStatementId;
    }

    public void setServerStatementId(long serverStatementId) {
        this.serverStatementId = serverStatementId;
    }

    public Field[] getParameterFields() {
        return this.parameterFields;
    }

    public void setParameterFields(Field[] parameterFields) {
        this.parameterFields = parameterFields;
    }

    public ColumnDefinition getResultFields() {
        return this.resultFields;
    }

    public void setResultFields(ColumnDefinition resultFields) {
        this.resultFields = resultFields;
    }

    private void storeStreamOrReader(int parameterIndex, NativePacketPayload packet, Closeable streamOrReader) {
        // TODO consider to use more precise type than just Closable
        this.session.checkClosed();
        boolean isStream = InputStream.class.isAssignableFrom(streamOrReader.getClass());
        byte[] bBuf = null;
        char[] cBuf = null;
        String clobEncoding = null;

        this.session.getSessionLock().lock();
        try {
            if (isStream) {
                bBuf = new byte[BLOB_STREAM_READ_BUF_SIZE];
            } else {
                clobEncoding = this.session.getPropertySet().getStringProperty(PropertyKey.clobCharacterEncoding).getStringValue();
                if (clobEncoding == null) {
                    clobEncoding = this.session.getPropertySet().getStringProperty(PropertyKey.characterEncoding).getValue();
                }
                int maxBytesChar = 2;
                if (clobEncoding != null) {
                    maxBytesChar = this.session.getServerSession().getCharsetSettings().getMaxBytesPerChar(clobEncoding);
                    if (maxBytesChar == 1) {
                        maxBytesChar = 2; // for safety
                    }
                }
                cBuf = new char[ServerPreparedQuery.BLOB_STREAM_READ_BUF_SIZE / maxBytesChar];
            }

            boolean readAny = false;
            int bytesInPacket = 0;
            int totalBytesRead = 0;
            int bytesReadAtLastSend = 0;
            int packetIsFullAt = this.session.getPropertySet().getMemorySizeProperty(PropertyKey.blobSendChunkSize).getValue();
            int numRead = 0;

            try {
                packet.setPosition(0);
                this.commandBuilder.buildComStmtSendLongDataHeader(packet, this.serverStatementId, parameterIndex);

                while ((numRead = isStream ? ((InputStream) streamOrReader).read(bBuf) : ((Reader) streamOrReader).read(cBuf)) != -1) {
                    readAny = true;

                    if (isStream) {
                        packet.writeBytes(StringLengthDataType.STRING_FIXED, bBuf, 0, numRead);
                        bytesInPacket += numRead;
                        totalBytesRead += numRead;
                    } else {
                        byte[] valueAsBytes = StringUtils.getBytes(cBuf, 0, numRead, clobEncoding);
                        packet.writeBytes(StringSelfDataType.STRING_EOF, valueAsBytes);
                        bytesInPacket += valueAsBytes.length;
                        totalBytesRead += valueAsBytes.length;
                    }

                    if (bytesInPacket >= packetIsFullAt) {
                        bytesReadAtLastSend = totalBytesRead;

                        this.session.getProtocol().sendCommand(packet, true, 0);

                        bytesInPacket = 0;
                        packet.setPosition(0);
                        this.commandBuilder.buildComStmtSendLongDataHeader(packet, this.serverStatementId, parameterIndex);
                    }
                }

                if (!readAny || totalBytesRead != bytesReadAtLastSend) {
                    this.session.getProtocol().sendCommand(packet, true, 0);
                }
            } catch (IOException ioEx) {
                throw ExceptionFactory.createException(
                        (isStream ? Messages.getString("ServerPreparedStatement.24") : Messages.getString("ServerPreparedStatement.25")) + ioEx.toString(),
                        ioEx, this.session.getExceptionInterceptor());
            } finally {
                if (this.autoClosePStmtStreams.getValue()) {
                    if (streamOrReader != null) {
                        try {
                            streamOrReader.close();
                        } catch (IOException ioEx) {
                            // ignore
                        }
                    }
                }
            }
        } finally {
            this.session.getSessionLock().unlock();
        }
    }

    /**
     * @param clearServerParameters
     *            flag indicating whether we need an additional clean up
     */
    public void clearParameters(boolean clearServerParameters) {
        boolean hadLongData = false;

        if (this.queryBindings != null) {
            hadLongData = this.queryBindings.clearBindValues();
            this.queryBindings.setLongParameterSwitchDetected(clearServerParameters && hadLongData);
        }

        if (clearServerParameters && hadLongData) {
            serverResetStatement();
        }
    }

    public void serverResetStatement() {
        this.session.checkClosed();
        this.session.getSessionLock().lock();
        try {
            TelemetrySpan span = this.session.getTelemetryHandler().startSpan(TelemetrySpanName.STMT_RESET_PREPARED);
            try (TelemetryScope scope = span.makeCurrent()) {
                String dbOperation = getQueryInfo().getStatementKeyword();
                span.setAttribute(TelemetryAttribute.DB_NAME, getCurrentDatabase());
                span.setAttribute(TelemetryAttribute.DB_OPERATION, dbOperation);
                span.setAttribute(TelemetryAttribute.DB_STATEMENT, dbOperation + TelemetryAttribute.STATEMENT_SUFFIX);
                span.setAttribute(TelemetryAttribute.DB_SYSTEM, TelemetryAttribute.DB_SYSTEM_DEFAULT);
                span.setAttribute(TelemetryAttribute.DB_USER, this.session.getHostInfo().getUser());
                span.setAttribute(TelemetryAttribute.THREAD_ID, Thread.currentThread().getId());
                span.setAttribute(TelemetryAttribute.THREAD_NAME, Thread.currentThread().getName());

                try {
                    this.session.getProtocol().sendCommand(this.commandBuilder.buildComStmtReset(this.session.getSharedSendPacket(), this.serverStatementId),
                            false, 0);
                } finally {
                    // OK_PACKET returned in previous sendCommand() was not processed so keep original transaction state.
                    this.session.getProtocol().getServerSession().preserveOldTransactionState();
                    this.session.clearInputStream();
                }
                // Nothing to be detected after a reset...
                this.queryBindings.setLongParameterSwitchDetected(false);
            } catch (Throwable t) {
                span.setError(t);
                throw t;
            } finally {
                span.end();
            }
        } finally {
            this.session.getSessionLock().unlock();
        }
    }

    /**
     * Computes the maximum parameter set size and the size of the entire batch given
     * the number of arguments in the batch.
     */
    @Override
    protected long[] computeMaxParameterSetSizeAndBatchSize(int numBatchedArgs) {
        long maxSizeOfParameterSet = 0;
        long sizeOfEntireBatch = 1 /* com_stmt_execute */ + 4 /* statement_id */ + 1 /* flags */ + 4 /* iteration_count */ + 1 /* new_params_bind_flag */;

        boolean supportsQueryAttributes = this.session.getServerSession().supportsQueryAttributes();
        if (supportsQueryAttributes) {
            sizeOfEntireBatch += 9 /* parameter_count */;
            sizeOfEntireBatch += (this.queryAttributesBindings.getCount() + 7) / 8 /* null_bitmap */;
            for (int i = 0; i < this.queryAttributesBindings.getCount(); i++) {
                BindValue queryAttribute = this.queryAttributesBindings.getAttributeValue(i);
                sizeOfEntireBatch += 2 /* parameter_type */ + queryAttribute.getName().length() /* parameter_name */ + queryAttribute.getBinaryLength();
            }
        }

        for (int i = 0; i < numBatchedArgs; i++) {
            long sizeOfParameterSet = (this.parameterCount + 7) / 8 /* null_bitmap */
                    + this.parameterCount * 2 /* parameter_type */;
            if (supportsQueryAttributes) {
                sizeOfParameterSet += this.parameterCount /* parameter_name (all empty) */;
            }

            BindValue[] bindValues = ((QueryBindings) this.batchedArgs.get(i)).getBindValues();
            for (int j = 0; j < bindValues.length; j++) {
                sizeOfParameterSet += bindValues[j].getBinaryLength();
            }

            sizeOfEntireBatch += sizeOfParameterSet;

            if (sizeOfParameterSet > maxSizeOfParameterSet) {
                maxSizeOfParameterSet = sizeOfParameterSet;
            }
        }

        return new long[] { maxSizeOfParameterSet, sizeOfEntireBatch };
    }

    private String truncateQueryToLog(String sql) {
        String queryStr = null;

        int maxQuerySizeToLog = this.session.getPropertySet().getIntegerProperty(PropertyKey.maxQuerySizeToLog).getValue();
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

    @Override
    public <M extends Message> M fillSendPacket(QueryBindings bindings) {
        return null; // we don't use this type of packet
    }

}
