/*
 * Copyright (c) 2017, 2018, Oracle and/or its affiliates. All rights reserved.
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

package com.mysql.cj;

import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;

import com.mysql.cj.conf.PropertyDefinitions;
import com.mysql.cj.conf.ReadableProperty;
import com.mysql.cj.exceptions.CJException;
import com.mysql.cj.exceptions.ExceptionFactory;
import com.mysql.cj.exceptions.MysqlErrorNumbers;
import com.mysql.cj.exceptions.WrongArgumentException;
import com.mysql.cj.log.ProfilerEvent;
import com.mysql.cj.log.ProfilerEventHandlerFactory;
import com.mysql.cj.log.ProfilerEventImpl;
import com.mysql.cj.protocol.ColumnDefinition;
import com.mysql.cj.protocol.Message;
import com.mysql.cj.protocol.ProtocolEntityFactory;
import com.mysql.cj.protocol.Resultset;
import com.mysql.cj.protocol.Resultset.Type;
import com.mysql.cj.protocol.a.ColumnDefinitionFactory;
import com.mysql.cj.protocol.a.NativeMessageBuilder;
import com.mysql.cj.protocol.a.NativeConstants;
import com.mysql.cj.protocol.a.NativeConstants.IntegerDataType;
import com.mysql.cj.protocol.a.NativeConstants.StringLengthDataType;
import com.mysql.cj.protocol.a.NativeConstants.StringSelfDataType;
import com.mysql.cj.protocol.a.NativePacketPayload;
import com.mysql.cj.result.Field;
import com.mysql.cj.util.LogUtils;
import com.mysql.cj.util.StringUtils;

//TODO should not be protocol-specific

public class ServerPreparedQuery extends AbstractPreparedQuery<ServerPreparedQueryBindings> {

    public static final int BLOB_STREAM_READ_BUF_SIZE = 8192;
    public static final byte OPEN_CURSOR_FLAG = 1;

    /** The ID that the server uses to identify this PreparedStatement */
    private long serverStatementId;

    /** Field-level metadata for parameters */
    private Field[] parameterFields;

    /** Field-level metadata for result sets. From statement prepare. */
    private ColumnDefinition resultFields;

    protected ReadableProperty<Boolean> gatherPerfMetrics;

    protected boolean logSlowQueries = false;

    private boolean useAutoSlowLog;

    protected ReadableProperty<Integer> slowQueryThresholdMillis;

    protected ReadableProperty<Boolean> explainSlowQueries;

    protected boolean queryWasSlow = false;

    protected NativeMessageBuilder commandBuilder = new NativeMessageBuilder(); // TODO use shared builder

    public static ServerPreparedQuery getInstance(NativeSession sess) {
        if (sess.getPropertySet().getBooleanReadableProperty(PropertyDefinitions.PNAME_autoGenerateTestcaseScript).getValue()) {
            return new ServerPreparedQueryTestcaseGenerator(sess);
        }
        return new ServerPreparedQuery(sess);
    }

    protected ServerPreparedQuery(NativeSession sess) {
        super(sess);
        this.gatherPerfMetrics = sess.getPropertySet().getBooleanReadableProperty(PropertyDefinitions.PNAME_gatherPerfMetrics);
        this.logSlowQueries = sess.getPropertySet().getBooleanReadableProperty(PropertyDefinitions.PNAME_logSlowQueries).getValue();
        this.useAutoSlowLog = sess.getPropertySet().getBooleanReadableProperty(PropertyDefinitions.PNAME_autoSlowLog).getValue();
        this.slowQueryThresholdMillis = sess.getPropertySet().getIntegerReadableProperty(PropertyDefinitions.PNAME_slowQueryThresholdMillis);
        this.explainSlowQueries = sess.getPropertySet().getBooleanReadableProperty(PropertyDefinitions.PNAME_explainSlowQueries);

    }

    /**
     * 
     * @param sql
     * @throws IOException
     */
    public void serverPrepare(String sql) throws IOException {
        this.session.checkClosed();

        synchronized (this.session) {
            long begin = 0;

            if (this.profileSQL) {
                begin = System.currentTimeMillis();
            }

            boolean loadDataQuery = StringUtils.startsWithIgnoreCaseAndWs(sql, "LOAD DATA");

            String characterEncoding = null;
            String connectionEncoding = this.session.getPropertySet().getStringReadableProperty(PropertyDefinitions.PNAME_characterEncoding).getValue();

            if (!loadDataQuery && (connectionEncoding != null)) {
                characterEncoding = connectionEncoding;
            }

            NativePacketPayload prepareResultPacket = this.session
                    .sendCommand(this.commandBuilder.buildComStmtPrepare(this.session.getSharedSendPacket(), sql, characterEncoding), false, 0);

            // 4.1.1 and newer use the first byte as an 'ok' or 'error' flag, so move the buffer pointer past it to start reading the statement id.
            prepareResultPacket.setPosition(1);

            this.serverStatementId = prepareResultPacket.readInteger(IntegerDataType.INT4);
            int fieldCount = (int) prepareResultPacket.readInteger(IntegerDataType.INT2);
            setParameterCount((int) prepareResultPacket.readInteger(IntegerDataType.INT2));

            this.queryBindings = new ServerPreparedQueryBindings(this.parameterCount, this.session);
            this.queryBindings.setLoadDataQuery(loadDataQuery);

            this.session.incrementNumberOfPrepares();

            if (this.profileSQL) {
                this.eventSink.consumeEvent(new ProfilerEventImpl(ProfilerEvent.TYPE_PREPARE, "", this.getCurrentCatalog(), this.session.getThreadId(),
                        this.statementId, -1, System.currentTimeMillis(), this.session.getCurrentTimeNanosOrMillis() - begin,
                        this.session.getQueryTimingUnits(), null, LogUtils.findCallingClassAndMethod(new Throwable()), truncateQueryToLog(sql)));
            }

            boolean checkEOF = !this.session.getServerSession().isEOFDeprecated();

            if (this.parameterCount > 0) {
                if (checkEOF) { // Skip the following EOF packet.
                    this.session.getProtocol().skipPacket();
                }

                this.parameterFields = this.session.getProtocol().read(ColumnDefinition.class, new ColumnDefinitionFactory(this.parameterCount, null))
                        .getFields();
            }

            // Read in the result set column information
            if (fieldCount > 0) {
                this.resultFields = this.session.getProtocol().read(ColumnDefinition.class, new ColumnDefinitionFactory(fieldCount, null));
            }
        }
    }

    @Override
    public void statementBegins() {
        super.statementBegins();
        this.queryWasSlow = false;
    }

    /**
     * 
     * @param maxRowsToRetrieve
     * @param createStreamingResultSet
     * @param metadata
     * @return
     */
    public <T extends Resultset> T serverExecute(int maxRowsToRetrieve, boolean createStreamingResultSet, ColumnDefinition metadata,
            ProtocolEntityFactory<T, NativePacketPayload> resultSetFactory) {
        if (this.session.shouldIntercept()) {
            T interceptedResults = this.session.invokeQueryInterceptorsPre(() -> {
                return getOriginalSql();
            }, this, true);

            if (interceptedResults != null) {
                return interceptedResults;
            }
        }
        String queryAsString = "";
        if (this.profileSQL || this.logSlowQueries || this.gatherPerfMetrics.getValue()) {
            queryAsString = asSql(true);
        }

        NativePacketPayload packet = prepareExecutePacket();
        NativePacketPayload resPacket = sendExecutePacket(packet, queryAsString);
        T rs = readExecuteResult(resPacket, maxRowsToRetrieve, createStreamingResultSet, metadata, resultSetFactory, queryAsString);

        return rs;
    }

    public NativePacketPayload prepareExecutePacket() {

        ServerPreparedQueryBindValue[] parameterBindings = this.queryBindings.getBindValues();

        if (this.queryBindings.isLongParameterSwitchDetected()) {
            // Check when values were bound
            boolean firstFound = false;
            long boundTimeToCheck = 0;

            for (int i = 0; i < this.parameterCount - 1; i++) {
                if (parameterBindings[i].isLongData) {
                    if (firstFound && boundTimeToCheck != parameterBindings[i].boundBeforeExecutionNum) {
                        throw ExceptionFactory.createException(
                                Messages.getString("ServerPreparedStatement.11") + Messages.getString("ServerPreparedStatement.12"),
                                MysqlErrorNumbers.SQL_STATE_DRIVER_NOT_CAPABLE, 0, true, null, this.session.getExceptionInterceptor());
                    }
                    firstFound = true;
                    boundTimeToCheck = parameterBindings[i].boundBeforeExecutionNum;
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
            if (parameterBindings[i].isLongData) {
                serverLongData(i, parameterBindings[i]);
            }
        }

        //
        // store the parameter values
        //

        NativePacketPayload packet = this.session.getSharedSendPacket();
        packet.writeInteger(IntegerDataType.INT1, NativeConstants.COM_STMT_EXECUTE);
        packet.writeInteger(IntegerDataType.INT4, this.serverStatementId);

        // we only create cursor-backed result sets if
        // a) The query is a SELECT
        // b) The server supports it
        // c) We know it is forward-only (note this doesn't preclude updatable result sets)
        // d) The user has set a fetch size
        if (this.resultFields != null && this.resultFields.getFields() != null && this.useCursorFetch && this.resultSetType == Type.FORWARD_ONLY
                && this.fetchSize > 0) {
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
        packet.writeInteger(IntegerDataType.INT1, this.queryBindings.getSendTypesToServer().get() ? (byte) 1 : (byte) 0);

        if (this.queryBindings.getSendTypesToServer().get()) {
            /*
             * Store types of parameters in the first package that is sent to the server.
             */
            for (int i = 0; i < this.parameterCount; i++) {
                packet.writeInteger(IntegerDataType.INT2, parameterBindings[i].bufferType);
            }
        }

        //
        // store the parameter values
        //
        for (int i = 0; i < this.parameterCount; i++) {
            if (!parameterBindings[i].isLongData) {
                if (!parameterBindings[i].isNull()) {
                    parameterBindings[i].storeBinding(packet, this.queryBindings.isLoadDataQuery(), this.charEncoding, this.session.getExceptionInterceptor());
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

        return packet;
    }

    public NativePacketPayload sendExecutePacket(NativePacketPayload packet, String queryAsString) { // TODO queryAsString should be shared instead of passed

        long begin = 0;

        boolean gatherPerformanceMetrics = this.gatherPerfMetrics.getValue();

        if (this.profileSQL || this.logSlowQueries || gatherPerformanceMetrics) {
            begin = this.session.getCurrentTimeNanosOrMillis();
        }

        resetCancelledState();

        CancelQueryTask timeoutTask = null;

        try {
            // Get this before executing to avoid a shared packet pollution in the case some other query is issued internally, such as when using I_S.

            timeoutTask = startQueryTimer(this, this.timeoutInMillis);

            statementBegins();

            NativePacketPayload resultPacket = this.session.sendCommand(packet, false, 0);

            long queryEndTime = 0L;

            if (this.logSlowQueries || gatherPerformanceMetrics || this.profileSQL) {
                queryEndTime = this.session.getCurrentTimeNanosOrMillis();
            }

            if (timeoutTask != null) {
                stopQueryTimer(timeoutTask, true, true);
                timeoutTask = null;
            }

            if (this.logSlowQueries || gatherPerformanceMetrics) {
                long elapsedTime = queryEndTime - begin;

                if (this.logSlowQueries) {
                    if (this.useAutoSlowLog) {
                        this.queryWasSlow = elapsedTime > this.slowQueryThresholdMillis.getValue();
                    } else {
                        this.queryWasSlow = this.session.getProtocol().getMetricsHolder().isAbonormallyLongQuery(elapsedTime);

                        this.session.getProtocol().getMetricsHolder().reportQueryTime(elapsedTime);
                    }
                }

                if (this.queryWasSlow) {

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

                    this.eventSink.consumeEvent(new ProfilerEventImpl(ProfilerEvent.TYPE_SLOW_QUERY, "", getCurrentCatalog(), this.session.getThreadId(),
                            getId(), 0, System.currentTimeMillis(), elapsedTime, this.session.getQueryTimingUnits(), null,
                            LogUtils.findCallingClassAndMethod(new Throwable()), mesgBuf.toString()));
                }

                if (gatherPerformanceMetrics) {
                    this.session.registerQueryExecutionTime(elapsedTime);
                }
            }

            this.session.incrementNumberOfPreparedExecutes();

            if (this.profileSQL) {
                this.setEventSink(ProfilerEventHandlerFactory.getInstance(this.session));

                this.eventSink.consumeEvent(new ProfilerEventImpl(ProfilerEvent.TYPE_EXECUTE, "", getCurrentCatalog(), this.session.getThreadId(),
                        this.statementId, -1, System.currentTimeMillis(), this.session.getCurrentTimeNanosOrMillis() - begin,
                        this.session.getQueryTimingUnits(), null, LogUtils.findCallingClassAndMethod(new Throwable()), truncateQueryToLog(queryAsString)));
            }

            return resultPacket;

        } catch (CJException sqlEx) {
            if (this.session.shouldIntercept()) {
                this.session.invokeQueryInterceptorsPost(() -> {
                    return getOriginalSql();
                }, this, null, true);
            }

            throw sqlEx;
        } finally {
            this.statementExecuting.set(false);
            stopQueryTimer(timeoutTask, false, false);
        }
    }

    public <T extends Resultset> T readExecuteResult(NativePacketPayload resultPacket, int maxRowsToRetrieve, boolean createStreamingResultSet,
            ColumnDefinition metadata, ProtocolEntityFactory<T, NativePacketPayload> resultSetFactory, String queryAsString) { // TODO queryAsString should be shared instead of passed
        try {
            long fetchStartTime = 0;

            if (this.profileSQL || this.logSlowQueries || this.gatherPerfMetrics.getValue()) {
                fetchStartTime = this.session.getCurrentTimeNanosOrMillis();
            }

            T rs = this.session.getProtocol().readAllResults(maxRowsToRetrieve, createStreamingResultSet, resultPacket, true,
                    metadata != null ? metadata : this.resultFields, resultSetFactory);

            if (this.session.shouldIntercept()) {
                T interceptedResults = this.session.invokeQueryInterceptorsPost(() -> {
                    return getOriginalSql();
                }, this, rs, true);

                if (interceptedResults != null) {
                    rs = interceptedResults;
                }
            }

            if (this.profileSQL) {
                long fetchEndTime = this.session.getCurrentTimeNanosOrMillis();

                this.eventSink.consumeEvent(new ProfilerEventImpl(ProfilerEvent.TYPE_FETCH, "", getCurrentCatalog(), this.session.getThreadId(), getId(),
                        rs.getResultId(), System.currentTimeMillis(), (fetchEndTime - fetchStartTime), this.session.getQueryTimingUnits(), null,
                        LogUtils.findCallingClassAndMethod(new Throwable()), null));
            }

            if (this.queryWasSlow && this.explainSlowQueries.getValue()) {
                this.session.getProtocol().explainSlowQuery(queryAsString, queryAsString);
            }

            this.queryBindings.getSendTypesToServer().set(false);
            //this.results = rs;

            if (this.session.hadWarnings()) {
                this.session.getProtocol().scanForAndThrowDataTruncation();
            }

            return rs;
        } catch (IOException ioEx) {
            throw ExceptionFactory.createCommunicationsException(this.session.getPropertySet(), this.session.getServerSession(),
                    this.session.getProtocol().getPacketSentTimeHolder().getLastPacketSentTime(),
                    this.session.getProtocol().getPacketReceivedTimeHolder().getLastPacketReceivedTime(), ioEx, this.session.getExceptionInterceptor());
        } catch (CJException sqlEx) {
            if (this.session.shouldIntercept()) {
                this.session.invokeQueryInterceptorsPost(() -> {
                    return getOriginalSql();
                }, this, null, true);
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
     * @param longData
     * 
     */
    private void serverLongData(int parameterIndex, ServerPreparedQueryBindValue longData) {
        synchronized (this) {
            NativePacketPayload packet = this.session.getSharedSendPacket();

            Object value = longData.value;

            if (value instanceof byte[]) {
                this.session.sendCommand(this.commandBuilder.buildComStmtSendLongData(packet, this.serverStatementId, parameterIndex, (byte[]) value), true, 0);
            } else if (value instanceof InputStream) {
                storeStream(parameterIndex, packet, (InputStream) value);
            } else if (value instanceof java.sql.Blob) {
                try {
                    storeStream(parameterIndex, packet, ((java.sql.Blob) value).getBinaryStream());
                } catch (Throwable t) {
                    throw ExceptionFactory.createException(t.getMessage(), this.session.getExceptionInterceptor());
                }
            } else if (value instanceof Reader) {
                storeReader(parameterIndex, packet, (Reader) value);
            } else {
                throw ExceptionFactory.createException(WrongArgumentException.class,
                        Messages.getString("ServerPreparedStatement.18") + value.getClass().getName() + "'", this.session.getExceptionInterceptor());
            }
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

    public void storeStream(int parameterIndex, NativePacketPayload packet, InputStream inStream) {
        this.session.checkClosed();
        synchronized (this.session) {
            byte[] buf = new byte[BLOB_STREAM_READ_BUF_SIZE];

            int numRead = 0;

            try {
                int bytesInPacket = 0;
                int totalBytesRead = 0;
                int bytesReadAtLastSend = 0;
                int packetIsFullAt = this.session.getPropertySet().getMemorySizeReadableProperty(PropertyDefinitions.PNAME_blobSendChunkSize).getValue();

                packet.setPosition(0);
                packet.writeInteger(IntegerDataType.INT1, NativeConstants.COM_STMT_SEND_LONG_DATA);
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

                        this.session.sendCommand(packet, true, 0);

                        bytesInPacket = 0;
                        packet.setPosition(0);
                        packet.writeInteger(IntegerDataType.INT1, NativeConstants.COM_STMT_SEND_LONG_DATA);
                        packet.writeInteger(IntegerDataType.INT4, this.serverStatementId);
                        packet.writeInteger(IntegerDataType.INT2, parameterIndex);
                    }
                }

                if (totalBytesRead != bytesReadAtLastSend) {
                    this.session.sendCommand(packet, true, 0);
                }

                if (!readAny) {
                    this.session.sendCommand(packet, true, 0);
                }
            } catch (IOException ioEx) {
                throw ExceptionFactory.createException(Messages.getString("ServerPreparedStatement.25") + ioEx.toString(), ioEx,
                        this.session.getExceptionInterceptor());
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

    // TODO: Investigate using NIO to do this faster
    public void storeReader(int parameterIndex, NativePacketPayload packet, Reader inStream) {
        this.session.checkClosed();
        synchronized (this.session) {
            String forcedEncoding = this.session.getPropertySet().getStringReadableProperty(PropertyDefinitions.PNAME_clobCharacterEncoding).getStringValue();

            String clobEncoding = (forcedEncoding == null
                    ? this.session.getPropertySet().getStringReadableProperty(PropertyDefinitions.PNAME_characterEncoding).getValue() : forcedEncoding);

            int maxBytesChar = 2;

            if (clobEncoding != null) {
                if (!clobEncoding.equals("UTF-16")) {
                    maxBytesChar = this.session.getServerSession().getMaxBytesPerChar(clobEncoding);

                    if (maxBytesChar == 1) {
                        maxBytesChar = 2; // for safety
                    }
                } else {
                    maxBytesChar = 4;
                }
            }

            char[] buf = new char[ServerPreparedQuery.BLOB_STREAM_READ_BUF_SIZE / maxBytesChar];

            int numRead = 0;

            int bytesInPacket = 0;
            int totalBytesRead = 0;
            int bytesReadAtLastSend = 0;
            int packetIsFullAt = this.session.getPropertySet().getMemorySizeReadableProperty(PropertyDefinitions.PNAME_blobSendChunkSize).getValue();

            try {
                packet.setPosition(0);
                packet.writeInteger(IntegerDataType.INT1, NativeConstants.COM_STMT_SEND_LONG_DATA);
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

                        this.session.sendCommand(packet, true, 0);

                        bytesInPacket = 0;
                        packet.setPosition(0);
                        packet.writeInteger(IntegerDataType.INT1, NativeConstants.COM_STMT_SEND_LONG_DATA);
                        packet.writeInteger(IntegerDataType.INT4, this.serverStatementId);
                        packet.writeInteger(IntegerDataType.INT2, parameterIndex);
                    }
                }

                if (totalBytesRead != bytesReadAtLastSend) {
                    this.session.sendCommand(packet, true, 0);
                }

                if (!readAny) {
                    this.session.sendCommand(packet, true, 0);
                }
            } catch (IOException ioEx) {
                throw ExceptionFactory.createException(Messages.getString("ServerPreparedStatement.24") + ioEx.toString(), ioEx,
                        this.session.getExceptionInterceptor());
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

    /**
     * 
     * @param clearServerParameters
     * @return Whether or not the long parameters have been 'switched' back to normal parameters.
     *         We cannot execute() if clearParameters() has not been called in this case.
     */
    public void clearParameters(boolean clearServerParameters) {
        boolean hadLongData = false;

        if (this.queryBindings != null) {
            hadLongData = this.queryBindings.clearBindValues();
            this.queryBindings.setLongParameterSwitchDetected(clearServerParameters && hadLongData ? false : true);
        }

        if (clearServerParameters && hadLongData) {
            serverResetStatement();
        }
    }

    public void serverResetStatement() {
        this.session.checkClosed();
        synchronized (this.session) {
            try {
                this.session.sendCommand(this.commandBuilder.buildComStmtReset(this.session.getSharedSendPacket(), this.serverStatementId), false, 0);
            } finally {
                this.session.clearInputStream();
            }
        }
    }

    /**
     * Computes the maximum parameter set size and the size of the entire batch given
     * the number of arguments in the batch.
     */
    @Override
    protected long[] computeMaxParameterSetSizeAndBatchSize(int numBatchedArgs) {

        long sizeOfEntireBatch = 1 + /* com_execute */+4 /* stmt id */ + 1 /* flags */ + 4 /* batch count padding */;
        long maxSizeOfParameterSet = 0;

        for (int i = 0; i < numBatchedArgs; i++) {
            ServerPreparedQueryBindValue[] paramArg = ((ServerPreparedQueryBindings) this.batchedArgs.get(i)).getBindValues();

            long sizeOfParameterSet = (this.parameterCount + 7) / 8; // for isNull
            sizeOfParameterSet += this.parameterCount * 2; // have to send types

            ServerPreparedQueryBindValue[] parameterBindings = this.queryBindings.getBindValues();
            for (int j = 0; j < parameterBindings.length; j++) {
                if (!paramArg[j].isNull()) {

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

    private String truncateQueryToLog(String sql) {
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

    @Override
    public <M extends Message> M fillSendPacket() {
        return null; // we don't use this type of packet
    }

    @Override
    public <M extends Message> M fillSendPacket(QueryBindings<?> bindings) {
        return null; // we don't use this type of packet
    }

}
