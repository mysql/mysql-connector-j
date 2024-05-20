/*
 * Copyright (c) 2015, 2024, Oracle and/or its affiliates.
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

package com.mysql.cj.protocol.a;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.lang.ref.SoftReference;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.net.MalformedURLException;
import java.net.Socket;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.UnknownHostException;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.DataTruncation;
import java.sql.Date;
import java.sql.SQLWarning;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.TimeZone;
import java.util.function.Supplier;

import com.mysql.cj.CharsetMapping;
import com.mysql.cj.MessageBuilder;
import com.mysql.cj.Messages;
import com.mysql.cj.MysqlType;
import com.mysql.cj.NativeCharsetSettings;
import com.mysql.cj.NativeSession;
import com.mysql.cj.Query;
import com.mysql.cj.QueryResult;
import com.mysql.cj.ServerPreparedQuery;
import com.mysql.cj.ServerVersion;
import com.mysql.cj.Session;
import com.mysql.cj.TransactionEventHandler;
import com.mysql.cj.conf.PropertyKey;
import com.mysql.cj.conf.PropertySet;
import com.mysql.cj.conf.RuntimeProperty;
import com.mysql.cj.conf.RuntimeProperty.RuntimePropertyListener;
import com.mysql.cj.exceptions.CJCommunicationsException;
import com.mysql.cj.exceptions.CJConnectionFeatureNotAvailableException;
import com.mysql.cj.exceptions.CJException;
import com.mysql.cj.exceptions.CJOperationNotSupportedException;
import com.mysql.cj.exceptions.CJPacketTooBigException;
import com.mysql.cj.exceptions.ClosedOnExpiredPasswordException;
import com.mysql.cj.exceptions.DataTruncationException;
import com.mysql.cj.exceptions.ExceptionFactory;
import com.mysql.cj.exceptions.FeatureNotAvailableException;
import com.mysql.cj.exceptions.MysqlErrorNumbers;
import com.mysql.cj.exceptions.PasswordExpiredException;
import com.mysql.cj.exceptions.WrongArgumentException;
import com.mysql.cj.interceptors.QueryInterceptor;
import com.mysql.cj.jdbc.exceptions.MysqlDataTruncation;
import com.mysql.cj.log.BaseMetricsHolder;
import com.mysql.cj.log.Log;
import com.mysql.cj.log.ProfilerEvent;
import com.mysql.cj.log.ProfilerEventHandler;
import com.mysql.cj.protocol.AbstractProtocol;
import com.mysql.cj.protocol.ColumnDefinition;
import com.mysql.cj.protocol.ExportControlled;
import com.mysql.cj.protocol.FullReadInputStream;
import com.mysql.cj.protocol.Message;
import com.mysql.cj.protocol.MessageReader;
import com.mysql.cj.protocol.MessageSender;
import com.mysql.cj.protocol.PacketReceivedTimeHolder;
import com.mysql.cj.protocol.PacketSentTimeHolder;
import com.mysql.cj.protocol.Protocol;
import com.mysql.cj.protocol.ProtocolEntity;
import com.mysql.cj.protocol.ProtocolEntityFactory;
import com.mysql.cj.protocol.ProtocolEntityReader;
import com.mysql.cj.protocol.ResultBuilder;
import com.mysql.cj.protocol.Resultset;
import com.mysql.cj.protocol.Resultset.Concurrency;
import com.mysql.cj.protocol.Resultset.Type;
import com.mysql.cj.protocol.ResultsetRow;
import com.mysql.cj.protocol.ResultsetRows;
import com.mysql.cj.protocol.ServerSession;
import com.mysql.cj.protocol.SocketConnection;
import com.mysql.cj.protocol.ValueEncoder;
import com.mysql.cj.protocol.a.NativeConstants.IntegerDataType;
import com.mysql.cj.protocol.a.NativeConstants.StringLengthDataType;
import com.mysql.cj.protocol.a.NativeConstants.StringSelfDataType;
import com.mysql.cj.protocol.a.result.OkPacket;
import com.mysql.cj.result.Field;
import com.mysql.cj.result.IntegerValueFactory;
import com.mysql.cj.result.Row;
import com.mysql.cj.result.StringValueFactory;
import com.mysql.cj.result.ValueFactory;
import com.mysql.cj.telemetry.TelemetryAttribute;
import com.mysql.cj.telemetry.TelemetryScope;
import com.mysql.cj.telemetry.TelemetrySpan;
import com.mysql.cj.telemetry.TelemetrySpanName;
import com.mysql.cj.util.LazyString;
import com.mysql.cj.util.StringUtils;
import com.mysql.cj.util.TestUtils;
import com.mysql.cj.util.TimeUtil;
import com.mysql.cj.util.Util;

public class NativeProtocol extends AbstractProtocol<NativePacketPayload> implements Protocol<NativePacketPayload>, RuntimePropertyListener {

    protected static final int INITIAL_PACKET_SIZE = 1024;
    protected static final int COMP_HEADER_LENGTH = 3;
    protected static final int MAX_QUERY_SIZE_TO_EXPLAIN = 1024 * 1024; // don't explain queries above 1MB
    protected static final int SSL_REQUEST_LENGTH = 32;
    private static final String EXPLAINABLE_STATEMENT = "SELECT";
    private static final String[] EXPLAINABLE_STATEMENT_EXTENSION = new String[] { "INSERT", "UPDATE", "REPLACE", "DELETE" };

    protected MessageSender<NativePacketPayload> packetSender;
    protected MessageReader<NativePacketHeader, NativePacketPayload> packetReader;

    protected NativeServerSession serverSession;

    /** Track this to manually shut down. */
    protected CompressedPacketSender compressedPacketSender;

    protected NativePacketPayload sharedSendPacket = null;

    /** Use this when reading in rows to avoid thousands of new() calls, because the byte arrays just get copied out of the packet anyway */
    protected NativePacketPayload reusablePacket = null;

    /**
     * Packet used for 'LOAD DATA LOCAL INFILE'
     * We use a SoftReference, so that we don't penalize intermittent use of this feature
     */
    private SoftReference<NativePacketPayload> loadFileBufRef;

    protected byte packetSequence = 0;
    protected boolean useCompression = false;

    private RuntimeProperty<Integer> maxAllowedPacket;
    private RuntimeProperty<Boolean> useServerPrepStmts;

    private boolean autoGenerateTestcaseScript;

    private boolean logSlowQueries = false;
    private boolean useAutoSlowLog;

    private boolean profileSQL = false;

    private long slowQueryThreshold;

    private int commandCount = 0;

    protected boolean hadWarnings = false;
    private int warningCount = 0;

    protected Map<Class<? extends ProtocolEntity>, ProtocolEntityReader<? extends ProtocolEntity, ? extends Message>> PROTOCOL_ENTITY_CLASS_TO_TEXT_READER;
    protected Map<Class<? extends ProtocolEntity>, ProtocolEntityReader<? extends ProtocolEntity, ? extends Message>> PROTOCOL_ENTITY_CLASS_TO_BINARY_READER;

    private int statementExecutionDepth = 0;
    private List<QueryInterceptor> queryInterceptors;

    private RuntimeProperty<Boolean> maintainTimeStats;
    private RuntimeProperty<Integer> maxQuerySizeToLog;

    private InputStream localInfileInputStream;

    private BaseMetricsHolder metricsHolder;

    static Map<Class<?>, Supplier<ValueEncoder>> DEFAULT_ENCODERS = new HashMap<>();
    static {
        DEFAULT_ENCODERS.put(BigDecimal.class, NumberValueEncoder::new);
        DEFAULT_ENCODERS.put(BigInteger.class, NumberValueEncoder::new);
        DEFAULT_ENCODERS.put(Blob.class, BlobValueEncoder::new);
        DEFAULT_ENCODERS.put(Boolean.class, BooleanValueEncoder::new);
        DEFAULT_ENCODERS.put(Byte.class, NumberValueEncoder::new);
        DEFAULT_ENCODERS.put(byte[].class, ByteArrayValueEncoder::new);
        DEFAULT_ENCODERS.put(Calendar.class, UtilCalendarValueEncoder::new);
        DEFAULT_ENCODERS.put(Clob.class, ClobValueEncoder::new);
        DEFAULT_ENCODERS.put(Date.class, SqlDateValueEncoder::new);
        DEFAULT_ENCODERS.put(java.util.Date.class, UtilDateValueEncoder::new);
        DEFAULT_ENCODERS.put(Double.class, NumberValueEncoder::new);
        DEFAULT_ENCODERS.put(Duration.class, DurationValueEncoder::new);
        DEFAULT_ENCODERS.put(Float.class, NumberValueEncoder::new);
        DEFAULT_ENCODERS.put(InputStream.class, InputStreamValueEncoder::new);
        DEFAULT_ENCODERS.put(Instant.class, InstantValueEncoder::new);
        DEFAULT_ENCODERS.put(Integer.class, NumberValueEncoder::new);
        DEFAULT_ENCODERS.put(LocalDate.class, LocalDateValueEncoder::new);
        DEFAULT_ENCODERS.put(LocalDateTime.class, LocalDateTimeValueEncoder::new);
        DEFAULT_ENCODERS.put(LocalTime.class, LocalTimeValueEncoder::new);
        DEFAULT_ENCODERS.put(Long.class, NumberValueEncoder::new);
        DEFAULT_ENCODERS.put(OffsetDateTime.class, OffsetDateTimeValueEncoder::new);
        DEFAULT_ENCODERS.put(OffsetTime.class, OffsetTimeValueEncoder::new);
        DEFAULT_ENCODERS.put(Reader.class, ReaderValueEncoder::new);
        DEFAULT_ENCODERS.put(Short.class, NumberValueEncoder::new);
        DEFAULT_ENCODERS.put(String.class, StringValueEncoder::new);
        DEFAULT_ENCODERS.put(Time.class, SqlTimeValueEncoder::new);
        DEFAULT_ENCODERS.put(Timestamp.class, SqlTimestampValueEncoder::new);
        DEFAULT_ENCODERS.put(ZonedDateTime.class, ZonedDateTimeValueEncoder::new);
    }

    private NativeMessageBuilder nativeMessageBuilder = null;

    public static NativeProtocol getInstance(Session session, SocketConnection socketConnection, PropertySet propertySet, Log log,
            TransactionEventHandler transactionManager) {
        NativeProtocol protocol = new NativeProtocol(log);
        protocol.init(session, socketConnection, propertySet, transactionManager);
        return protocol;
    }

    public NativeProtocol(Log logger) {
        this.log = logger;
        this.metricsHolder = new BaseMetricsHolder();
    }

    @Override
    public void init(Session sess, SocketConnection phConnection, PropertySet propSet, TransactionEventHandler trManager) {
        super.init(sess, phConnection, propSet, trManager);

        this.maintainTimeStats = this.propertySet.getBooleanProperty(PropertyKey.maintainTimeStats);
        this.maxQuerySizeToLog = this.propertySet.getIntegerProperty(PropertyKey.maxQuerySizeToLog);
        this.useAutoSlowLog = this.propertySet.getBooleanProperty(PropertyKey.autoSlowLog).getValue();
        this.logSlowQueries = this.propertySet.getBooleanProperty(PropertyKey.logSlowQueries).getValue();
        this.maxAllowedPacket = this.propertySet.getIntegerProperty(PropertyKey.maxAllowedPacket);
        this.profileSQL = this.propertySet.getBooleanProperty(PropertyKey.profileSQL).getValue();
        this.autoGenerateTestcaseScript = this.propertySet.getBooleanProperty(PropertyKey.autoGenerateTestcaseScript).getValue();
        this.useServerPrepStmts = this.propertySet.getBooleanProperty(PropertyKey.useServerPrepStmts);

        this.reusablePacket = new NativePacketPayload(INITIAL_PACKET_SIZE);

        try {
            this.packetSender = new SimplePacketSender(this.socketConnection.getMysqlOutput());
            this.packetReader = new SimplePacketReader(this.socketConnection, this.maxAllowedPacket);
        } catch (IOException ioEx) {
            throw ExceptionFactory.createCommunicationsException(this.propertySet, this.serverSession, this.getPacketSentTimeHolder(),
                    this.getPacketReceivedTimeHolder(), ioEx, getExceptionInterceptor());
        }

        if (this.propertySet.getBooleanProperty(PropertyKey.logSlowQueries).getValue()) {
            calculateSlowQueryThreshold();
        }

        this.authProvider = new NativeAuthenticationProvider();
        this.authProvider.init(this, this.getPropertySet(), this.socketConnection.getExceptionInterceptor());

        Map<Class<? extends ProtocolEntity>, ProtocolEntityReader<? extends ProtocolEntity, NativePacketPayload>> protocolEntityClassToTextReader = new HashMap<>();
        protocolEntityClassToTextReader.put(ColumnDefinition.class, new ColumnDefinitionReader(this));
        protocolEntityClassToTextReader.put(ResultsetRow.class, new ResultsetRowReader(this));
        protocolEntityClassToTextReader.put(Resultset.class, new TextResultsetReader(this));
        this.PROTOCOL_ENTITY_CLASS_TO_TEXT_READER = Collections.unmodifiableMap(protocolEntityClassToTextReader);

        Map<Class<? extends ProtocolEntity>, ProtocolEntityReader<? extends ProtocolEntity, NativePacketPayload>> protocolEntityClassToBinaryReader = new HashMap<>();
        protocolEntityClassToBinaryReader.put(ColumnDefinition.class, new ColumnDefinitionReader(this));
        protocolEntityClassToBinaryReader.put(Resultset.class, new BinaryResultsetReader(this));
        this.PROTOCOL_ENTITY_CLASS_TO_BINARY_READER = Collections.unmodifiableMap(protocolEntityClassToBinaryReader);
    }

    public Session getSession() {
        return this.session;
    }

    @Override
    public MessageBuilder<NativePacketPayload> getMessageBuilder() {
        return getNativeMessageBuilder();
    }

    public MessageSender<NativePacketPayload> getPacketSender() {
        return this.packetSender;
    }

    public MessageReader<NativePacketHeader, NativePacketPayload> getPacketReader() {
        return this.packetReader;
    }

    private NativeMessageBuilder getNativeMessageBuilder() {
        if (this.nativeMessageBuilder != null) {
            return this.nativeMessageBuilder;
        }
        return this.nativeMessageBuilder = new NativeMessageBuilder(this.serverSession.supportsQueryAttributes());
    }

    @Override
    public Supplier<ValueEncoder> getValueEncoderSupplier(Object obj) {
        if (obj == null) {
            return NullValueEncoder::new;
        }
        Supplier<ValueEncoder> res = DEFAULT_ENCODERS.get(obj.getClass());
        if (res == null) {
            Optional<Supplier<ValueEncoder>> mysqlType = DEFAULT_ENCODERS.entrySet().stream().filter(m -> m.getKey().isAssignableFrom(obj.getClass()))
                    .map(Entry::getValue).findFirst();
            if (mysqlType.isPresent()) {
                res = mysqlType.get();
            }
        }
        return res;
    }

    /**
     * Negotiates the SSL communication channel used when connecting to a MySQL server that has SSL enabled.
     */
    @Override
    public void negotiateSSLConnection() {
        if (!ExportControlled.enabled()) {
            throw new CJConnectionFeatureNotAvailableException(this.getPropertySet(), this.serverSession, this.getPacketSentTimeHolder(), null);
        }

        long clientParam = this.serverSession.getClientParam();

        NativePacketPayload packet = new NativePacketPayload(SSL_REQUEST_LENGTH);
        packet.writeInteger(IntegerDataType.INT4, clientParam);
        packet.writeInteger(IntegerDataType.INT4, NativeConstants.MAX_PACKET_SIZE);
        packet.writeInteger(IntegerDataType.INT1, this.serverSession.getCharsetSettings().configurePreHandshake(false));
        packet.writeBytes(StringLengthDataType.STRING_FIXED, new byte[23]);  // Set of bytes reserved for future use.

        send(packet, packet.getPosition());

        try {
            this.socketConnection.performTlsHandshake(this.serverSession, this.log);

            // i/o streams were replaced, build new packet sender/reader
            this.packetSender = new SimplePacketSender(this.socketConnection.getMysqlOutput());
            this.packetReader = new SimplePacketReader(this.socketConnection, this.maxAllowedPacket);

        } catch (FeatureNotAvailableException e) {
            throw new CJConnectionFeatureNotAvailableException(this.getPropertySet(), this.serverSession, this.getPacketSentTimeHolder(), e);
        } catch (IOException e) {
            throw ExceptionFactory.createCommunicationsException(this.propertySet, this.serverSession, this.getPacketSentTimeHolder(),
                    this.getPacketReceivedTimeHolder(), e, getExceptionInterceptor());
        }
    }

    public void rejectProtocol(NativePacketPayload msg) {
        try {
            this.socketConnection.getMysqlSocket().close();
        } catch (Exception e) {
            // ignore
        }

        int errno = 2000;

        NativePacketPayload buf = msg;
        buf.setPosition(1); // skip the packet type
        errno = (int) buf.readInteger(IntegerDataType.INT2);

        String serverErrorMessage = "";
        try {
            serverErrorMessage = buf.readString(StringSelfDataType.STRING_TERM, "ASCII");
        } catch (Exception e) {
            //
        }

        StringBuilder errorBuf = new StringBuilder(Messages.getString("Protocol.0"));
        errorBuf.append(serverErrorMessage);
        errorBuf.append("\"");

        String xOpen = MysqlErrorNumbers.mysqlToSqlState(errno);

        throw ExceptionFactory.createException(MysqlErrorNumbers.get(xOpen) + ", " + errorBuf.toString(), xOpen, errno, false, null, getExceptionInterceptor());
    }

    @Override
    public void beforeHandshake() {
        // Reset packet sequences
        this.packetReader.resetMessageSequence();

        // Create session state
        this.serverSession = new NativeServerSession(this.propertySet);

        this.serverSession.setCharsetSettings(new NativeCharsetSettings((NativeSession) this.session));

        // Read the first packet
        this.serverSession.setCapabilities(readServerCapabilities());
    }

    @Override
    public void afterHandshake() {
        checkTransactionState();

        try {
            //
            // Can't enable compression until after handshake
            //
            if ((this.serverSession.getCapabilities().getCapabilityFlags() & NativeServerSession.CLIENT_COMPRESS) != 0
                    && this.propertySet.getBooleanProperty(PropertyKey.useCompression).getValue()
                    && !(this.socketConnection.getMysqlInput().getUnderlyingStream() instanceof CompressedInputStream)) {
                this.useCompression = true;
                this.socketConnection.setMysqlInput(new FullReadInputStream(new CompressedInputStream(this.socketConnection.getMysqlInput(),
                        this.propertySet.getBooleanProperty(PropertyKey.traceProtocol), this.log)));
                this.compressedPacketSender = new CompressedPacketSender(this.socketConnection.getMysqlOutput());
                this.packetSender = this.compressedPacketSender;
            }

            applyPacketDecorators(this.packetSender, this.packetReader);

            this.socketConnection.getSocketFactory().afterHandshake();
        } catch (IOException ioEx) {
            throw ExceptionFactory.createCommunicationsException(this.propertySet, this.serverSession, this.getPacketSentTimeHolder(),
                    this.getPacketReceivedTimeHolder(), ioEx, getExceptionInterceptor());
        }

        // Changing defaults for 8.0.3+ server: PNAME_useInformationSchema=true
        RuntimeProperty<Boolean> useInformationSchema = this.propertySet.<Boolean>getProperty(PropertyKey.useInformationSchema);
        if (versionMeetsMinimum(8, 0, 3) && !useInformationSchema.getValue() && !useInformationSchema.isExplicitlySet()) {
            useInformationSchema.setValue(true);
        }

        // listen for properties changes to allow decorators reconfiguration
        this.maintainTimeStats.addListener(this);
        this.propertySet.getBooleanProperty(PropertyKey.traceProtocol).addListener(this);
        this.propertySet.getBooleanProperty(PropertyKey.enablePacketDebug).addListener(this);
    }

    @Override
    public void handlePropertyChange(RuntimeProperty<?> prop) {
        switch (prop.getPropertyDefinition().getPropertyKey()) {
            case maintainTimeStats:
            case traceProtocol:
            case enablePacketDebug:
                applyPacketDecorators(this.packetSender.undecorateAll(), this.packetReader.undecorateAll());
                break;
            default:
                break;
        }
    }

    /**
     * Apply optional decorators to configured PacketSender and PacketReader.
     *
     * @param sender
     *            {@link MessageSender}
     * @param messageReader
     *            {@link MessageReader}
     */
    public void applyPacketDecorators(MessageSender<NativePacketPayload> sender, MessageReader<NativePacketHeader, NativePacketPayload> messageReader) {
        TimeTrackingPacketSender ttSender = null;
        TimeTrackingPacketReader ttReader = null;
        LinkedList<StringBuilder> debugRingBuffer = null;

        if (this.maintainTimeStats.getValue()) {
            ttSender = new TimeTrackingPacketSender(sender);
            sender = ttSender;

            ttReader = new TimeTrackingPacketReader(messageReader);
            messageReader = ttReader;
        }

        if (this.propertySet.getBooleanProperty(PropertyKey.traceProtocol).getValue()) {
            sender = new TracingPacketSender(sender, this.log, this.socketConnection.getHost(), getServerSession().getCapabilities().getThreadId());
            messageReader = new TracingPacketReader(messageReader, this.log);
        }

        if (this.getPropertySet().getBooleanProperty(PropertyKey.enablePacketDebug).getValue()) {

            debugRingBuffer = new LinkedList<>();

            sender = new DebugBufferingPacketSender(sender, debugRingBuffer, this.propertySet.getIntegerProperty(PropertyKey.packetDebugBufferSize));
            messageReader = new DebugBufferingPacketReader(messageReader, debugRingBuffer,
                    this.propertySet.getIntegerProperty(PropertyKey.packetDebugBufferSize));
        }

        // do it after other decorators to have trace and debug applied to individual packets
        messageReader = new MultiPacketReader(messageReader);

        // atomic replacement of currently used objects
        synchronized (this.packetReader) {
            this.packetReader = messageReader;
            this.packetDebugRingBuffer = debugRingBuffer;
            this.setPacketSentTimeHolder(ttSender != null ? ttSender : new PacketSentTimeHolder() {
            });
        }
        synchronized (this.packetSender) {
            this.packetSender = sender;
            this.setPacketReceivedTimeHolder(ttReader != null ? ttReader : new PacketReceivedTimeHolder() {
            });
        }
    }

    @Override
    public NativeCapabilities readServerCapabilities() {
        // Read the first packet
        NativePacketPayload buf = readMessage(null);

        // Server Greeting Error packet instead of Server Greeting
        if (buf.isErrorPacket()) {
            rejectProtocol(buf);
        }

        return new NativeCapabilities(buf);
    }

    @Override
    public NativeServerSession getServerSession() {
        return this.serverSession;
    }

    @Override
    public void changeDatabase(String database) {
        if (database == null || database.length() == 0) {
            return;
        }

        TelemetrySpan span1 = this.session.getTelemetryHandler().startSpan(TelemetrySpanName.CHANGE_DATABASE);
        try (TelemetryScope scope1 = span1.makeCurrent()) {
            span1.setAttribute(TelemetryAttribute.DB_NAME, database);
            span1.setAttribute(TelemetryAttribute.DB_OPERATION, TelemetryAttribute.OPERATION_INIT_DB);
            span1.setAttribute(TelemetryAttribute.DB_STATEMENT, TelemetryAttribute.OPERATION_INIT_DB + TelemetryAttribute.STATEMENT_SUFFIX);
            span1.setAttribute(TelemetryAttribute.DB_SYSTEM, TelemetryAttribute.DB_SYSTEM_DEFAULT);
            span1.setAttribute(TelemetryAttribute.DB_USER, this.session.getHostInfo().getUser());
            span1.setAttribute(TelemetryAttribute.THREAD_ID, Thread.currentThread().getId());
            span1.setAttribute(TelemetryAttribute.THREAD_NAME, Thread.currentThread().getName());

            try {
                sendCommand(getNativeMessageBuilder().buildComInitDb(getSharedSendPacket(), database), false, 0);
            } catch (CJException ex) {
                if (this.getPropertySet().getBooleanProperty(PropertyKey.createDatabaseIfNotExist).getValue()) {
                    TelemetrySpan span2 = this.session.getTelemetryHandler().startSpan(TelemetrySpanName.CREATE_DATABASE);
                    try (TelemetryScope scope2 = span2.makeCurrent()) {
                        span2.setAttribute(TelemetryAttribute.DB_NAME, database);
                        span2.setAttribute(TelemetryAttribute.DB_OPERATION, TelemetryAttribute.OPERATION_CREATE);
                        span2.setAttribute(TelemetryAttribute.DB_STATEMENT, TelemetryAttribute.OPERATION_CREATE + TelemetryAttribute.STATEMENT_SUFFIX);
                        span2.setAttribute(TelemetryAttribute.DB_SYSTEM, TelemetryAttribute.DB_SYSTEM_DEFAULT);
                        span2.setAttribute(TelemetryAttribute.DB_USER, this.session.getHostInfo().getUser());
                        span2.setAttribute(TelemetryAttribute.THREAD_ID, Thread.currentThread().getId());
                        span2.setAttribute(TelemetryAttribute.THREAD_NAME, Thread.currentThread().getName());

                        sendCommand(getNativeMessageBuilder().buildComQuery(getSharedSendPacket(), this.session,
                                "CREATE DATABASE IF NOT EXISTS " + StringUtils.quoteIdentifier(database, true)), false, 0);
                    } catch (Throwable t) {
                        span2.setError(t);
                        throw t;
                    } finally {
                        span2.end();
                    }
                    sendCommand(getNativeMessageBuilder().buildComInitDb(getSharedSendPacket(), database), false, 0);
                } else {
                    throw ExceptionFactory.createCommunicationsException(this.getPropertySet(), this.serverSession, this.getPacketSentTimeHolder(),
                            this.getPacketReceivedTimeHolder(), ex, getExceptionInterceptor());
                }
            }
        } catch (Throwable t) {
            span1.setError(t);
            throw t;
        } finally {
            span1.end();
        }
    }

    @Override
    public final NativePacketPayload readMessage(NativePacketPayload reuse) {
        try {
            NativePacketHeader header = this.packetReader.readHeader();
            NativePacketPayload buf = this.packetReader.readMessage(Optional.ofNullable(reuse), header);
            this.packetSequence = header.getMessageSequence();
            return buf;

        } catch (IOException ioEx) {
            throw ExceptionFactory.createCommunicationsException(this.propertySet, this.serverSession, this.getPacketSentTimeHolder(),
                    this.getPacketReceivedTimeHolder(), ioEx, getExceptionInterceptor());
        } catch (OutOfMemoryError oom) {
            throw ExceptionFactory.createException(oom.getMessage(), MysqlErrorNumbers.SQL_STATE_MEMORY_ALLOCATION_ERROR, 0, false, oom,
                    this.exceptionInterceptor);
        }
    }

    public final NativePacketPayload probeMessage(NativePacketPayload reuse) {
        try {
            NativePacketHeader header = this.packetReader.probeHeader();
            NativePacketPayload buf = this.packetReader.probeMessage(Optional.ofNullable(reuse), header);
            this.packetSequence = header.getMessageSequence();
            return buf;

        } catch (IOException ioEx) {
            throw ExceptionFactory.createCommunicationsException(this.propertySet, this.serverSession, this.getPacketSentTimeHolder(),
                    this.getPacketReceivedTimeHolder(), ioEx, getExceptionInterceptor());
        } catch (OutOfMemoryError oom) {
            throw ExceptionFactory.createException(oom.getMessage(), MysqlErrorNumbers.SQL_STATE_MEMORY_ALLOCATION_ERROR, 0, false, oom,
                    this.exceptionInterceptor);
        }
    }

    /**
     * @param packet
     *            {@link Message}
     * @param packetLen
     *            length of header + payload
     */
    @Override
    public final void send(Message packet, int packetLen) {
        try {
            if (this.maxAllowedPacket.getValue() > 0 && packetLen > this.maxAllowedPacket.getValue()) {
                throw new CJPacketTooBigException(packetLen, this.maxAllowedPacket.getValue());
            }

            this.packetSequence++;
            this.packetSender.send(packet.getByteBuffer(), packetLen, this.packetSequence);

            // Don't hold on to large packets
            if (packet == this.sharedSendPacket) {
                reclaimLargeSharedSendPacket();
            }
        } catch (IOException ioEx) {
            throw ExceptionFactory.createCommunicationsException(this.getPropertySet(), this.serverSession, this.getPacketSentTimeHolder(),
                    this.getPacketReceivedTimeHolder(), ioEx, getExceptionInterceptor());
        }
    }

    @Override
    public final NativePacketPayload sendCommand(Message queryPacket, boolean skipCheck, int timeoutMillis) {
        int command = queryPacket.getByteBuffer()[0];
        this.commandCount++;

        if (this.queryInterceptors != null) {
            NativePacketPayload interceptedPacketPayload = (NativePacketPayload) invokeQueryInterceptorsPre(queryPacket, false);

            if (interceptedPacketPayload != null) {
                return interceptedPacketPayload;
            }
        }

        this.packetReader.resetMessageSequence();

        int oldTimeout = 0;

        if (timeoutMillis != 0) {
            try {
                oldTimeout = this.socketConnection.getMysqlSocket().getSoTimeout();
                this.socketConnection.getMysqlSocket().setSoTimeout(timeoutMillis);
            } catch (IOException e) {
                throw ExceptionFactory.createCommunicationsException(this.propertySet, this.serverSession, this.getPacketSentTimeHolder(),
                        this.getPacketReceivedTimeHolder(), e, getExceptionInterceptor());
            }
        }

        try {

            checkForOutstandingStreamingData();

            // Clear serverStatus...this value is guarded by an external mutex, as you can only ever be processing one command at a time
            this.serverSession.setStatusFlags(0, true);
            this.hadWarnings = false;
            this.setWarningCount(0);

            //
            // Compressed input stream needs cleared at beginning of each command execution...
            //
            if (this.useCompression) {
                int bytesLeft = this.socketConnection.getMysqlInput().available();

                if (bytesLeft > 0) {
                    this.socketConnection.getMysqlInput().skip(bytesLeft);
                }
            }

            try {
                clearInputStream();
                this.packetSequence = -1;
                send(queryPacket, queryPacket.getPosition());

            } catch (CJException ex) {
                // don't wrap CJExceptions
                throw ex;
            } catch (Exception ex) {
                throw ExceptionFactory.createCommunicationsException(this.propertySet, this.serverSession, this.getPacketSentTimeHolder(),
                        this.getPacketReceivedTimeHolder(), ex, getExceptionInterceptor());
            }

            NativePacketPayload returnPacket = null;

            if (!skipCheck) {
                if (command == NativeConstants.COM_STMT_EXECUTE || command == NativeConstants.COM_STMT_RESET) {
                    this.packetReader.resetMessageSequence();
                }

                returnPacket = checkErrorMessage(command);

                if (this.queryInterceptors != null) {
                    returnPacket = (NativePacketPayload) invokeQueryInterceptorsPost(queryPacket, returnPacket, false);
                }
            }

            return returnPacket;
        } catch (IOException ioEx) {
            this.serverSession.preserveOldTransactionState();
            throw ExceptionFactory.createCommunicationsException(this.propertySet, this.serverSession, this.getPacketSentTimeHolder(),
                    this.getPacketReceivedTimeHolder(), ioEx, getExceptionInterceptor());
        } catch (CJException e) {
            this.serverSession.preserveOldTransactionState();
            throw e;

        } finally {
            if (timeoutMillis != 0) {
                try {
                    this.socketConnection.getMysqlSocket().setSoTimeout(oldTimeout);
                } catch (IOException e) {
                    throw ExceptionFactory.createCommunicationsException(this.propertySet, this.serverSession, this.getPacketSentTimeHolder(),
                            this.getPacketReceivedTimeHolder(), e, getExceptionInterceptor());
                }
            }
        }
    }

    public void checkTransactionState() {
        int transState = this.serverSession.getTransactionState();
        if (transState == ServerSession.TRANSACTION_COMPLETED) {
            this.transactionManager.transactionCompleted();
        } else if (transState == ServerSession.TRANSACTION_STARTED) {
            this.transactionManager.transactionBegun();
        }
    }

    @Override
    public NativePacketPayload checkErrorMessage() {
        return checkErrorMessage(-1);
    }

    /**
     * Checks for errors in the reply packet, and if none, returns the reply
     * packet, ready for reading
     *
     * @param command
     *            the command being issued (if used)
     * @return NativePacketPayload
     * @throws CJException
     *             if an error packet was received
     * @throws CJCommunicationsException
     *             if a database error occurs
     */
    private NativePacketPayload checkErrorMessage(int command) {
        NativePacketPayload resultPacket = null;
        this.serverSession.setStatusFlags(0);

        try {
            // Check return value, if we get a java.io.EOFException, the server has gone away. We'll pass it on up the exception chain and let someone higher up
            // decide what to do (barf, reconnect, etc).
            resultPacket = readMessage(this.reusablePacket);
        } catch (CJException ex) {
            // Don't wrap CJExceptions
            throw ex;
        } catch (Exception fallThru) {
            throw ExceptionFactory.createCommunicationsException(this.propertySet, this.serverSession, this.getPacketSentTimeHolder(),
                    this.getPacketReceivedTimeHolder(), fallThru, getExceptionInterceptor());
        }

        checkErrorMessage(resultPacket);

        return resultPacket;
    }

    public void checkErrorMessage(NativePacketPayload resultPacket) {
        resultPacket.setPosition(0);
        byte statusCode = (byte) resultPacket.readInteger(IntegerDataType.INT1);

        // Error handling
        if (statusCode == (byte) 0xff) {
            String serverErrorMessage;
            int errno = 2000;

            errno = (int) resultPacket.readInteger(IntegerDataType.INT2);

            String xOpen = null;

            serverErrorMessage = resultPacket.readString(StringSelfDataType.STRING_TERM, this.serverSession.getCharsetSettings().getErrorMessageEncoding());

            if (serverErrorMessage.charAt(0) == '#') {

                // we have an SQLState
                if (serverErrorMessage.length() > 6) {
                    xOpen = serverErrorMessage.substring(1, 6);
                    serverErrorMessage = serverErrorMessage.substring(6);

                    if (xOpen.equals("HY000")) {
                        xOpen = MysqlErrorNumbers.mysqlToSqlState(errno);
                    }
                } else {
                    xOpen = MysqlErrorNumbers.mysqlToSqlState(errno);
                }
            } else {
                xOpen = MysqlErrorNumbers.mysqlToSqlState(errno);
            }

            clearInputStream();

            StringBuilder errorBuf = new StringBuilder();

            String xOpenErrorMessage = MysqlErrorNumbers.get(xOpen);

            boolean useOnlyServerErrorMessages = this.propertySet.getBooleanProperty(PropertyKey.useOnlyServerErrorMessages).getValue();
            if (!useOnlyServerErrorMessages) {
                if (xOpenErrorMessage != null) {
                    errorBuf.append(xOpenErrorMessage);
                    errorBuf.append(Messages.getString("Protocol.0"));
                }
            }

            errorBuf.append(serverErrorMessage);

            if (!useOnlyServerErrorMessages) {
                if (xOpenErrorMessage != null) {
                    errorBuf.append("\"");
                }
            }

            appendDeadlockStatusInformation(this.session, xOpen, errorBuf);

            if (xOpen != null) {
                if (xOpen.startsWith("22")) {
                    throw new DataTruncationException(errorBuf.toString(), 0, true, false, 0, 0, errno);
                }

                if (errno == MysqlErrorNumbers.ER_MUST_CHANGE_PASSWORD) {
                    throw ExceptionFactory.createException(PasswordExpiredException.class, errorBuf.toString(), getExceptionInterceptor());

                } else if (errno == MysqlErrorNumbers.ER_MUST_CHANGE_PASSWORD_LOGIN) {
                    throw ExceptionFactory.createException(ClosedOnExpiredPasswordException.class, errorBuf.toString(), getExceptionInterceptor());

                } else if (errno == MysqlErrorNumbers.ER_CLIENT_INTERACTION_TIMEOUT) {
                    throw ExceptionFactory.createException(CJCommunicationsException.class, errorBuf.toString(), null, getExceptionInterceptor());
                }
            }

            throw ExceptionFactory.createException(errorBuf.toString(), xOpen, errno, false, null, getExceptionInterceptor());

        }
    }

    private void reclaimLargeSharedSendPacket() {
        if (this.sharedSendPacket != null && this.sharedSendPacket.getCapacity() > 1048576) {
            this.sharedSendPacket = new NativePacketPayload(INITIAL_PACKET_SIZE);
        }
    }

    public void clearInputStream() {
        try {
            int len;

            // Due to a bug in some older Linux kernels (fixed after the patch "tcp: fix FIONREAD/SIOCINQ"), our SocketInputStream.available() may return 1 even
            // if there is no data in the Stream, so, we need to check if InputStream.skip() actually skipped anything.
            while ((len = this.socketConnection.getMysqlInput().available()) > 0 && this.socketConnection.getMysqlInput().skip(len) > 0) {
                continue;
            }
        } catch (IOException ioEx) {
            throw ExceptionFactory.createCommunicationsException(this.propertySet, this.serverSession, this.getPacketSentTimeHolder(),
                    this.getPacketReceivedTimeHolder(), ioEx, getExceptionInterceptor());
        }
    }

    /**
     * Don't hold on to overly-large packets
     */
    public void reclaimLargeReusablePacket() {
        if (this.reusablePacket != null && this.reusablePacket.getCapacity() > 1048576) {
            this.reusablePacket = new NativePacketPayload(INITIAL_PACKET_SIZE);
        }
    }

    /**
     * Send a query stored in a packet to the server.
     *
     * @param <T>
     *            extends {@link Resultset}
     * @param callingQuery
     *            {@link Query}
     * @param queryPacket
     *            {@link NativePacketPayload} containing query
     * @param maxRows
     *            rows limit
     * @param streamResults
     *            whether a stream result should be created
     * @param cachedMetadata
     *            use this metadata instead of the one provided on wire
     * @param resultSetFactory
     *            {@link ProtocolEntityFactory}
     * @return T instance
     * @throws IOException
     *             if an i/o error occurs
     */
    public final <T extends Resultset> T sendQueryPacket(Query callingQuery, NativePacketPayload queryPacket, int maxRows, boolean streamResults,
            ColumnDefinition cachedMetadata, ProtocolEntityFactory<T, NativePacketPayload> resultSetFactory) throws IOException {
        final long queryStartTime = getCurrentTimeNanosOrMillis();

        this.statementExecutionDepth++;

        byte[] queryBuf = queryPacket.getByteBuffer();
        int oldPacketPosition = queryPacket.getPosition(); // save the packet position
        int queryPosition = queryPacket.getTag("QUERY");
        LazyString query = new LazyString(queryBuf, queryPosition, oldPacketPosition - queryPosition);

        try {

            if (this.queryInterceptors != null) {
                T interceptedResults = invokeQueryInterceptorsPre(query, callingQuery, false);
                if (interceptedResults != null) {
                    return interceptedResults;
                }
            }

            if (this.autoGenerateTestcaseScript) {
                StringBuilder debugBuf = new StringBuilder(query.length() + 32);
                generateQueryCommentBlock(debugBuf);
                debugBuf.append(query);
                debugBuf.append(';');
                TestUtils.dumpTestcaseQuery(debugBuf.toString());
            }

            // Send query command and sql query string
            NativePacketPayload resultPacket = sendCommand(queryPacket, false,
                    callingQuery.getTimeoutInMillis() <= 0 ? 0 : callingQuery.getTimeoutInMillis());

            final long queryEndTime = getCurrentTimeNanosOrMillis();
            final long queryDuration = queryEndTime - queryStartTime;
            if (callingQuery != null) {
                callingQuery.setExecuteTime(queryDuration);
            }

            boolean queryWasSlow = this.logSlowQueries && (this.useAutoSlowLog ? this.metricsHolder.checkAbonormallyLongQuery(queryDuration)
                    : queryDuration > this.propertySet.getIntegerProperty(PropertyKey.slowQueryThresholdMillis).getValue());

            long fetchBeginTime = this.profileSQL ? getCurrentTimeNanosOrMillis() : 0L;

            T rs = readAllResults(maxRows, streamResults, resultPacket, false, cachedMetadata, resultSetFactory);

            if (this.profileSQL || queryWasSlow) {
                long fetchEndTime = this.profileSQL ? getCurrentTimeNanosOrMillis() : 0L;

                // Extract the actual query from the network packet
                boolean truncated = oldPacketPosition - queryPosition > this.maxQuerySizeToLog.getValue();
                int extractPosition = truncated ? this.maxQuerySizeToLog.getValue() + queryPosition : oldPacketPosition;
                String extractedQuery = StringUtils.toString(queryBuf, queryPosition, extractPosition - queryPosition);
                if (truncated) {
                    extractedQuery += Messages.getString("Protocol.2");
                }

                ProfilerEventHandler eventSink = this.session.getProfilerEventHandler();

                if (this.logSlowQueries) {
                    if (queryWasSlow) {
                        eventSink.processEvent(ProfilerEvent.TYPE_SLOW_QUERY, this.session, callingQuery, rs, queryDuration, new Throwable(),
                                Messages.getString("Protocol.SlowQuery",
                                        new Object[] { this.useAutoSlowLog ? " 95% of all queries " : String.valueOf(this.slowQueryThreshold),
                                                this.queryTimingUnits, Long.valueOf(queryDuration), extractedQuery }));

                        if (this.propertySet.getBooleanProperty(PropertyKey.explainSlowQueries).getValue()) {
                            if (oldPacketPosition - queryPosition < MAX_QUERY_SIZE_TO_EXPLAIN) {
                                queryPacket.setPosition(queryPosition); // skip until the query is located in the packet
                                explainSlowQuery(query.toString(), extractedQuery);
                            } else {
                                this.log.logWarn(Messages.getString("Protocol.3", new Object[] { MAX_QUERY_SIZE_TO_EXPLAIN }));
                            }
                        }
                    }

                    if (this.serverSession.noGoodIndexUsed()) {
                        eventSink.processEvent(ProfilerEvent.TYPE_SLOW_QUERY, this.session, callingQuery, rs, queryDuration, new Throwable(),
                                Messages.getString("Protocol.4") + extractedQuery);
                    }
                    if (this.serverSession.noIndexUsed()) {
                        eventSink.processEvent(ProfilerEvent.TYPE_SLOW_QUERY, this.session, callingQuery, rs, queryDuration, new Throwable(),
                                Messages.getString("Protocol.5") + extractedQuery);
                    }
                    if (this.serverSession.queryWasSlow()) {
                        eventSink.processEvent(ProfilerEvent.TYPE_SLOW_QUERY, this.session, callingQuery, rs, queryDuration, new Throwable(),
                                Messages.getString("Protocol.ServerSlowQuery") + extractedQuery);
                    }
                }

                if (this.profileSQL) {
                    eventSink.processEvent(ProfilerEvent.TYPE_QUERY, this.session, callingQuery, rs, queryDuration, new Throwable(), extractedQuery);
                    eventSink.processEvent(ProfilerEvent.TYPE_FETCH, this.session, callingQuery, rs, fetchEndTime - fetchBeginTime, new Throwable(), null);
                }
            }

            if (this.hadWarnings) {
                scanForAndThrowDataTruncation();
            }

            if (this.queryInterceptors != null) {
                rs = invokeQueryInterceptorsPost(query, callingQuery, rs, false);
            }

            return rs;

        } catch (CJException sqlEx) {
            if (this.queryInterceptors != null) {
                // TODO why doing this?
                invokeQueryInterceptorsPost(query, callingQuery, null, false); // we don't do anything with the result set in this case
            }

            if (callingQuery != null) {
                callingQuery.checkCancelTimeout();
            }

            throw sqlEx;

        } finally {
            this.statementExecutionDepth--;
        }
    }

    public <T extends Resultset> T invokeQueryInterceptorsPre(Supplier<String> sql, Query interceptedQuery, boolean forceExecute) {
        T previousResultSet = null;

        for (int i = 0, s = this.queryInterceptors.size(); i < s; i++) {
            QueryInterceptor interceptor = this.queryInterceptors.get(i);

            boolean executeTopLevelOnly = interceptor.executeTopLevelOnly();
            boolean shouldExecute = executeTopLevelOnly && (this.statementExecutionDepth == 1 || forceExecute) || !executeTopLevelOnly;

            if (shouldExecute) {
                T interceptedResultSet = interceptor.preProcess(sql, interceptedQuery);

                if (interceptedResultSet != null) {
                    previousResultSet = interceptedResultSet;
                }
            }
        }

        return previousResultSet;
    }

    /**
     *
     * @param <M>
     *            extends {@link Message}
     * @param queryPacket
     *            {@link NativePacketPayload} containing query
     * @param forceExecute
     *            currently ignored
     * @return M instance
     */
    public <M extends Message> M invokeQueryInterceptorsPre(M queryPacket, boolean forceExecute) {
        M previousPacketPayload = null;

        for (int i = 0, s = this.queryInterceptors.size(); i < s; i++) {
            QueryInterceptor interceptor = this.queryInterceptors.get(i);

            // TODO how to handle executeTopLevelOnly in such case ?
            //            boolean executeTopLevelOnly = interceptor.executeTopLevelOnly();
            //            boolean shouldExecute = (executeTopLevelOnly && (this.statementExecutionDepth == 1 || forceExecute)) || (!executeTopLevelOnly);
            //            if (shouldExecute) {

            M interceptedPacketPayload = interceptor.preProcess(queryPacket);
            if (interceptedPacketPayload != null) {
                previousPacketPayload = interceptedPacketPayload;
            }
            //            }
        }

        return previousPacketPayload;
    }

    public <T extends Resultset> T invokeQueryInterceptorsPost(Supplier<String> sql, Query interceptedQuery, T originalResultSet, boolean forceExecute) {
        for (int i = 0, s = this.queryInterceptors.size(); i < s; i++) {
            QueryInterceptor interceptor = this.queryInterceptors.get(i);

            boolean executeTopLevelOnly = interceptor.executeTopLevelOnly();
            boolean shouldExecute = executeTopLevelOnly && (this.statementExecutionDepth == 1 || forceExecute) || !executeTopLevelOnly;

            if (shouldExecute) {
                T interceptedResultSet = interceptor.postProcess(sql, interceptedQuery, originalResultSet, this.serverSession);

                if (interceptedResultSet != null) {
                    originalResultSet = interceptedResultSet;
                }
            }
        }

        return originalResultSet;
    }

    /**
     *
     * @param <M>
     *            extends {@link Message}
     * @param queryPacket
     *            {@link NativePacketPayload} containing query
     * @param originalResponsePacket
     *            {@link NativePacketPayload} containing response
     * @param forceExecute
     *            currently ignored
     * @return T instance
     */
    public <M extends Message> M invokeQueryInterceptorsPost(M queryPacket, M originalResponsePacket, boolean forceExecute) {
        for (int i = 0, s = this.queryInterceptors.size(); i < s; i++) {
            QueryInterceptor interceptor = this.queryInterceptors.get(i);

            // TODO how to handle executeTopLevelOnly in such case ?
            //            boolean executeTopLevelOnly = interceptor.executeTopLevelOnly();
            //            boolean shouldExecute = (executeTopLevelOnly && (this.statementExecutionDepth == 1 || forceExecute)) || (!executeTopLevelOnly);
            //            if (shouldExecute) {

            M interceptedPacketPayload = interceptor.postProcess(queryPacket, originalResponsePacket);
            if (interceptedPacketPayload != null) {
                originalResponsePacket = interceptedPacketPayload;
            }
            //            }
        }

        return originalResponsePacket;
    }

    public long getCurrentTimeNanosOrMillis() {
        return this.useNanosForElapsedTime ? TimeUtil.getCurrentTimeNanosOrMillis() : System.currentTimeMillis();
    }

    public boolean hadWarnings() {
        return this.hadWarnings;
    }

    public void setHadWarnings(boolean hadWarnings) {
        this.hadWarnings = hadWarnings;
    }

    /**
     * Runs an 'EXPLAIN' on the given query and dumps the results to the log
     *
     * @param query
     *            full query string
     * @param truncatedQuery
     *            query string truncated for profiling
     *
     */
    public void explainSlowQuery(String query, String truncatedQuery) {
        if (StringUtils.startsWithIgnoreCaseAndWs(truncatedQuery, EXPLAINABLE_STATEMENT)
                || versionMeetsMinimum(5, 6, 3) && StringUtils.startsWithIgnoreCaseAndWs(truncatedQuery, EXPLAINABLE_STATEMENT_EXTENSION) != -1) {

            TelemetrySpan span = this.session.getTelemetryHandler().startSpan(TelemetrySpanName.EXPLAIN_QUERY);
            try (TelemetryScope scope = span.makeCurrent()) {
                span.setAttribute(TelemetryAttribute.DB_NAME, this.session.getHostInfo().getDatabase());
                span.setAttribute(TelemetryAttribute.DB_OPERATION, TelemetryAttribute.OPERATION_EXPLAIN);
                span.setAttribute(TelemetryAttribute.DB_STATEMENT, TelemetryAttribute.OPERATION_EXPLAIN + TelemetryAttribute.STATEMENT_SUFFIX);
                span.setAttribute(TelemetryAttribute.DB_SYSTEM, TelemetryAttribute.DB_SYSTEM_DEFAULT);
                span.setAttribute(TelemetryAttribute.DB_USER, this.session.getHostInfo().getUser());
                span.setAttribute(TelemetryAttribute.THREAD_ID, Thread.currentThread().getId());
                span.setAttribute(TelemetryAttribute.THREAD_NAME, Thread.currentThread().getName());
                try {
                    NativePacketPayload resultPacket = sendCommand(
                            getNativeMessageBuilder().buildComQuery(getSharedSendPacket(), this.session, "EXPLAIN " + query), false, 0);

                    Resultset rs = readAllResults(-1, false, resultPacket, false, null, new ResultsetFactory(Type.FORWARD_ONLY, null));

                    StringBuilder explainResults = new StringBuilder(Messages.getString("Protocol.6"));
                    explainResults.append(truncatedQuery);
                    explainResults.append(Messages.getString("Protocol.7"));

                    appendResultSetSlashGStyle(explainResults, rs);

                    this.log.logWarn(explainResults.toString());
                } catch (CJException sqlEx) {
                    throw sqlEx;

                } catch (Exception ex) {
                    throw ExceptionFactory.createException(ex.getMessage(), ex, getExceptionInterceptor());
                }
            } catch (Throwable t) {
                span.setError(t);
                throw t;
            } finally {
                span.end();
            }
        }
    }

    /**
     * Reads and discards a single MySQL packet.
     *
     * @throws CJException
     *             if the network fails while skipping the
     *             packet.
     */
    public final void skipPacket() {
        try {
            this.packetReader.skipPacket();
        } catch (IOException ioEx) {
            throw ExceptionFactory.createCommunicationsException(this.propertySet, this.serverSession, this.getPacketSentTimeHolder(),
                    this.getPacketReceivedTimeHolder(), ioEx, getExceptionInterceptor());
        }
    }

    /**
     * Log-off of the MySQL server and close the socket.
     *
     */
    public final void quit() {
        try {
            try {
                if (!ExportControlled.isSSLEstablished(this.socketConnection.getMysqlSocket())) { // Fix for Bug#56979 does not apply to secure sockets.
                    if (!this.socketConnection.getMysqlSocket().isClosed()) {
                        try {
                            // The response won't be read, this fixes BUG#56979 [Improper connection closing logic leads to TIME_WAIT sockets on server].
                            this.socketConnection.getMysqlSocket().shutdownInput();
                        } catch (UnsupportedOperationException e) {
                            // Ignore, some sockets do not support this method.
                        }
                    }
                }
            } catch (IOException e) {
                // Can't do anything constructive about this.
            }

            this.packetSequence = -1;
            NativePacketPayload packet = new NativePacketPayload(1);
            send(getNativeMessageBuilder().buildComQuit(packet), packet.getPosition());
        } finally {
            this.socketConnection.forceClose();
            this.localInfileInputStream = null;
        }
    }

    /**
     * Returns the packet used for sending data (used by PreparedStatement) with position set to 0.
     * Guarded by external synchronization on a mutex.
     *
     * @return A packet to send data with
     */
    public NativePacketPayload getSharedSendPacket() {
        if (this.sharedSendPacket == null) {
            this.sharedSendPacket = new NativePacketPayload(INITIAL_PACKET_SIZE);
        }
        this.sharedSendPacket.setPosition(0);

        return this.sharedSendPacket;
    }

    private void calculateSlowQueryThreshold() {
        this.slowQueryThreshold = this.propertySet.getIntegerProperty(PropertyKey.slowQueryThresholdMillis).getValue();

        if (this.propertySet.getBooleanProperty(PropertyKey.useNanosForElapsedTime).getValue()) {
            long nanosThreshold = this.propertySet.getLongProperty(PropertyKey.slowQueryThresholdNanos).getValue();

            if (nanosThreshold != 0) {
                this.slowQueryThreshold = nanosThreshold;
            } else {
                this.slowQueryThreshold *= 1000000; // 1 million millis in a nano
            }
        }
    }

    /**
     * Re-authenticates as the given user and password
     *
     * @param user
     *            user name
     * @param password
     *            password
     * @param database
     *            database name
     *
     */
    @Override
    public void changeUser(String user, String password, String database) {
        this.packetSequence = -1;
        this.packetSender = this.packetSender.undecorateAll();
        this.packetReader = this.packetReader.undecorateAll();

        this.authProvider.changeUser(user, password, database);
    }

    protected boolean useNanosForElapsedTime() {
        return this.useNanosForElapsedTime;
    }

    public long getSlowQueryThreshold() {
        return this.slowQueryThreshold;
    }

    public int getCommandCount() {
        return this.commandCount;
    }

    public void setQueryInterceptors(List<QueryInterceptor> queryInterceptors) {
        this.queryInterceptors = queryInterceptors.isEmpty() ? null : queryInterceptors;
    }

    public List<QueryInterceptor> getQueryInterceptors() {
        return this.queryInterceptors;
    }

    public void setSocketTimeout(int milliseconds) {
        try {
            Socket soc = this.socketConnection.getMysqlSocket();
            if (soc != null) {
                soc.setSoTimeout(milliseconds);
            }
        } catch (IOException e) {
            throw ExceptionFactory.createException(WrongArgumentException.class, Messages.getString("Protocol.8"), e, getExceptionInterceptor());
        }
    }

    public void releaseResources() {
        if (this.compressedPacketSender != null) {
            this.compressedPacketSender.stop();
        }
    }

    @Override
    public void connect(String user, String password, String database) {
        // session creation & initialization happens here

        beforeHandshake();

        this.authProvider.connect(user, password, database);
    }

    protected boolean isDataAvailable() {
        try {
            return this.socketConnection.getMysqlInput().available() > 0;
        } catch (IOException ioEx) {
            throw ExceptionFactory.createCommunicationsException(this.propertySet, this.serverSession, this.getPacketSentTimeHolder(),
                    this.getPacketReceivedTimeHolder(), ioEx, getExceptionInterceptor());
        }
    }

    public NativePacketPayload getReusablePacket() {
        return this.reusablePacket;
    }

    public int getWarningCount() {
        return this.warningCount;
    }

    public void setWarningCount(int warningCount) {
        this.warningCount = warningCount;
    }

    public void dumpPacketRingBuffer() {
        // use local variable to allow unsynchronized usage of the buffer
        LinkedList<StringBuilder> localPacketDebugRingBuffer = this.packetDebugRingBuffer;
        if (localPacketDebugRingBuffer != null) {
            StringBuilder dumpBuffer = new StringBuilder();

            dumpBuffer.append("Last " + localPacketDebugRingBuffer.size() + " packets received from server, from oldest->newest:\n");
            dumpBuffer.append("\n");

            for (Iterator<StringBuilder> ringBufIter = localPacketDebugRingBuffer.iterator(); ringBufIter.hasNext();) {
                dumpBuffer.append(ringBufIter.next());
                dumpBuffer.append("\n");
            }

            this.log.logTrace(dumpBuffer.toString());
        }
    }

    @Override
    public boolean versionMeetsMinimum(int major, int minor, int subminor) {
        return this.serverSession.getServerVersion().meetsMinimum(new ServerVersion(major, minor, subminor));
    }

    public static MysqlType findMysqlType(PropertySet propertySet, int mysqlTypeId, short colFlag, long length, LazyString tableName,
            LazyString originalTableName, int collationIndex, String encoding) {
        boolean isUnsigned = (colFlag & MysqlType.FIELD_FLAG_UNSIGNED) > 0;
        boolean isFromFunction = originalTableName.length() == 0;
        boolean isBinary = (colFlag & MysqlType.FIELD_FLAG_BINARY) > 0;
        /**
         * Is this field owned by a server-created temporary table?
         */
        boolean isImplicitTemporaryTable = tableName.length() > 0 && tableName.toString().startsWith("#sql_");

        boolean isOpaqueBinary = isBinary && collationIndex == CharsetMapping.MYSQL_COLLATION_INDEX_binary
                && (mysqlTypeId == MysqlType.FIELD_TYPE_STRING || mysqlTypeId == MysqlType.FIELD_TYPE_VAR_STRING || mysqlTypeId == MysqlType.FIELD_TYPE_VARCHAR)
                        ?
                        // queries resolved by temp tables also have this 'signature', check for that
                        !isImplicitTemporaryTable
                        : "binary".equalsIgnoreCase(encoding);

        switch (mysqlTypeId) {
            case MysqlType.FIELD_TYPE_DECIMAL:
            case MysqlType.FIELD_TYPE_NEWDECIMAL:
                return isUnsigned ? MysqlType.DECIMAL_UNSIGNED : MysqlType.DECIMAL;

            case MysqlType.FIELD_TYPE_TINY:
                // Adjust for pseudo-boolean
                if (!isUnsigned && length == 1 && propertySet.getBooleanProperty(PropertyKey.tinyInt1isBit).getValue()) {
                    if (propertySet.getBooleanProperty(PropertyKey.transformedBitIsBoolean).getValue()) {
                        return MysqlType.BOOLEAN;
                    }
                    return MysqlType.BIT;
                }
                return isUnsigned ? MysqlType.TINYINT_UNSIGNED : MysqlType.TINYINT;

            case MysqlType.FIELD_TYPE_SHORT:
                return isUnsigned ? MysqlType.SMALLINT_UNSIGNED : MysqlType.SMALLINT;

            case MysqlType.FIELD_TYPE_LONG:
                return isUnsigned ? MysqlType.INT_UNSIGNED : MysqlType.INT;

            case MysqlType.FIELD_TYPE_FLOAT:
                return isUnsigned ? MysqlType.FLOAT_UNSIGNED : MysqlType.FLOAT;

            case MysqlType.FIELD_TYPE_DOUBLE:
                return isUnsigned ? MysqlType.DOUBLE_UNSIGNED : MysqlType.DOUBLE;

            case MysqlType.FIELD_TYPE_NULL:
                return MysqlType.NULL;

            case MysqlType.FIELD_TYPE_TIMESTAMP:
                return MysqlType.TIMESTAMP;

            case MysqlType.FIELD_TYPE_LONGLONG:
                return isUnsigned ? MysqlType.BIGINT_UNSIGNED : MysqlType.BIGINT;

            case MysqlType.FIELD_TYPE_INT24:
                return isUnsigned ? MysqlType.MEDIUMINT_UNSIGNED : MysqlType.MEDIUMINT;

            case MysqlType.FIELD_TYPE_DATE:
                return MysqlType.DATE;

            case MysqlType.FIELD_TYPE_TIME:
                return MysqlType.TIME;

            case MysqlType.FIELD_TYPE_DATETIME:
                return MysqlType.DATETIME;

            case MysqlType.FIELD_TYPE_YEAR:
                return MysqlType.YEAR;

            case MysqlType.FIELD_TYPE_VARCHAR:
            case MysqlType.FIELD_TYPE_VAR_STRING:

                if (isOpaqueBinary && !(isFromFunction && propertySet.getBooleanProperty(PropertyKey.functionsNeverReturnBlobs).getValue())) {
                    return MysqlType.VARBINARY;
                }

                return MysqlType.VARCHAR;

            case MysqlType.FIELD_TYPE_BIT:
                //if (length > 1) {
                // we need to pretend this is a full binary blob
                //this.colFlag |= MysqlType.FIELD_FLAG_BINARY;
                //this.colFlag |= MysqlType.FIELD_FLAG_BLOB;
                //return MysqlType.VARBINARY;
                //}
                return MysqlType.BIT;

            case MysqlType.FIELD_TYPE_JSON:
                return MysqlType.JSON;

            case MysqlType.FIELD_TYPE_ENUM:
                return MysqlType.ENUM;

            case MysqlType.FIELD_TYPE_SET:
                return MysqlType.SET;

            case MysqlType.FIELD_TYPE_TINY_BLOB:
                if (!isBinary || collationIndex != CharsetMapping.MYSQL_COLLATION_INDEX_binary
                        || propertySet.getBooleanProperty(PropertyKey.blobsAreStrings).getValue()
                        || isFromFunction && propertySet.getBooleanProperty(PropertyKey.functionsNeverReturnBlobs).getValue()) {
                    return MysqlType.TINYTEXT;
                }
                return MysqlType.TINYBLOB;

            case MysqlType.FIELD_TYPE_MEDIUM_BLOB:
                if (!isBinary || collationIndex != CharsetMapping.MYSQL_COLLATION_INDEX_binary
                        || propertySet.getBooleanProperty(PropertyKey.blobsAreStrings).getValue()
                        || isFromFunction && propertySet.getBooleanProperty(PropertyKey.functionsNeverReturnBlobs).getValue()) {
                    return MysqlType.MEDIUMTEXT;
                }
                return MysqlType.MEDIUMBLOB;

            case MysqlType.FIELD_TYPE_LONG_BLOB:
                if (!isBinary || collationIndex != CharsetMapping.MYSQL_COLLATION_INDEX_binary
                        || propertySet.getBooleanProperty(PropertyKey.blobsAreStrings).getValue()
                        || isFromFunction && propertySet.getBooleanProperty(PropertyKey.functionsNeverReturnBlobs).getValue()) {
                    return MysqlType.LONGTEXT;
                }
                return MysqlType.LONGBLOB;

            case MysqlType.FIELD_TYPE_BLOB:
                // Sometimes MySQL uses this protocol-level type for all possible BLOB variants,
                // we can divine what the actual type is by the length reported

                int newMysqlTypeId = mysqlTypeId;

                // fixing initial type according to length
                if (length <= MysqlType.TINYBLOB.getPrecision()) {
                    newMysqlTypeId = MysqlType.FIELD_TYPE_TINY_BLOB;

                } else if (length <= MysqlType.BLOB.getPrecision()) {
                    if (!isBinary || collationIndex != CharsetMapping.MYSQL_COLLATION_INDEX_binary
                            || propertySet.getBooleanProperty(PropertyKey.blobsAreStrings).getValue()
                            || isFromFunction && propertySet.getBooleanProperty(PropertyKey.functionsNeverReturnBlobs).getValue()) {
                        newMysqlTypeId = MysqlType.FIELD_TYPE_VARCHAR;
                        return MysqlType.TEXT;
                    }
                    return MysqlType.BLOB;

                } else if (length <= MysqlType.MEDIUMBLOB.getPrecision()) {
                    newMysqlTypeId = MysqlType.FIELD_TYPE_MEDIUM_BLOB;
                } else {
                    newMysqlTypeId = MysqlType.FIELD_TYPE_LONG_BLOB;
                }

                // call this method again with correct this.mysqlType set
                return findMysqlType(propertySet, newMysqlTypeId, colFlag, length, tableName, originalTableName, collationIndex, encoding);

            case MysqlType.FIELD_TYPE_STRING:
                if (isOpaqueBinary && !propertySet.getBooleanProperty(PropertyKey.blobsAreStrings).getValue()) {
                    return MysqlType.BINARY;
                }
                return MysqlType.CHAR;

            case MysqlType.FIELD_TYPE_GEOMETRY:
                return MysqlType.GEOMETRY;

            case MysqlType.FIELD_TYPE_VECTOR:
                return MysqlType.VECTOR;

            default:
                return MysqlType.UNKNOWN;
        }
    }

    /*
     * Reading results
     */

    @Override
    public <T extends ProtocolEntity> T read(Class<T> requiredClass, ProtocolEntityFactory<T, NativePacketPayload> protocolEntityFactory) throws IOException {
        @SuppressWarnings("unchecked")
        ProtocolEntityReader<T, NativePacketPayload> sr = (ProtocolEntityReader<T, NativePacketPayload>) this.PROTOCOL_ENTITY_CLASS_TO_TEXT_READER
                .get(requiredClass);
        if (sr == null) {
            throw ExceptionFactory.createException(FeatureNotAvailableException.class, "ProtocolEntityReader isn't available for class " + requiredClass);
        }
        return sr.read(protocolEntityFactory);
    }

    @Override
    public <T extends ProtocolEntity> T read(Class<Resultset> requiredClass, int maxRows, boolean streamResults, NativePacketPayload resultPacket,
            boolean isBinaryEncoded, ColumnDefinition metadata, ProtocolEntityFactory<T, NativePacketPayload> protocolEntityFactory) throws IOException {
        @SuppressWarnings("unchecked")
        ProtocolEntityReader<T, NativePacketPayload> sr = isBinaryEncoded
                ? (ProtocolEntityReader<T, NativePacketPayload>) this.PROTOCOL_ENTITY_CLASS_TO_BINARY_READER.get(requiredClass)
                : (ProtocolEntityReader<T, NativePacketPayload>) this.PROTOCOL_ENTITY_CLASS_TO_TEXT_READER.get(requiredClass);
        if (sr == null) {
            throw ExceptionFactory.createException(FeatureNotAvailableException.class, "ProtocolEntityReader isn't available for class " + requiredClass);
        }
        return sr.read(maxRows, streamResults, resultPacket, metadata, protocolEntityFactory);
    }

    /**
     * Read next result set from multi-result chain.
     *
     * @param <T>
     *            extends {@link ProtocolEntity}
     * @param currentProtocolEntity
     *            T instance
     * @param maxRows
     *            rows limit
     * @param streamResults
     *            whether a stream result should be created
     * @param isBinaryEncoded
     *            true for binary protocol
     * @param resultSetFactory
     *            {@link ProtocolEntityFactory}
     * @return T instance
     * @throws IOException
     *             if an i/o error occurs
     */
    public <T extends ProtocolEntity> T readNextResultset(T currentProtocolEntity, int maxRows, boolean streamResults, boolean isBinaryEncoded,
            ProtocolEntityFactory<T, NativePacketPayload> resultSetFactory) throws IOException {
        T result = null;
        if (Resultset.class.isAssignableFrom(currentProtocolEntity.getClass()) && this.serverSession.useMultiResults()) {
            if (this.serverSession.hasMoreResults()) {

                T currentResultSet = currentProtocolEntity;
                T newResultSet;
                do {
                    NativePacketPayload fieldPacket = checkErrorMessage();
                    fieldPacket.setPosition(0);
                    newResultSet = read(Resultset.class, maxRows, streamResults, fieldPacket, isBinaryEncoded, null, resultSetFactory);
                    ((Resultset) currentResultSet).setNextResultset((Resultset) newResultSet);
                    currentResultSet = newResultSet;

                    if (result == null) {
                        // we should return the first result set in chain
                        result = currentResultSet;
                    }
                } while (streamResults && this.serverSession.hasMoreResults() // we need to consume all result sets which don't contain rows from streamer right now,
                        && !((Resultset) currentResultSet).hasRows()); // because next data portion from streamer is available only via ResultsetRows.next()

            }
        }
        return result;
    }

    public <T extends Resultset> T readAllResults(int maxRows, boolean streamResults, NativePacketPayload resultPacket, boolean isBinaryEncoded,
            ColumnDefinition metadata, ProtocolEntityFactory<T, NativePacketPayload> resultSetFactory) throws IOException {
        resultPacket.setPosition(0);
        T topLevelResultSet = read(Resultset.class, maxRows, streamResults, resultPacket, isBinaryEncoded, metadata, resultSetFactory);

        if (this.serverSession.hasMoreResults()) {
            T currentResultSet = topLevelResultSet;
            if (streamResults) {
                currentResultSet = readNextResultset(currentResultSet, maxRows, true, isBinaryEncoded, resultSetFactory);
            } else {
                while (this.serverSession.hasMoreResults()) {
                    currentResultSet = readNextResultset(currentResultSet, maxRows, false, isBinaryEncoded, resultSetFactory);
                }
                clearInputStream();
            }
        }

        if (this.hadWarnings) {
            scanForAndThrowDataTruncation();
        }

        reclaimLargeReusablePacket();
        return topLevelResultSet;
    }

    @SuppressWarnings("unchecked")
    public final <T> T readServerStatusForResultSets(NativePacketPayload rowPacket, boolean saveOldStatus) {
        T result = null;
        if (rowPacket.isEOFPacket()) {
            // read EOF packet
            rowPacket.setPosition(1); // skip the packet signature header
            this.warningCount = (int) rowPacket.readInteger(IntegerDataType.INT2);
            if (this.warningCount > 0) {
                this.hadWarnings = true; // this is a 'latch', it's reset by sendCommand()
            }

            this.serverSession.setStatusFlags((int) rowPacket.readInteger(IntegerDataType.INT2), saveOldStatus);
            checkTransactionState();
        } else {
            // read OK packet
            OkPacket ok = OkPacket.parse(rowPacket, this.serverSession);
            result = (T) ok;

            this.serverSession.setStatusFlags(ok.getStatusFlags(), saveOldStatus);
            this.serverSession.getServerSessionStateController().setSessionStateChanges(ok.getSessionStateChanges());
            checkTransactionState();

            this.warningCount = ok.getWarningCount();
            if (this.warningCount > 0) {
                this.hadWarnings = true; // this is a 'latch', it's reset by sendCommand()
            }
        }
        return result;
    }

    @Override
    public <T extends QueryResult> T readQueryResult(ResultBuilder<T> resultBuilder) {
        throw ExceptionFactory.createException(CJOperationNotSupportedException.class, "Not supported");
    }

    @Override
    public InputStream getLocalInfileInputStream() {
        return this.localInfileInputStream;
    }

    @Override
    public void setLocalInfileInputStream(InputStream stream) {
        this.localInfileInputStream = stream;
    }

    /**
     * Reads and sends a file to the server for LOAD DATA LOCAL INFILE
     *
     * @param fileName
     *            the file name to send.
     * @return NativePacketPayload
     */
    public final NativePacketPayload sendFileToServer(String fileName) {
        NativePacketPayload filePacket = this.loadFileBufRef == null ? null : this.loadFileBufRef.get();

        int bigPacketLength = Math.min(this.maxAllowedPacket.getValue() - NativeConstants.HEADER_LENGTH * 3,
                alignPacketSize(this.maxAllowedPacket.getValue() - 16, 4096) - NativeConstants.HEADER_LENGTH * 3);
        int oneMeg = 1024 * 1024;
        int smallerPacketSizeAligned = Math.min(oneMeg - NativeConstants.HEADER_LENGTH * 3,
                alignPacketSize(oneMeg - 16, 4096) - NativeConstants.HEADER_LENGTH * 3);
        int packetLength = Math.min(smallerPacketSizeAligned, bigPacketLength);

        if (filePacket == null) {
            try {
                filePacket = new NativePacketPayload(packetLength);
                this.loadFileBufRef = new SoftReference<>(filePacket);
            } catch (OutOfMemoryError oom) {
                throw ExceptionFactory.createException(Messages.getString("MysqlIO.111", new Object[] { packetLength }),
                        MysqlErrorNumbers.SQL_STATE_MEMORY_ALLOCATION_ERROR, 0, false, oom, this.exceptionInterceptor);
            }
        }

        filePacket.setPosition(0);

        byte[] fileBuf = new byte[packetLength];
        BufferedInputStream fileIn = null;
        try {
            fileIn = getFileStream(fileName);

            int bytesRead = 0;
            while ((bytesRead = fileIn.read(fileBuf)) != -1) {
                filePacket.setPosition(0);
                filePacket.writeBytes(StringLengthDataType.STRING_FIXED, fileBuf, 0, bytesRead);
                send(filePacket, filePacket.getPosition());
            }
        } catch (IOException ioEx) {
            boolean isParanoid = this.propertySet.getBooleanProperty(PropertyKey.paranoid).getValue();

            StringBuilder messageBuf = new StringBuilder(Messages.getString("MysqlIO.62"));
            if (fileName != null && !isParanoid) {
                messageBuf.append("'");
                messageBuf.append(fileName);
                messageBuf.append("'");
            }
            messageBuf.append(Messages.getString("MysqlIO.63"));
            if (!isParanoid) {
                messageBuf.append(Messages.getString("MysqlIO.64"));
                messageBuf.append(Util.stackTraceToString(ioEx));
            }

            throw ExceptionFactory.createException(messageBuf.toString(), ioEx, this.exceptionInterceptor);
        } finally {
            if (fileIn != null) {
                try {
                    fileIn.close();
                } catch (Exception ex) {
                    throw ExceptionFactory.createException(Messages.getString("MysqlIO.65"), ex, this.exceptionInterceptor);
                }

                fileIn = null;
            } else {
                // File open failed, but server needs one packet.
                filePacket.setPosition(0);
                send(filePacket, filePacket.getPosition());
                checkErrorMessage(); // To clear response off of queue.
            }
        }

        // send empty packet to mark EOF
        filePacket.setPosition(0);
        send(filePacket, filePacket.getPosition());

        return checkErrorMessage();
    }

    private BufferedInputStream getFileStream(String fileName) throws IOException {
        RuntimeProperty<Boolean> allowLoadLocalInfile = this.propertySet.getBooleanProperty(PropertyKey.allowLoadLocalInfile);
        RuntimeProperty<String> allowLoadLocaInfileInPath = this.propertySet.getStringProperty(PropertyKey.allowLoadLocalInfileInPath);
        RuntimeProperty<Boolean> allowUrlInLocalInfile = this.propertySet.getBooleanProperty(PropertyKey.allowUrlInLocalInfile);

        if (!allowLoadLocalInfile.getValue() && !allowLoadLocaInfileInPath.isExplicitlySet()) {
            throw ExceptionFactory.createException(Messages.getString("MysqlIO.LoadDataLocalNotAllowed"), this.exceptionInterceptor);
        }

        if (allowLoadLocalInfile.getValue()) {
            // "LOAD DATA LOCAL INFILE" is enabled without restrictions.
            InputStream hookedStream = getLocalInfileInputStream();
            if (hookedStream != null) {
                return new BufferedInputStream(hookedStream);
            } else if (allowUrlInLocalInfile.getValue()) {
                // Look for ':'.
                if (fileName.indexOf(':') != -1) {
                    try {
                        URL urlFromFileName = new URL(fileName);
                        return new BufferedInputStream(urlFromFileName.openStream());
                    } catch (MalformedURLException e) {
                        // Ignore and fall back to trying this as a file input stream.
                    }
                }
            }
            return new BufferedInputStream(new FileInputStream(new File(fileName).getCanonicalFile()));
        }

        // Given the code paths above, allowLoadLocaInfileInPath.isExplicitlySet() must be true and restrictions to "LOAD DATA LOCAL INFILE" apply.
        String safePathValue = allowLoadLocaInfileInPath.getValue();
        Path safePath;
        if (safePathValue.length() == 0) {
            throw ExceptionFactory.createException(
                    Messages.getString("MysqlIO.60", new Object[] { safePathValue, PropertyKey.allowLoadLocalInfileInPath.getKeyName() }),
                    this.exceptionInterceptor);
        }
        try {
            safePath = Paths.get(safePathValue).toRealPath();
        } catch (IOException | InvalidPathException e) {
            throw ExceptionFactory.createException(
                    Messages.getString("MysqlIO.60", new Object[] { safePathValue, PropertyKey.allowLoadLocalInfileInPath.getKeyName() }), e,
                    this.exceptionInterceptor);
        }

        if (allowUrlInLocalInfile.getValue()) {
            try {
                URL urlFromFileName = new URL(fileName);

                if (!urlFromFileName.getProtocol().equalsIgnoreCase("file")) {
                    throw ExceptionFactory.createException(Messages.getString("MysqlIO.66", new Object[] { urlFromFileName.getProtocol() }),
                            this.exceptionInterceptor);
                }

                try {
                    InetAddress addr = InetAddress.getByName(urlFromFileName.getHost());
                    if (!addr.isLoopbackAddress()) {
                        throw ExceptionFactory.createException(Messages.getString("MysqlIO.67", new Object[] { urlFromFileName.getHost() }),
                                this.exceptionInterceptor);
                    }
                } catch (UnknownHostException e) {
                    throw ExceptionFactory.createException(Messages.getString("MysqlIO.68", new Object[] { fileName }), e, this.exceptionInterceptor);
                }

                Path filePath = null;
                try {
                    filePath = Paths.get(urlFromFileName.toURI()).toRealPath();
                } catch (InvalidPathException e) {
                    // Windows paths often can't be extracted, but the URL is still valid.
                    String pathString = urlFromFileName.getPath();
                    if (pathString.indexOf(':') != -1 && (pathString.startsWith("/") || pathString.startsWith("\\"))) {
                        pathString = pathString.replaceFirst("^[/\\\\]*", "");
                    }
                    filePath = Paths.get(pathString).toRealPath();
                } catch (IllegalArgumentException e) {
                    // Try the path directly.
                    filePath = Paths.get(urlFromFileName.getPath()).toRealPath();
                }
                if (!filePath.startsWith(safePath)) {
                    throw ExceptionFactory.createException(Messages.getString("MysqlIO.61", new Object[] { filePath, safePath }), this.exceptionInterceptor);
                }

                return new BufferedInputStream(urlFromFileName.openStream());
            } catch (MalformedURLException | URISyntaxException e) {
                // Fall back to trying this as a file input stream.
            }
        }

        Path filePath = Paths.get(fileName).toRealPath();
        if (!filePath.startsWith(safePath)) {
            throw ExceptionFactory.createException(Messages.getString("MysqlIO.61", new Object[] { filePath, safePath }), this.exceptionInterceptor);
        }
        return new BufferedInputStream(new FileInputStream(filePath.toFile()));
    }

    private int alignPacketSize(int a, int l) {
        return a + l - 1 & ~(l - 1);
    }

    private ResultsetRows streamingData = null;

    public ResultsetRows getStreamingData() {
        return this.streamingData;
    }

    public void setStreamingData(ResultsetRows streamingData) {
        this.streamingData = streamingData;
    }

    public void checkForOutstandingStreamingData() {
        if (this.streamingData != null) {
            boolean shouldClobber = this.propertySet.getBooleanProperty(PropertyKey.clobberStreamingResults).getValue();

            if (!shouldClobber) {
                throw ExceptionFactory.createException(Messages.getString("MysqlIO.39") + this.streamingData + Messages.getString("MysqlIO.40")
                        + Messages.getString("MysqlIO.41") + Messages.getString("MysqlIO.42"), this.exceptionInterceptor);
            }

            // Close the result set
            this.streamingData.getOwner().closeOwner(false);

            // clear any pending data....
            clearInputStream();
        }
    }

    public void unsetStreamingData(ResultsetRows streamer) {
        if (this.streamingData == null) {
            throw ExceptionFactory.createException(Messages.getString("MysqlIO.17") + streamer + Messages.getString("MysqlIO.18"), this.exceptionInterceptor);
        }

        // don't try to discard streamer other than passed in; it's probably already replaced by the next result in a multi-resultset
        if (streamer == this.streamingData) {
            this.streamingData = null;
        }
    }

    public void scanForAndThrowDataTruncation() {
        if (this.streamingData == null && this.propertySet.getBooleanProperty(PropertyKey.jdbcCompliantTruncation).getValue() && getWarningCount() > 0) {
            int warningCountOld = getWarningCount();
            convertShowWarningsToSQLWarnings(true);
            setWarningCount(warningCountOld);
        }
    }

    public StringBuilder generateQueryCommentBlock(StringBuilder buf) {
        buf.append("/* conn id ");
        buf.append(getServerSession().getCapabilities().getThreadId());
        buf.append(" clock: ");
        buf.append(System.currentTimeMillis());
        buf.append(" */ ");

        return buf;
    }

    public BaseMetricsHolder getMetricsHolder() {
        return this.metricsHolder;
    }

    private void appendDeadlockStatusInformation(Session sess, String xOpen, StringBuilder errorBuf) {
        if (sess.getPropertySet().getBooleanProperty(PropertyKey.includeInnodbStatusInDeadlockExceptions).getValue() && xOpen != null
                && (xOpen.startsWith("40") || xOpen.startsWith("41")) && getStreamingData() == null) {

            TelemetrySpan span = this.session.getTelemetryHandler().startSpan(TelemetrySpanName.STMT_EXECUTE);
            try (TelemetryScope scope = span.makeCurrent()) {
                span.setAttribute(TelemetryAttribute.DB_NAME, this.session.getHostInfo().getDatabase());
                span.setAttribute(TelemetryAttribute.DB_OPERATION, TelemetryAttribute.OPERATION_SHOW);
                span.setAttribute(TelemetryAttribute.DB_STATEMENT, TelemetryAttribute.OPERATION_SHOW + TelemetryAttribute.STATEMENT_SUFFIX);
                span.setAttribute(TelemetryAttribute.DB_SYSTEM, TelemetryAttribute.DB_SYSTEM_DEFAULT);
                span.setAttribute(TelemetryAttribute.DB_USER, this.session.getHostInfo().getUser());
                span.setAttribute(TelemetryAttribute.THREAD_ID, Thread.currentThread().getId());
                span.setAttribute(TelemetryAttribute.THREAD_NAME, Thread.currentThread().getName());

                try {
                    NativePacketPayload resultPacket = sendCommand(
                            getNativeMessageBuilder().buildComQuery(getSharedSendPacket(), this.session, "SHOW ENGINE INNODB STATUS"), false, 0);

                    Resultset rs = readAllResults(-1, false, resultPacket, false, null, new ResultsetFactory(Type.FORWARD_ONLY, null));

                    int colIndex = 0;
                    Field f = null;
                    for (int i = 0; i < rs.getColumnDefinition().getFields().length; i++) {
                        f = rs.getColumnDefinition().getFields()[i];
                        if ("Status".equals(f.getName())) {
                            colIndex = i;
                            break;
                        }
                    }

                    ValueFactory<String> vf = new StringValueFactory(this.propertySet);

                    Row r;
                    if ((r = rs.getRows().next()) != null) {
                        errorBuf.append("\n\n").append(r.getValue(colIndex, vf));
                    } else {
                        errorBuf.append("\n\n").append(Messages.getString("MysqlIO.NoInnoDBStatusFound"));
                    }
                } catch (IOException | CJException ex) {
                    errorBuf.append("\n\n").append(Messages.getString("MysqlIO.InnoDBStatusFailed")).append("\n\n").append(Util.stackTraceToString(ex));
                }
            } catch (Throwable t) {
                span.setError(t);
                throw t;
            } finally {
                span.end();
            }
        }

        if (sess.getPropertySet().getBooleanProperty(PropertyKey.includeThreadDumpInDeadlockExceptions).getValue()) {
            errorBuf.append("\n\n*** Java threads running at time of deadlock ***\n\n");

            ThreadMXBean threadMBean = ManagementFactory.getThreadMXBean();
            long[] threadIds = threadMBean.getAllThreadIds();

            ThreadInfo[] threads = threadMBean.getThreadInfo(threadIds, Integer.MAX_VALUE);
            List<ThreadInfo> activeThreads = new ArrayList<>();

            for (ThreadInfo info : threads) {
                if (info != null) {
                    activeThreads.add(info);
                }
            }

            for (ThreadInfo threadInfo : activeThreads) {
                // "Thread-60" daemon prio=1 tid=0x093569c0 nid=0x1b99 in Object.wait()

                errorBuf.append('"').append(threadInfo.getThreadName()).append("\" tid=").append(threadInfo.getThreadId()).append(" ")
                        .append(threadInfo.getThreadState());

                if (threadInfo.getLockName() != null) {
                    errorBuf.append(" on lock=").append(threadInfo.getLockName());
                }
                if (threadInfo.isSuspended()) {
                    errorBuf.append(" (suspended)");
                }
                if (threadInfo.isInNative()) {
                    errorBuf.append(" (running in native)");
                }

                StackTraceElement[] stackTrace = threadInfo.getStackTrace();

                if (stackTrace.length > 0) {
                    errorBuf.append(" in ");
                    errorBuf.append(stackTrace[0].getClassName()).append(".");
                    errorBuf.append(stackTrace[0].getMethodName()).append("()");
                }

                errorBuf.append("\n");

                if (threadInfo.getLockOwnerName() != null) {
                    errorBuf.append("\t owned by ").append(threadInfo.getLockOwnerName()).append(" Id=").append(threadInfo.getLockOwnerId()).append("\n");
                }

                for (int j = 0; j < stackTrace.length; j++) {
                    StackTraceElement ste = stackTrace[j];
                    errorBuf.append("\tat ").append(ste.toString()).append("\n");
                }
            }
        }
    }

    private StringBuilder appendResultSetSlashGStyle(StringBuilder appendTo, Resultset rs) {
        Field[] fields = rs.getColumnDefinition().getFields();
        int maxWidth = 0;
        for (int i = 0; i < fields.length; i++) {
            if (fields[i].getColumnLabel().length() > maxWidth) {
                maxWidth = fields[i].getColumnLabel().length();
            }
        }

        int rowCount = 1;
        Row r;
        while ((r = rs.getRows().next()) != null) {
            appendTo.append("*************************** ");
            appendTo.append(rowCount++);
            appendTo.append(". row ***************************\n");

            for (int i = 0; i < fields.length; i++) {
                int leftPad = maxWidth - fields[i].getColumnLabel().length();
                for (int j = 0; j < leftPad; j++) {
                    appendTo.append(" ");
                }
                appendTo.append(fields[i].getColumnLabel()).append(": ");
                String stringVal = r.getValue(i, new StringValueFactory(this.propertySet));
                appendTo.append(stringVal != null ? stringVal : "NULL").append("\n");
            }
            appendTo.append("\n");
        }
        return appendTo;
    }

    /**
     * Turns output of 'SHOW WARNINGS' into JDBC SQLWarning instances.
     *
     * If 'forTruncationOnly' is true, only looks for truncation warnings, and
     * actually throws DataTruncation as an exception.
     *
     * @param forTruncationOnly
     *            if this method should only scan for data truncation warnings
     *
     * @return the SQLWarning chain (or null if no warnings)
     */
    public SQLWarning convertShowWarningsToSQLWarnings(boolean forTruncationOnly) {
        if (this.warningCount == 0) {
            return null;
        }

        SQLWarning currentWarning = null;
        ResultsetRows rows = null;

        TelemetrySpan span = this.session.getTelemetryHandler().startSpan(TelemetrySpanName.SHOW_WARNINGS);
        try (TelemetryScope scope = span.makeCurrent()) {
            span.setAttribute(TelemetryAttribute.DB_NAME, this.session.getHostInfo().getDatabase());
            span.setAttribute(TelemetryAttribute.DB_OPERATION, TelemetryAttribute.OPERATION_SHOW);
            span.setAttribute(TelemetryAttribute.DB_STATEMENT, TelemetryAttribute.OPERATION_SHOW + TelemetryAttribute.STATEMENT_SUFFIX);
            span.setAttribute(TelemetryAttribute.DB_SYSTEM, TelemetryAttribute.DB_SYSTEM_DEFAULT);
            span.setAttribute(TelemetryAttribute.DB_USER, this.session.getHostInfo().getUser());
            span.setAttribute(TelemetryAttribute.THREAD_ID, Thread.currentThread().getId());
            span.setAttribute(TelemetryAttribute.THREAD_NAME, Thread.currentThread().getName());

            try {
                NativePacketPayload resultPacket = sendCommand(getNativeMessageBuilder().buildComQuery(getSharedSendPacket(), this.session, "SHOW WARNINGS"),
                        false, 0);

                Resultset warnRs = readAllResults(-1, this.warningCount > 99 /* stream large warning counts */, resultPacket, false, null,
                        new ResultsetFactory(Type.FORWARD_ONLY, Concurrency.READ_ONLY));

                int codeFieldIndex = warnRs.getColumnDefinition().findColumn("Code", false, 1) - 1;
                int messageFieldIndex = warnRs.getColumnDefinition().findColumn("Message", false, 1) - 1;

                ValueFactory<String> svf = new StringValueFactory(this.propertySet);
                ValueFactory<Integer> ivf = new IntegerValueFactory(this.propertySet);

                rows = warnRs.getRows();
                Row r;
                while ((r = rows.next()) != null) {

                    int code = r.getValue(codeFieldIndex, ivf);

                    if (forTruncationOnly) {
                        if (code == MysqlErrorNumbers.ER_WARN_DATA_TRUNCATED || code == MysqlErrorNumbers.ER_WARN_DATA_OUT_OF_RANGE) {
                            DataTruncation newTruncation = new MysqlDataTruncation(r.getValue(messageFieldIndex, svf), 0, false, false, 0, 0, code);

                            if (currentWarning == null) {
                                currentWarning = newTruncation;
                            } else {
                                currentWarning.setNextWarning(newTruncation);
                            }
                        }
                    } else {
                        //String level = warnRs.getString("Level");
                        String message = r.getValue(messageFieldIndex, svf);

                        SQLWarning newWarning = new SQLWarning(message, MysqlErrorNumbers.mysqlToSqlState(code), code);
                        if (currentWarning == null) {
                            currentWarning = newWarning;
                        } else {
                            currentWarning.setNextWarning(newWarning);
                        }
                    }
                }

                if (forTruncationOnly && currentWarning != null) {
                    throw ExceptionFactory.createException(currentWarning.getMessage(), currentWarning);
                }

                return currentWarning;
            } catch (IOException ex) {
                throw ExceptionFactory.createException(ex.getMessage(), ex);
            } finally {
                if (rows != null) {
                    rows.close();
                }
            }
        } catch (Throwable t) {
            span.setError(t);
            throw t;
        } finally {
            span.end();
        }
    }

    @Override
    public ColumnDefinition readMetadata() {
        throw ExceptionFactory.createException(CJOperationNotSupportedException.class, "Not supported");
    }

    @Override
    public void close() throws IOException {
        throw ExceptionFactory.createException(CJOperationNotSupportedException.class, "Not supported");
    }

    /**
     * Configures the client's timezone if required.
     *
     * @throws CJException
     *             if the timezone the server is configured to use can't be
     *             mapped to a Java timezone.
     */
    @Override
    public void configureTimeZone() {
        String connectionTimeZone = getPropertySet().getStringProperty(PropertyKey.connectionTimeZone).getValue();

        TimeZone selectedTz = null;

        if (connectionTimeZone == null || StringUtils.isEmptyOrWhitespaceOnly(connectionTimeZone) || "LOCAL".equals(connectionTimeZone)) {
            selectedTz = TimeZone.getDefault();

        } else if ("SERVER".equals(connectionTimeZone)) {
            // Session time zone will be detected after the first ServerSession.getSessionTimeZone() call.
            return;

        } else {
            selectedTz = TimeZone.getTimeZone(ZoneId.of(connectionTimeZone)); // TODO use ZoneId.of(String zoneId, Map<String, String> aliasMap) for custom abbreviations support
        }

        this.serverSession.setSessionTimeZone(selectedTz);

        if (getPropertySet().getBooleanProperty(PropertyKey.forceConnectionTimeZoneToSession).getValue()) {
            // TODO don't send 'SET SESSION time_zone' if time_zone is already equal to the selectedTz (but it requires time zone detection)

            TelemetrySpan span = this.session.getTelemetryHandler().startSpan(TelemetrySpanName.SET_VARIABLE, "time_zone");
            try (TelemetryScope scope = span.makeCurrent()) {
                span.setAttribute(TelemetryAttribute.DB_NAME, this.session.getHostInfo().getDatabase());
                span.setAttribute(TelemetryAttribute.DB_OPERATION, TelemetryAttribute.OPERATION_SET);
                span.setAttribute(TelemetryAttribute.DB_STATEMENT, TelemetryAttribute.OPERATION_SET + TelemetryAttribute.STATEMENT_SUFFIX);
                span.setAttribute(TelemetryAttribute.DB_SYSTEM, TelemetryAttribute.DB_SYSTEM_DEFAULT);
                span.setAttribute(TelemetryAttribute.DB_USER, this.session.getHostInfo().getUser());
                span.setAttribute(TelemetryAttribute.THREAD_ID, Thread.currentThread().getId());
                span.setAttribute(TelemetryAttribute.THREAD_NAME, Thread.currentThread().getName());

                StringBuilder query = new StringBuilder("SET SESSION time_zone='");

                ZoneId zid = selectedTz.toZoneId().normalized();
                if (zid instanceof ZoneOffset) {
                    String offsetStr = ((ZoneOffset) zid).getId().replace("Z", "+00:00");
                    query.append(offsetStr);
                    this.serverSession.getServerVariables().put("time_zone", offsetStr);
                } else {
                    query.append(selectedTz.getID());
                    this.serverSession.getServerVariables().put("time_zone", selectedTz.getID());
                }

                query.append("'");
                sendCommand(getNativeMessageBuilder().buildComQuery(null, this.session, query.toString()), false, 0);
            } catch (Throwable t) {
                span.setError(t);
                throw t;
            } finally {
                span.end();
            }
        }
    }

    @Override
    public void initServerSession() {
        configureTimeZone();

        if (this.serverSession.getServerVariables().containsKey("max_allowed_packet")) {
            int serverMaxAllowedPacket = this.serverSession.getServerVariable("max_allowed_packet", -1);

            // use server value if maxAllowedPacket hasn't been given, or max_allowed_packet is smaller
            if (serverMaxAllowedPacket != -1 && (!this.maxAllowedPacket.isExplicitlySet() || serverMaxAllowedPacket < this.maxAllowedPacket.getValue())) {
                this.maxAllowedPacket.setValue(serverMaxAllowedPacket);
            }

            if (this.useServerPrepStmts.getValue()) {
                RuntimeProperty<Integer> blobSendChunkSize = this.propertySet.getProperty(PropertyKey.blobSendChunkSize);
                int preferredBlobSendChunkSize = blobSendChunkSize.getValue();

                // LONG_DATA and MySQLIO packet header size
                int packetHeaderSize = ServerPreparedQuery.BLOB_STREAM_READ_BUF_SIZE + 11;
                int allowedBlobSendChunkSize = Math.min(preferredBlobSendChunkSize, this.maxAllowedPacket.getValue()) - packetHeaderSize;

                if (allowedBlobSendChunkSize <= 0) {
                    throw ExceptionFactory.createException(Messages.getString("Connection.15", new Object[] { packetHeaderSize }),
                            MysqlErrorNumbers.SQL_STATE_INVALID_CONNECTION_ATTRIBUTE, 0, false, null, this.exceptionInterceptor);
                }

                blobSendChunkSize.setValue(allowedBlobSendChunkSize);
            }
        }

        this.serverSession.getCharsetSettings().configurePostHandshake(false);
    }

}
