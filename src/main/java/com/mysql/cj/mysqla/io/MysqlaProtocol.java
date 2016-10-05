/*
  Copyright (c) 2015, 2016, Oracle and/or its affiliates. All rights reserved.

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

package com.mysql.cj.mysqla.io;

import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.lang.ref.SoftReference;
import java.net.MalformedURLException;
import java.net.SocketException;
import java.net.URL;
import java.sql.SQLException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import com.mysql.cj.api.MysqlConnection;
import com.mysql.cj.api.ProfilerEvent;
import com.mysql.cj.api.ProfilerEventHandler;
import com.mysql.cj.api.authentication.AuthenticationProvider;
import com.mysql.cj.api.conf.PropertySet;
import com.mysql.cj.api.conf.ReadableProperty;
import com.mysql.cj.api.conf.RuntimeProperty;
import com.mysql.cj.api.conf.RuntimeProperty.RuntimePropertyListener;
import com.mysql.cj.api.io.PacketReceivedTimeHolder;
import com.mysql.cj.api.io.PacketSentTimeHolder;
import com.mysql.cj.api.io.ServerSession;
import com.mysql.cj.api.io.SocketConnection;
import com.mysql.cj.api.jdbc.JdbcConnection;
import com.mysql.cj.api.jdbc.Statement;
import com.mysql.cj.api.jdbc.interceptors.StatementInterceptor;
import com.mysql.cj.api.log.Log;
import com.mysql.cj.api.mysqla.io.NativeProtocol;
import com.mysql.cj.api.mysqla.io.PacketHeader;
import com.mysql.cj.api.mysqla.io.PacketPayload;
import com.mysql.cj.api.mysqla.io.PacketReader;
import com.mysql.cj.api.mysqla.io.PacketSender;
import com.mysql.cj.api.mysqla.io.ProtocolEntityFactory;
import com.mysql.cj.api.mysqla.io.ProtocolEntityReader;
import com.mysql.cj.api.mysqla.result.ColumnDefinition;
import com.mysql.cj.api.mysqla.result.ProtocolEntity;
import com.mysql.cj.api.mysqla.result.Resultset;
import com.mysql.cj.api.mysqla.result.ResultsetRow;
import com.mysql.cj.api.mysqla.result.ResultsetRows;
import com.mysql.cj.core.CharsetMapping;
import com.mysql.cj.core.Constants;
import com.mysql.cj.core.Messages;
import com.mysql.cj.core.MysqlType;
import com.mysql.cj.core.ServerVersion;
import com.mysql.cj.core.conf.PropertyDefinitions;
import com.mysql.cj.core.exceptions.CJCommunicationsException;
import com.mysql.cj.core.exceptions.CJConnectionFeatureNotAvailableException;
import com.mysql.cj.core.exceptions.CJException;
import com.mysql.cj.core.exceptions.CJPacketTooBigException;
import com.mysql.cj.core.exceptions.CJTimeoutException;
import com.mysql.cj.core.exceptions.ClosedOnExpiredPasswordException;
import com.mysql.cj.core.exceptions.DataTruncationException;
import com.mysql.cj.core.exceptions.ExceptionFactory;
import com.mysql.cj.core.exceptions.FeatureNotAvailableException;
import com.mysql.cj.core.exceptions.MysqlErrorNumbers;
import com.mysql.cj.core.exceptions.OperationCancelledException;
import com.mysql.cj.core.exceptions.PasswordExpiredException;
import com.mysql.cj.core.exceptions.UnableToConnectException;
import com.mysql.cj.core.exceptions.WrongArgumentException;
import com.mysql.cj.core.io.AbstractProtocol;
import com.mysql.cj.core.io.ExportControlled;
import com.mysql.cj.core.profiler.ProfilerEventImpl;
import com.mysql.cj.core.util.LazyString;
import com.mysql.cj.core.util.LogUtils;
import com.mysql.cj.core.util.StringUtils;
import com.mysql.cj.core.util.TestUtils;
import com.mysql.cj.core.util.Util;
import com.mysql.cj.jdbc.PreparedStatement;
import com.mysql.cj.jdbc.StatementImpl;
import com.mysql.cj.jdbc.exceptions.SQLError;
import com.mysql.cj.jdbc.util.ResultSetUtil;
import com.mysql.cj.jdbc.util.TimeUtil;
import com.mysql.cj.mysqla.MysqlaConstants;
import com.mysql.cj.mysqla.authentication.MysqlaAuthenticationProvider;
import com.mysql.cj.mysqla.result.OkPacket;

public class MysqlaProtocol extends AbstractProtocol implements NativeProtocol, RuntimePropertyListener {

    protected static final int INITIAL_PACKET_SIZE = 1024;
    protected static final int COMP_HEADER_LENGTH = 3;
    protected static final int MAX_QUERY_SIZE_TO_EXPLAIN = 1024 * 1024; // don't explain queries above 1MB
    private static final String EXPLAINABLE_STATEMENT = "SELECT";
    private static final String[] EXPLAINABLE_STATEMENT_EXTENSION = new String[] { "INSERT", "UPDATE", "REPLACE", "DELETE" };

    protected PacketSender packetSender;
    protected PacketReader packetReader;

    protected MysqlaServerSession serverSession;

    /** Track this to manually shut down. */
    protected CompressedPacketSender compressedPacketSender;

    private PacketPayload sendPacket = null;
    protected PacketPayload sharedSendPacket = null;
    /** Use this when reading in rows to avoid thousands of new() calls, because the byte arrays just get copied out of the packet anyway */
    protected PacketPayload reusablePacket = null;

    /**
     * Packet used for 'LOAD DATA LOCAL INFILE'
     * 
     * We use a SoftReference, so that we don't penalize intermittent use of this feature
     */
    private SoftReference<PacketPayload> loadFileBufRef;

    protected byte packetSequence = 0;
    protected boolean useCompression = false;
    private ReadableProperty<Integer> maxAllowedPacket;

    private boolean needToGrabQueryFromPacket;
    private boolean autoGenerateTestcaseScript;

    /** Does the server support long column info? */
    private boolean logSlowQueries = false;
    private boolean useAutoSlowLog;

    private boolean profileSQL = false;

    private boolean useNanosForElapsedTime;
    private long slowQueryThreshold;
    private String queryTimingUnits;

    private int commandCount = 0;

    protected boolean hadWarnings = false;
    private int warningCount = 0;
    private boolean queryBadIndexUsed = false;
    private boolean queryNoIndexUsed = false;
    private boolean serverQueryWasSlow = false;

    protected Map<Class<? extends ProtocolEntity>, ProtocolEntityReader<? extends ProtocolEntity>> PROTOCOL_ENTITY_CLASS_TO_TEXT_READER;
    protected Map<Class<? extends ProtocolEntity>, ProtocolEntityReader<? extends ProtocolEntity>> PROTOCOL_ENTITY_CLASS_TO_BINARY_READER;

    /**
     * Does the character set of this connection match the character set of the
     * platform
     */
    protected boolean platformDbCharsetMatches = true; // changed once we've connected.

    private int statementExecutionDepth = 0;
    private List<StatementInterceptor> statementInterceptors;

    private ReadableProperty<Boolean> maintainTimeStats;
    private ReadableProperty<Integer> maxQuerySizeToLog;

    private InputStream localInfileInputStream;

    /**
     * We store the platform 'encoding' here, only used to avoid munging filenames for LOAD DATA LOCAL INFILE...
     */
    private static String jvmPlatformCharset = null;

    static {
        OutputStreamWriter outWriter = null;

        //
        // Use the I/O system to get the encoding (if possible), to avoid security restrictions on System.getProperty("file.encoding") in applets (why is that
        // restricted?)
        //
        try {
            outWriter = new OutputStreamWriter(new ByteArrayOutputStream());
            jvmPlatformCharset = outWriter.getEncoding();
        } finally {
            try {
                if (outWriter != null) {
                    outWriter.close();
                }
            } catch (IOException ioEx) {
                // ignore
            }
        }
    }

    public static MysqlaProtocol getInstance(MysqlConnection conn, SocketConnection socketConnection, PropertySet propertySet, Log log) {
        MysqlaProtocol protocol = new MysqlaProtocol(log);
        protocol.init(conn, propertySet.getIntegerReadableProperty(PropertyDefinitions.PNAME_socketTimeout).getValue(), socketConnection, propertySet);
        return protocol;
    }

    public MysqlaProtocol(Log logger) {
        this.log = logger;
    }

    @Override
    public void init(MysqlConnection conn, int socketTimeout, SocketConnection phConnection, PropertySet propSet) {

        this.connection = conn;
        this.propertySet = propSet;

        this.socketConnection = phConnection;
        this.exceptionInterceptor = this.socketConnection.getExceptionInterceptor();

        this.maintainTimeStats = this.propertySet.getBooleanReadableProperty(PropertyDefinitions.PNAME_maintainTimeStats);
        this.maxQuerySizeToLog = this.propertySet.getIntegerReadableProperty(PropertyDefinitions.PNAME_maxQuerySizeToLog);
        this.useAutoSlowLog = this.propertySet.getBooleanReadableProperty(PropertyDefinitions.PNAME_autoSlowLog).getValue();
        this.logSlowQueries = this.propertySet.getBooleanReadableProperty(PropertyDefinitions.PNAME_logSlowQueries).getValue();
        this.maxAllowedPacket = this.propertySet.getIntegerReadableProperty(PropertyDefinitions.PNAME_maxAllowedPacket);
        this.profileSQL = this.propertySet.getBooleanReadableProperty(PropertyDefinitions.PNAME_profileSQL).getValue();
        this.autoGenerateTestcaseScript = this.propertySet.getBooleanReadableProperty(PropertyDefinitions.PNAME_autoGenerateTestcaseScript).getValue();

        this.reusablePacket = new Buffer(INITIAL_PACKET_SIZE);
        this.sendPacket = new Buffer(INITIAL_PACKET_SIZE);

        this.packetSender = new SimplePacketSender(this.socketConnection.getMysqlOutput());
        this.packetReader = new SimplePacketReader(this.socketConnection, this.maxAllowedPacket);

        this.needToGrabQueryFromPacket = (this.profileSQL || this.logSlowQueries || this.autoGenerateTestcaseScript);

        if (this.propertySet.getBooleanReadableProperty(PropertyDefinitions.PNAME_useNanosForElapsedTime).getValue() && TimeUtil.nanoTimeAvailable()) {
            this.useNanosForElapsedTime = true;

            this.queryTimingUnits = Messages.getString("Nanoseconds");
        } else {
            this.queryTimingUnits = Messages.getString("Milliseconds");
        }

        if (this.propertySet.getBooleanReadableProperty(PropertyDefinitions.PNAME_logSlowQueries).getValue()) {
            calculateSlowQueryThreshold();
        }

        this.authProvider = new MysqlaAuthenticationProvider(this.log);
        this.authProvider.init(this, this.getPropertySet(), this.socketConnection.getExceptionInterceptor());

        Map<Class<? extends ProtocolEntity>, ProtocolEntityReader<? extends ProtocolEntity>> protocolEntityClassToTextReader = new HashMap<>();
        protocolEntityClassToTextReader.put(ColumnDefinition.class, new ColumnDefinitionReader(this));
        protocolEntityClassToTextReader.put(ResultsetRow.class, new ResultsetRowReader(this));
        protocolEntityClassToTextReader.put(Resultset.class, new TextResultsetReader(this));
        this.PROTOCOL_ENTITY_CLASS_TO_TEXT_READER = Collections.unmodifiableMap(protocolEntityClassToTextReader);

        Map<Class<? extends ProtocolEntity>, ProtocolEntityReader<? extends ProtocolEntity>> protocolEntityClassToBinaryReader = new HashMap<>();
        protocolEntityClassToBinaryReader.put(ColumnDefinition.class, new ColumnDefinitionReader(this));
        protocolEntityClassToBinaryReader.put(Resultset.class, new BinaryResultsetReader(this));
        this.PROTOCOL_ENTITY_CLASS_TO_BINARY_READER = Collections.unmodifiableMap(protocolEntityClassToBinaryReader);

    }

    public PacketSender getPacketSender() {
        return this.packetSender;
    }

    public PacketReader getPacketReader() {
        return this.packetReader;
    }

    /**
     * Negotiates the SSL communications channel used when connecting
     * to a MySQL server that understands SSL.
     * 
     * @param packLength
     */
    @Override
    public void negotiateSSLConnection(int packLength) {
        if (!ExportControlled.enabled()) {
            throw new CJConnectionFeatureNotAvailableException(this.getPropertySet(), this.serverSession,
                    this.getPacketSentTimeHolder().getLastPacketSentTime(), null);
        }

        long clientParam = this.serverSession.getClientParam();
        clientParam |= MysqlaServerSession.CLIENT_SSL;
        this.serverSession.setClientParam(clientParam);

        PacketPayload packet = new Buffer(packLength);
        packet.writeInteger(IntegerDataType.INT4, clientParam);
        packet.writeInteger(IntegerDataType.INT4, MysqlaConstants.MAX_PACKET_SIZE);
        packet.writeInteger(IntegerDataType.INT1, AuthenticationProvider.getCharsetForHandshake(this.authProvider.getEncodingForHandshake(),
                this.serverSession.getCapabilities().getServerVersion()));
        packet.writeBytes(StringLengthDataType.STRING_FIXED, new byte[23]);  // Set of bytes reserved for future use.

        send(packet, packet.getPosition());

        try {
            ExportControlled.transformSocketToSSLSocket(this.socketConnection, this.serverSession.getServerVersion());
        } catch (FeatureNotAvailableException nae) {
            throw new CJConnectionFeatureNotAvailableException(this.getPropertySet(), this.serverSession,
                    this.getPacketSentTimeHolder().getLastPacketSentTime(), nae);
        } catch (IOException ioEx) {
            throw ExceptionFactory.createCommunicationsException(this.getConnection().getPropertySet(), this.serverSession,
                    this.getPacketSentTimeHolder().getLastPacketSentTime(), this.getPacketReceivedTimeHolder().getLastPacketReceivedTime(), ioEx,
                    getExceptionInterceptor());
        }
        // i/o streams were replaced, build new packet sender/reader
        this.packetSender = new SimplePacketSender(this.socketConnection.getMysqlOutput());
        this.packetReader = new SimplePacketReader(this.socketConnection, this.maxAllowedPacket);
    }

    // TODO: find a better place for method?
    @Override
    public void rejectConnection(String message) {
        try {
            ((JdbcConnection) this.connection).close();
        } catch (SQLException e) {
            throw ExceptionFactory.createException(e.getMessage(), e, getExceptionInterceptor());
        }
        this.socketConnection.forceClose();
        throw ExceptionFactory.createException(UnableToConnectException.class, message, getExceptionInterceptor());
    }

    @Override
    public void rejectProtocol(PacketPayload buf) {
        try {
            this.socketConnection.getMysqlSocket().close();
        } catch (Exception e) {
            // ignore
        }

        int errno = 2000;

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

        String xOpen = SQLError.mysqlToSqlState(errno);

        throw ExceptionFactory.createException(SQLError.get(xOpen) + ", " + errorBuf.toString(), xOpen, errno, false, null, getExceptionInterceptor());
    }

    @Override
    public void beforeHandshake() {
        // Reset packet sequences
        this.packetReader.resetPacketSequence();

        // Create session state
        this.serverSession = new MysqlaServerSession(this.propertySet);

        // Read the first packet
        MysqlaCapabilities capabilities = readServerCapabilities();
        this.serverSession.setCapabilities(capabilities);

    }

    @Override
    public void afterHandshake() {

        checkTransactionState();

        PropertySet pset = this.getPropertySet();

        //
        // Can't enable compression until after handshake
        //
        if (((this.serverSession.getCapabilities().getCapabilityFlags() & MysqlaServerSession.CLIENT_COMPRESS) != 0)
                && pset.getBooleanReadableProperty(PropertyDefinitions.PNAME_useCompression).getValue()
                && !(this.socketConnection.getMysqlInput().getUnderlyingStream() instanceof CompressedInputStream)) {
            this.useCompression = true;
            this.socketConnection.setMysqlInput(new CompressedInputStream(this.socketConnection.getMysqlInput(),
                    pset.getBooleanReadableProperty(PropertyDefinitions.PNAME_traceProtocol), this.log));
            this.compressedPacketSender = new CompressedPacketSender(this.socketConnection.getMysqlOutput());
            this.packetSender = this.compressedPacketSender;
        }

        applyPacketDecorators(this.packetSender, this.packetReader);

        try {
            this.socketConnection.setMysqlSocket(this.socketConnection.getSocketFactory().afterHandshake());
        } catch (IOException ioEx) {
            throw ExceptionFactory.createCommunicationsException(this.getPropertySet(), this.serverSession,
                    this.getPacketSentTimeHolder().getLastPacketSentTime(), this.getPacketReceivedTimeHolder().getLastPacketReceivedTime(), ioEx,
                    getExceptionInterceptor());
        }

        // listen for properties changes to allow decorators reconfiguration
        this.maintainTimeStats.addListener(this);
        pset.getBooleanReadableProperty(PropertyDefinitions.PNAME_traceProtocol).addListener(this);
        pset.getBooleanReadableProperty(PropertyDefinitions.PNAME_enablePacketDebug).addListener(this);
    }

    @Override
    public void handlePropertyChange(RuntimeProperty<?> prop) {
        switch (prop.getPropertyDefinition().getName()) {
            case PropertyDefinitions.PNAME_maintainTimeStats:
            case PropertyDefinitions.PNAME_traceProtocol:
            case PropertyDefinitions.PNAME_enablePacketDebug:

                applyPacketDecorators(this.packetSender.undecorateAll(), this.packetReader.undecorateAll());

                break;

            default:
                break;
        }
    }

    /**
     * Apply optional decorators to configured PacketSender and PacketReader.
     */
    public void applyPacketDecorators(PacketSender sender, PacketReader reader) {
        TimeTrackingPacketSender ttSender = null;
        TimeTrackingPacketReader ttReader = null;
        LinkedList<StringBuilder> debugRingBuffer = null;

        if (this.maintainTimeStats.getValue()) {
            ttSender = new TimeTrackingPacketSender(sender);
            sender = ttSender;

            ttReader = new TimeTrackingPacketReader(reader);
            reader = ttReader;
        }

        if (this.propertySet.getBooleanReadableProperty(PropertyDefinitions.PNAME_traceProtocol).getValue()) {
            sender = new TracingPacketSender(sender, this.log, this.socketConnection.getHost(), getServerSession().getCapabilities().getThreadId());
            reader = new TracingPacketReader(reader, this.log);
        }

        if (this.getPropertySet().getBooleanReadableProperty(PropertyDefinitions.PNAME_enablePacketDebug).getValue()) {

            debugRingBuffer = new LinkedList<StringBuilder>();

            sender = new DebugBufferingPacketSender(sender, debugRingBuffer,
                    this.propertySet.getIntegerReadableProperty(PropertyDefinitions.PNAME_packetDebugBufferSize));
            reader = new DebugBufferingPacketReader(reader, debugRingBuffer,
                    this.propertySet.getIntegerReadableProperty(PropertyDefinitions.PNAME_packetDebugBufferSize));
        }

        // do it after other decorators to have trace and debug applied to individual packets 
        reader = new MultiPacketReader(reader);

        // atomic replacement of currently used objects
        synchronized (this.packetReader) {
            this.packetReader = reader;
            this.packetDebugRingBuffer = debugRingBuffer;
            this.setPacketSentTimeHolder(ttSender != null ? ttSender : new PacketSentTimeHolder() {
                public long getLastPacketSentTime() {
                    return 0;
                }
            });
        }
        synchronized (this.packetSender) {
            this.packetSender = sender;
            this.setPacketReceivedTimeHolder(ttReader != null ? ttReader : new PacketReceivedTimeHolder() {
                public long getLastPacketReceivedTime() {
                    return 0;
                }
            });
        }
    }

    public MysqlaCapabilities readServerCapabilities() {
        // Read the first packet
        PacketPayload buf = readPacket(null);
        MysqlaCapabilities serverCapabilities = new MysqlaCapabilities();
        serverCapabilities.setInitialHandshakePacket(buf);

        // ERR packet instead of Initial Handshake
        if (serverCapabilities.getProtocolVersion() == -1) {
            rejectProtocol(buf);
        }

        return serverCapabilities;

    }

    @Override
    public MysqlaServerSession getServerSession() {
        return this.serverSession;
    }

    @Override
    public void changeDatabase(String database) {
        if (database == null || database.length() == 0) {
            return;
        }

        try {
            sendCommand(MysqlaConstants.COM_INIT_DB, database, null, false, null, 0);
        } catch (CJException ex) {
            if (this.getPropertySet().getBooleanReadableProperty(PropertyDefinitions.PNAME_createDatabaseIfNotExist).getValue()) {
                sendCommand(MysqlaConstants.COM_QUERY, "CREATE DATABASE IF NOT EXISTS " + database, null, false, null, 0);
                sendCommand(MysqlaConstants.COM_INIT_DB, database, null, false, null, 0);
            } else {
                throw ExceptionFactory.createCommunicationsException(this.getPropertySet(), this.serverSession,
                        this.getPacketSentTimeHolder().getLastPacketSentTime(), this.getPacketReceivedTimeHolder().getLastPacketReceivedTime(), ex,
                        getExceptionInterceptor());
            }
        }
    }

    @Override
    public final PacketPayload readPacket(PacketPayload reuse) {
        try {
            PacketHeader header = this.packetReader.readHeader();
            PacketPayload buf = this.packetReader.readPayload(Optional.ofNullable(reuse), header.getPacketLength());
            this.packetSequence = header.getPacketSequence();
            return buf;

        } catch (IOException ioEx) {
            throw ExceptionFactory.createCommunicationsException(this.propertySet, this.serverSession, this.getPacketSentTimeHolder().getLastPacketSentTime(),
                    this.getPacketReceivedTimeHolder().getLastPacketReceivedTime(), ioEx, getExceptionInterceptor());
        } catch (OutOfMemoryError oom) {
            // TODO: we can't cleanly handle OOM and we shouldn't even try. we should remove ALL of these except for the one allocating the large buffer for sending a file (iirc)
            try {
                ((JdbcConnection) this.connection).realClose(false, false, true, oom);
            } catch (Exception ex) {
            }
            throw oom;
        }
    }

    /**
     * @param packet
     * @param packetLen
     *            length of header + payload
     */
    @Override
    public final void send(PacketPayload packet, int packetLen) {
        try {
            if (this.maxAllowedPacket.getValue() > 0 && packetLen > this.maxAllowedPacket.getValue()) {
                throw new CJPacketTooBigException(packetLen, this.maxAllowedPacket.getValue());
            }

            this.packetSequence++;
            this.packetSender.send(packet.getByteBuffer(), packetLen, this.packetSequence);

            //
            // Don't hold on to large packets
            //
            if (packet == this.sharedSendPacket) {
                reclaimLargeSharedSendPacket();
            }
        } catch (IOException ioEx) {
            throw ExceptionFactory.createCommunicationsException(this.getPropertySet(), this.serverSession,
                    this.getPacketSentTimeHolder().getLastPacketSentTime(), this.getPacketReceivedTimeHolder().getLastPacketReceivedTime(), ioEx,
                    getExceptionInterceptor());
        }
    }

    @Override
    public final PacketPayload sendCommand(int command, String extraData, PacketPayload queryPacket, boolean skipCheck, String extraDataCharEncoding,
            int timeoutMillis) {
        this.commandCount++;

        this.packetReader.resetPacketSequence();

        int oldTimeout = 0;

        if (timeoutMillis != 0) {
            try {
                oldTimeout = this.socketConnection.getMysqlSocket().getSoTimeout();
                this.socketConnection.getMysqlSocket().setSoTimeout(timeoutMillis);
            } catch (SocketException e) {
                throw ExceptionFactory.createCommunicationsException(this.propertySet, this.serverSession,
                        this.getPacketSentTimeHolder().getLastPacketSentTime(), this.getPacketReceivedTimeHolder().getLastPacketReceivedTime(), e,
                        getExceptionInterceptor());
            }
        }

        try {

            checkForOutstandingStreamingData();

            // Clear serverStatus...this value is guarded by an external mutex, as you can only ever be processing one command at a time
            this.serverSession.setStatusFlags(0, true);
            this.hadWarnings = false;
            this.setWarningCount(0);

            this.queryNoIndexUsed = false;
            this.queryBadIndexUsed = false;
            this.serverQueryWasSlow = false;

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

                //
                // PreparedStatements construct their own packets, for efficiency's sake.
                //
                // If this is a generic query, we need to re-use the sending packet.
                //
                if (queryPacket == null) {
                    int packLength = COMP_HEADER_LENGTH + 1 + ((extraData != null) ? extraData.length() : 0) + 2;

                    if (this.sendPacket == null) {
                        this.sendPacket = new Buffer(packLength);
                    }

                    this.packetSequence = -1;

                    this.sendPacket.setPosition(0);
                    this.sendPacket.writeInteger(IntegerDataType.INT1, command);

                    if ((command == MysqlaConstants.COM_INIT_DB) || (command == MysqlaConstants.COM_CREATE_DB) || (command == MysqlaConstants.COM_DROP_DB)
                            || (command == MysqlaConstants.COM_QUERY) || (command == MysqlaConstants.COM_STMT_PREPARE)) {

                        this.sendPacket.writeBytes(StringLengthDataType.STRING_FIXED, StringUtils.getBytes(extraData, extraDataCharEncoding));

                    } else if (command == MysqlaConstants.COM_PROCESS_KILL) {
                        long id = Long.parseLong(extraData);
                        this.sendPacket.writeInteger(IntegerDataType.INT4, id);
                    }

                    send(this.sendPacket, this.sendPacket.getPosition());
                } else {
                    this.packetSequence = -1;
                    send(queryPacket, queryPacket.getPosition()); // packet passed by PreparedStatement
                }
            } catch (CJException ex) {
                // don't wrap CJExceptions
                throw ex;
            } catch (Exception ex) {
                throw ExceptionFactory.createCommunicationsException(this.propertySet, this.serverSession,
                        this.getPacketSentTimeHolder().getLastPacketSentTime(), this.getPacketReceivedTimeHolder().getLastPacketReceivedTime(), ex,
                        getExceptionInterceptor());
            }

            PacketPayload returnPacket = null;

            if (!skipCheck) {
                if ((command == MysqlaConstants.COM_STMT_EXECUTE) || (command == MysqlaConstants.COM_STMT_RESET)) {
                    this.packetReader.resetPacketSequence();
                }

                returnPacket = checkErrorPacket(command);
            }

            return returnPacket;
        } catch (IOException ioEx) {
            this.serverSession.preserveOldTransactionState();
            throw ExceptionFactory.createCommunicationsException(this.propertySet, this.serverSession, this.getPacketSentTimeHolder().getLastPacketSentTime(),
                    this.getPacketReceivedTimeHolder().getLastPacketReceivedTime(), ioEx, getExceptionInterceptor());
        } catch (CJException e) {
            this.serverSession.preserveOldTransactionState();
            throw e;

        } finally {
            if (timeoutMillis != 0) {
                try {
                    this.socketConnection.getMysqlSocket().setSoTimeout(oldTimeout);
                } catch (SocketException e) {
                    throw ExceptionFactory.createCommunicationsException(this.propertySet, this.serverSession,
                            this.getPacketSentTimeHolder().getLastPacketSentTime(), this.getPacketReceivedTimeHolder().getLastPacketReceivedTime(), e,
                            getExceptionInterceptor());
                }
            }
        }
    }

    public void checkTransactionState() {
        int transState = this.serverSession.getTransactionState();
        try {
            if (transState == ServerSession.TRANSACTION_COMPLETED) {
                ((JdbcConnection) this.connection).transactionCompleted();
            } else if (transState == ServerSession.TRANSACTION_STARTED) {
                ((JdbcConnection) this.connection).transactionBegun();
            }
        } catch (SQLException e) {
            throw ExceptionFactory.createException(e.getMessage(), e, getExceptionInterceptor());
        }
    }

    public PacketPayload checkErrorPacket() {
        return checkErrorPacket(-1);
    }

    /**
     * Checks for errors in the reply packet, and if none, returns the reply
     * packet, ready for reading
     * 
     * @param command
     *            the command being issued (if used)
     * 
     * @throws CJException
     *             if an error packet was received
     * @throws CJCommunicationsException
     */
    private PacketPayload checkErrorPacket(int command) {

        PacketPayload resultPacket = null;
        this.serverSession.setStatusFlags(0);

        try {
            // Check return value, if we get a java.io.EOFException, the server has gone away. We'll pass it on up the exception chain and let someone higher up
            // decide what to do (barf, reconnect, etc).
            resultPacket = readPacket(this.reusablePacket);
        } catch (CJException ex) {
            // Don't wrap CJExceptions
            throw ex;
        } catch (Exception fallThru) {
            throw ExceptionFactory.createCommunicationsException(this.propertySet, this.serverSession, this.getPacketSentTimeHolder().getLastPacketSentTime(),
                    this.getPacketReceivedTimeHolder().getLastPacketReceivedTime(), fallThru, getExceptionInterceptor());
        }

        checkErrorPacket(resultPacket);

        return resultPacket;
    }

    public void checkErrorPacket(PacketPayload resultPacket) {

        resultPacket.setPosition(0);
        byte statusCode = (byte) resultPacket.readInteger(IntegerDataType.INT1);

        // Error handling
        if (statusCode == (byte) 0xff) {
            String serverErrorMessage;
            int errno = 2000;

            errno = (int) resultPacket.readInteger(IntegerDataType.INT2);

            String xOpen = null;

            serverErrorMessage = resultPacket.readString(StringSelfDataType.STRING_TERM, this.serverSession.getErrorMessageEncoding());

            if (serverErrorMessage.charAt(0) == '#') {

                // we have an SQLState
                if (serverErrorMessage.length() > 6) {
                    xOpen = serverErrorMessage.substring(1, 6);
                    serverErrorMessage = serverErrorMessage.substring(6);

                    if (xOpen.equals("HY000")) {
                        xOpen = SQLError.mysqlToSqlState(errno);
                    }
                } else {
                    xOpen = SQLError.mysqlToSqlState(errno);
                }
            } else {
                xOpen = SQLError.mysqlToSqlState(errno);
            }

            clearInputStream();

            StringBuilder errorBuf = new StringBuilder();

            String xOpenErrorMessage = SQLError.get(xOpen);

            boolean useOnlyServerErrorMessages = this.propertySet.getBooleanReadableProperty(PropertyDefinitions.PNAME_useOnlyServerErrorMessages).getValue();
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

            ResultSetUtil.appendDeadlockStatusInformation(this.connection, xOpen, errorBuf);

            if (xOpen != null) {
                if (xOpen.startsWith("22")) {
                    throw new DataTruncationException(errorBuf.toString(), 0, true, false, 0, 0, errno);
                }

                if (errno == MysqlErrorNumbers.ER_MUST_CHANGE_PASSWORD) {
                    throw ExceptionFactory.createException(PasswordExpiredException.class, errorBuf.toString(), getExceptionInterceptor());

                } else if (errno == MysqlErrorNumbers.ER_MUST_CHANGE_PASSWORD_LOGIN) {
                    throw ExceptionFactory.createException(ClosedOnExpiredPasswordException.class, errorBuf.toString(), getExceptionInterceptor());
                }
            }

            throw ExceptionFactory.createException(errorBuf.toString(), xOpen, errno, false, null, getExceptionInterceptor());

        }
    }

    private void reclaimLargeSharedSendPacket() {
        if ((this.sharedSendPacket != null) && (this.sharedSendPacket.getCapacity() > 1048576)) {
            this.sharedSendPacket = new Buffer(INITIAL_PACKET_SIZE);
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
            throw ExceptionFactory.createCommunicationsException(this.propertySet, this.serverSession, this.getPacketSentTimeHolder().getLastPacketSentTime(),
                    this.getPacketReceivedTimeHolder().getLastPacketReceivedTime(), ioEx, getExceptionInterceptor());
        }
    }

    /**
     * Don't hold on to overly-large packets
     */
    public void reclaimLargeReusablePacket() {
        if ((this.reusablePacket != null) && (this.reusablePacket.getCapacity() > 1048576)) {
            this.reusablePacket = new Buffer(INITIAL_PACKET_SIZE);
        }
    }

    /**
     * Send a query stored in a packet directly to the server.
     * 
     * @param callingStatement
     * @param resultSetConcurrency
     * @param characterEncoding
     * @param queryPacket
     * @param maxRows
     * @param conn
     * @param streamResults
     * @param catalog
     * @param unpackFieldInfo
     *            should we read MYSQL_FIELD info (if available)?
     * @throws IOException
     * 
     */
    public final <T extends Resultset> T sqlQueryDirect(StatementImpl callingStatement, String query, String characterEncoding, PacketPayload queryPacket,
            int maxRows, boolean streamResults, String catalog, ColumnDefinition cachedMetadata,
            GetProfilerEventHandlerInstanceFunction getProfilerEventHandlerInstanceFunction, ProtocolEntityFactory<T> resultSetFactory) throws IOException {
        this.statementExecutionDepth++;

        try {
            if (this.statementInterceptors != null) {
                T interceptedResults = invokeStatementInterceptorsPre(query, callingStatement, false);

                if (interceptedResults != null) {
                    return interceptedResults;
                }
            }

            long queryStartTime = 0;
            long queryEndTime = 0;

            String statementComment = ((JdbcConnection) this.connection).getStatementComment();

            if (this.propertySet.getBooleanReadableProperty(PropertyDefinitions.PNAME_includeThreadNamesAsStatementComment).getValue()) {
                statementComment = (statementComment != null ? statementComment + ", " : "") + "java thread: " + Thread.currentThread().getName();
            }

            if (query != null) {
                // We don't know exactly how many bytes we're going to get from the query. Since we're dealing with Unicode, the max is 2, so pad it
                // (2 * query) + space for headers
                int packLength = 1 + (query.length() * 3) + 2;

                byte[] commentAsBytes = null;

                if (statementComment != null) {
                    commentAsBytes = StringUtils.getBytes(statementComment, characterEncoding);

                    packLength += commentAsBytes.length;
                    packLength += 6; // for /*[space] [space]*/
                }

                if (this.sendPacket == null) {
                    this.sendPacket = new Buffer(packLength);
                }
                this.sendPacket.setPosition(0);

                this.sendPacket.writeInteger(IntegerDataType.INT1, MysqlaConstants.COM_QUERY);

                if (commentAsBytes != null) {
                    this.sendPacket.writeBytes(StringLengthDataType.STRING_FIXED, Constants.SLASH_STAR_SPACE_AS_BYTES);
                    this.sendPacket.writeBytes(StringLengthDataType.STRING_FIXED, commentAsBytes);
                    this.sendPacket.writeBytes(StringLengthDataType.STRING_FIXED, Constants.SPACE_STAR_SLASH_SPACE_AS_BYTES);
                }

                if (!this.platformDbCharsetMatches && StringUtils.startsWithIgnoreCaseAndWs(query, "LOAD DATA")) {
                    this.sendPacket.writeBytes(StringLengthDataType.STRING_FIXED, StringUtils.getBytes(query));
                } else {
                    this.sendPacket.writeBytes(StringLengthDataType.STRING_FIXED, StringUtils.getBytes(query, characterEncoding));
                }

                queryPacket = this.sendPacket;
            }

            byte[] queryBuf = null;
            int oldPacketPosition = 0;

            if (this.needToGrabQueryFromPacket) {
                queryBuf = queryPacket.getByteBuffer();

                // save the packet position
                oldPacketPosition = queryPacket.getPosition();

                queryStartTime = getCurrentTimeNanosOrMillis();
            }

            if (this.autoGenerateTestcaseScript) {
                String testcaseQuery = null;

                if (query != null) {
                    if (statementComment != null) {
                        testcaseQuery = "/* " + statementComment + " */ " + query;
                    } else {
                        testcaseQuery = query;
                    }
                } else {
                    testcaseQuery = StringUtils.toString(queryBuf, 1, (oldPacketPosition - 1));
                }

                StringBuilder debugBuf = new StringBuilder(testcaseQuery.length() + 32);
                ((JdbcConnection) this.connection).generateConnectionCommentBlock(debugBuf);
                debugBuf.append(testcaseQuery);
                debugBuf.append(';');
                TestUtils.dumpTestcaseQuery(debugBuf.toString());
            }

            // Send query command and sql query string
            PacketPayload resultPacket = sendCommand(MysqlaConstants.COM_QUERY, null, queryPacket, false, null, 0);

            long fetchBeginTime = 0;
            long fetchEndTime = 0;

            String profileQueryToLog = null;

            boolean queryWasSlow = false;

            if (this.profileSQL || this.logSlowQueries) {
                queryEndTime = getCurrentTimeNanosOrMillis();

                boolean shouldExtractQuery = false;

                if (this.profileSQL) {
                    shouldExtractQuery = true;
                } else if (this.logSlowQueries) {
                    long queryTime = queryEndTime - queryStartTime;

                    boolean logSlow = false;

                    if (!this.useAutoSlowLog) {
                        logSlow = queryTime > this.propertySet.getIntegerReadableProperty(PropertyDefinitions.PNAME_slowQueryThresholdMillis).getValue();
                    } else {
                        logSlow = ((JdbcConnection) this.connection).isAbonormallyLongQuery(queryTime);

                        ((JdbcConnection) this.connection).reportQueryTime(queryTime);
                    }

                    if (logSlow) {
                        shouldExtractQuery = true;
                        queryWasSlow = true;
                    }
                }

                if (shouldExtractQuery) {
                    // Extract the actual query from the network packet
                    boolean truncated = false;

                    int extractPosition = oldPacketPosition;

                    if (oldPacketPosition > this.maxQuerySizeToLog.getValue()) {
                        extractPosition = this.maxQuerySizeToLog.getValue() + 1;
                        truncated = true;
                    }

                    profileQueryToLog = StringUtils.toString(queryBuf, 1, (extractPosition - 1));

                    if (truncated) {
                        profileQueryToLog += Messages.getString("Protocol.2");
                    }
                }

                fetchBeginTime = queryEndTime;
            }

            T rs = readAllResults(maxRows, streamResults, resultPacket, false, cachedMetadata, resultSetFactory);

            if (queryWasSlow && !this.serverQueryWasSlow /* don't log slow queries twice */) {
                StringBuilder mesgBuf = new StringBuilder(48 + profileQueryToLog.length());

                mesgBuf.append(Messages.getString("Protocol.SlowQuery",
                        new Object[] { String.valueOf(this.useAutoSlowLog ? " 95% of all queries " : this.slowQueryThreshold), this.queryTimingUnits,
                                Long.valueOf(queryEndTime - queryStartTime) }));
                mesgBuf.append(profileQueryToLog);

                ProfilerEventHandler eventSink = getProfilerEventHandlerInstanceFunction.apply();

                eventSink.consumeEvent(new ProfilerEventImpl(ProfilerEvent.TYPE_SLOW_QUERY, "", catalog, this.connection.getId(),
                        (callingStatement != null) ? callingStatement.getId() : 999, rs.getResultId(), System.currentTimeMillis(),
                        (int) (queryEndTime - queryStartTime), this.queryTimingUnits, null, LogUtils.findCallingClassAndMethod(new Throwable()),
                        mesgBuf.toString()));

                if (this.propertySet.getBooleanReadableProperty(PropertyDefinitions.PNAME_explainSlowQueries).getValue()) {
                    if (oldPacketPosition < MAX_QUERY_SIZE_TO_EXPLAIN) {
                        queryPacket.setPosition(1); // skip first byte 
                        explainSlowQuery(queryPacket.readBytes(StringLengthDataType.STRING_FIXED, oldPacketPosition - 1), profileQueryToLog);
                    } else {
                        this.log.logWarn(Messages.getString("Protocol.3", new Object[] { MAX_QUERY_SIZE_TO_EXPLAIN }));
                    }
                }
            }

            if (this.logSlowQueries) {

                ProfilerEventHandler eventSink = getProfilerEventHandlerInstanceFunction.apply();

                if (this.queryBadIndexUsed && this.profileSQL) {
                    eventSink.consumeEvent(new ProfilerEventImpl(ProfilerEvent.TYPE_SLOW_QUERY, "", catalog, this.connection.getId(),
                            (callingStatement != null) ? callingStatement.getId() : 999, rs.getResultId(), System.currentTimeMillis(),
                            (queryEndTime - queryStartTime), this.queryTimingUnits, null, LogUtils.findCallingClassAndMethod(new Throwable()),
                            Messages.getString("Protocol.4") + profileQueryToLog));
                }

                if (this.queryNoIndexUsed && this.profileSQL) {
                    eventSink.consumeEvent(new ProfilerEventImpl(ProfilerEvent.TYPE_SLOW_QUERY, "", catalog, this.connection.getId(),
                            (callingStatement != null) ? callingStatement.getId() : 999, rs.getResultId(), System.currentTimeMillis(),
                            (queryEndTime - queryStartTime), this.queryTimingUnits, null, LogUtils.findCallingClassAndMethod(new Throwable()),
                            Messages.getString("Protocol.5") + profileQueryToLog));
                }

                if (this.serverQueryWasSlow && this.profileSQL) {
                    eventSink.consumeEvent(new ProfilerEventImpl(ProfilerEvent.TYPE_SLOW_QUERY, "", catalog, this.connection.getId(),
                            (callingStatement != null) ? callingStatement.getId() : 999, rs.getResultId(), System.currentTimeMillis(),
                            (queryEndTime - queryStartTime), this.queryTimingUnits, null, LogUtils.findCallingClassAndMethod(new Throwable()),
                            Messages.getString("Protocol.ServerSlowQuery") + profileQueryToLog));
                }
            }

            if (this.profileSQL) {
                fetchEndTime = getCurrentTimeNanosOrMillis();

                ProfilerEventHandler eventSink = getProfilerEventHandlerInstanceFunction.apply();

                eventSink.consumeEvent(new ProfilerEventImpl(ProfilerEvent.TYPE_QUERY, "", catalog, this.connection.getId(),
                        (callingStatement != null) ? callingStatement.getId() : 999, rs.getResultId(), System.currentTimeMillis(),
                        (queryEndTime - queryStartTime), this.queryTimingUnits, null, LogUtils.findCallingClassAndMethod(new Throwable()), profileQueryToLog));

                eventSink.consumeEvent(new ProfilerEventImpl(ProfilerEvent.TYPE_FETCH, "", catalog, this.connection.getId(),
                        (callingStatement != null) ? callingStatement.getId() : 999, rs.getResultId(), System.currentTimeMillis(),
                        (fetchEndTime - fetchBeginTime), this.queryTimingUnits, null, LogUtils.findCallingClassAndMethod(new Throwable()), null));
            }

            if (this.hadWarnings) {
                scanForAndThrowDataTruncation();
            }

            if (this.statementInterceptors != null) {
                T interceptedResults = invokeStatementInterceptorsPost(query, callingStatement, rs, false, null);

                if (interceptedResults != null) {
                    rs = interceptedResults;
                }
            }

            return rs;
        } catch (SQLException | CJException sqlEx) {
            if (this.statementInterceptors != null) {
                invokeStatementInterceptorsPost(query, callingStatement, null, false, sqlEx); // we don't do anything with the result set in this case
            }

            if (callingStatement != null) {
                synchronized (callingStatement.cancelTimeoutMutex) {
                    if (callingStatement.wasCancelled) {
                        CJException cause = null;

                        if (callingStatement.wasCancelledByTimeout) {
                            cause = new CJTimeoutException();
                        } else {
                            cause = new OperationCancelledException();
                        }

                        try {
                            callingStatement.resetCancelledState();
                        } catch (SQLException e) {
                            throw ExceptionFactory.createException(e.getMessage(), e);
                        }

                        throw cause;
                    }
                }
            }

            if (sqlEx instanceof CJException) {
                // don't wrap CJException
                throw (CJException) sqlEx;
            }
            throw ExceptionFactory.createException(sqlEx.getMessage(), sqlEx);
        } finally {
            this.statementExecutionDepth--;
        }
    }

    public <T extends Resultset> T invokeStatementInterceptorsPre(String sql, Statement interceptedStatement, boolean forceExecute) {
        T previousResultSet = null;

        for (int i = 0, s = this.statementInterceptors.size(); i < s; i++) {
            StatementInterceptor interceptor = this.statementInterceptors.get(i);

            boolean executeTopLevelOnly = interceptor.executeTopLevelOnly();
            boolean shouldExecute = (executeTopLevelOnly && (this.statementExecutionDepth == 1 || forceExecute)) || (!executeTopLevelOnly);

            if (shouldExecute) {
                String sqlToInterceptor = sql;

                //if (interceptedStatement instanceof PreparedStatement) {
                //  sqlToInterceptor = ((PreparedStatement) interceptedStatement)
                //          .asSql();
                //}

                try {
                    T interceptedResultSet = interceptor.preProcess(sqlToInterceptor, interceptedStatement);

                    if (interceptedResultSet != null) {
                        previousResultSet = interceptedResultSet;
                    }
                } catch (SQLException ex) {
                    throw ExceptionFactory.createException(ex.getMessage(), ex);
                }
            }
        }

        return previousResultSet;
    }

    public <T extends Resultset> T invokeStatementInterceptorsPost(String sql, Statement interceptedStatement, T originalResultSet, boolean forceExecute,
            Exception statementException) {

        for (int i = 0, s = this.statementInterceptors.size(); i < s; i++) {
            StatementInterceptor interceptor = this.statementInterceptors.get(i);

            boolean executeTopLevelOnly = interceptor.executeTopLevelOnly();
            boolean shouldExecute = (executeTopLevelOnly && (this.statementExecutionDepth == 1 || forceExecute)) || (!executeTopLevelOnly);

            if (shouldExecute) {
                String sqlToInterceptor = sql;

                try {
                    T interceptedResultSet = interceptor.postProcess(sqlToInterceptor, interceptedStatement, originalResultSet, this.getWarningCount(),
                            this.queryNoIndexUsed, this.queryBadIndexUsed, statementException);

                    if (interceptedResultSet != null) {
                        originalResultSet = interceptedResultSet;
                    }
                } catch (SQLException ex) {
                    throw ExceptionFactory.createException(ex.getMessage(), ex);
                }
            }
        }

        return originalResultSet;
    }

    public long getCurrentTimeNanosOrMillis() {
        if (this.useNanosForElapsedTime) {
            return TimeUtil.getCurrentTimeNanosOrMillis();
        }

        return System.currentTimeMillis();
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
     * @param querySQL
     * @param truncatedQuery
     * 
     */
    public void explainSlowQuery(byte[] querySQL, String truncatedQuery) {
        if (StringUtils.startsWithIgnoreCaseAndWs(truncatedQuery, EXPLAINABLE_STATEMENT)
                || (versionMeetsMinimum(5, 6, 3) && StringUtils.startsWithIgnoreCaseAndWs(truncatedQuery, EXPLAINABLE_STATEMENT_EXTENSION) != -1)) {

            PreparedStatement stmt = null;
            java.sql.ResultSet rs = null;

            try {
                stmt = (PreparedStatement) ((JdbcConnection) this.connection).clientPrepareStatement("EXPLAIN ?");
                stmt.setBytesNoEscapeNoQuotes(1, querySQL);
                rs = stmt.executeQuery();

                StringBuilder explainResults = new StringBuilder(Messages.getString("Protocol.6"));
                explainResults.append(truncatedQuery);
                explainResults.append(Messages.getString("Protocol.7"));

                ResultSetUtil.appendResultSetSlashGStyle(explainResults, rs);

                this.log.logWarn(explainResults.toString());
            } catch (SQLException | CJException sqlEx) {
            } catch (Exception ex) {
                throw ExceptionFactory.createException(ex.getMessage(), ex, getExceptionInterceptor());
            } finally {
                try {
                    if (rs != null) {
                        rs.close();
                    }

                    if (stmt != null) {
                        stmt.close();
                    }
                } catch (SQLException ex) {
                    throw ExceptionFactory.createException(ex.getMessage(), ex);
                }
            }
        }
    }

    /**
     * Reads and discards a single MySQL packet from the input stream.
     * 
     * @throws CJException
     *             if the network fails while skipping the
     *             packet.
     */
    public final void skipPacket() {
        try {

            int packetLength = this.packetReader.readHeader().getPacketLength();

            this.socketConnection.getMysqlInput().skipFully(packetLength);

        } catch (IOException ioEx) {
            throw ExceptionFactory.createCommunicationsException(this.propertySet, this.serverSession, this.getPacketSentTimeHolder().getLastPacketSentTime(),
                    this.getPacketReceivedTimeHolder().getLastPacketReceivedTime(), ioEx, getExceptionInterceptor());
        } catch (OutOfMemoryError oom) {
            // TODO: we can't cleanly handle OOM and we shouldn't even try. we should remove ALL of these except for the one allocating the large buffer for sending a file (iirc)
            try {
                ((JdbcConnection) this.connection).realClose(false, false, true, oom);
            } catch (Exception ex) {
            }
            throw oom;
        }
    }

    /**
     * Log-off of the MySQL server and close the socket.
     * 
     */
    public final void quit() {
        try {
            // we're not going to read the response, fixes BUG#56979 Improper connection closing logic leads to TIME_WAIT sockets on server

            try {
                if (!this.socketConnection.getMysqlSocket().isClosed()) {
                    try {
                        this.socketConnection.getMysqlSocket().shutdownInput();
                    } catch (UnsupportedOperationException ex) {
                        // ignore, some sockets do not support this method
                    }
                }
            } catch (IOException ioEx) {
                this.log.logWarn("Caught while disconnecting...", ioEx);
            }

            PacketPayload packet = new Buffer(6);
            this.packetSequence = -1;
            packet.writeInteger(IntegerDataType.INT1, MysqlaConstants.COM_QUIT);
            send(packet, packet.getPosition());
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
    public PacketPayload getSharedSendPacket() {
        if (this.sharedSendPacket == null) {
            this.sharedSendPacket = new Buffer(INITIAL_PACKET_SIZE);
        }
        this.sharedSendPacket.setPosition(0);

        return this.sharedSendPacket;
    }

    private void calculateSlowQueryThreshold() {
        this.slowQueryThreshold = this.propertySet.getIntegerReadableProperty(PropertyDefinitions.PNAME_slowQueryThresholdMillis).getValue();

        if (this.propertySet.getBooleanReadableProperty(PropertyDefinitions.PNAME_useNanosForElapsedTime).getValue()) {
            long nanosThreshold = this.propertySet.getLongReadableProperty(PropertyDefinitions.PNAME_slowQueryThresholdNanos).getValue();

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
     * @param password
     * @param database
     * 
     */
    public void changeUser(String user, String password, String database) {
        this.packetSequence = -1;

        this.authProvider.changeUser(this.serverSession, user, password, database);
    }

    /**
     * Determines if the database charset is the same as the platform charset
     */
    public void checkForCharsetMismatch() {
        String characterEncoding = this.propertySet.getStringReadableProperty(PropertyDefinitions.PNAME_characterEncoding).getValue();
        if (characterEncoding != null) {
            String encodingToCheck = jvmPlatformCharset;

            if (encodingToCheck == null) {
                encodingToCheck = Constants.PLATFORM_ENCODING;
            }

            if (encodingToCheck == null) {
                this.platformDbCharsetMatches = false;
            } else {
                this.platformDbCharsetMatches = encodingToCheck.equals(characterEncoding);
            }
        }
    }

    public void setServerSlowQueryFlags() {
        ServerSession state = this.serverSession;
        this.queryBadIndexUsed = state.noGoodIndexUsed();
        this.queryNoIndexUsed = state.noIndexUsed();
        this.serverQueryWasSlow = state.queryWasSlow();
    }

    protected boolean useNanosForElapsedTime() {
        return this.useNanosForElapsedTime;
    }

    public long getSlowQueryThreshold() {
        return this.slowQueryThreshold;
    }

    public String getQueryTimingUnits() {
        return this.queryTimingUnits;
    }

    public int getCommandCount() {
        return this.commandCount;
    }

    public void setStatementInterceptors(List<StatementInterceptor> statementInterceptors) {
        this.statementInterceptors = statementInterceptors.isEmpty() ? null : statementInterceptors;
    }

    public List<StatementInterceptor> getStatementInterceptors() {
        return this.statementInterceptors;
    }

    public void setSocketTimeout(int milliseconds) {
        try {
            this.socketConnection.getMysqlSocket().setSoTimeout(milliseconds);
        } catch (SocketException e) {
            throw ExceptionFactory.createException(WrongArgumentException.class, Messages.getString("Protocol.8"), e, getExceptionInterceptor());
        }
    }

    public void releaseResources() {
        if (this.compressedPacketSender != null) {
            this.compressedPacketSender.stop();
        }
    }

    public void connect(String user, String password, String database) {
        // session creation & initialization happens here

        beforeHandshake();

        this.authProvider.connect(this.serverSession, user, password, database);

    }

    @Override
    public JdbcConnection getConnection() {
        return (JdbcConnection) this.connection;
    }

    public void setConnection(JdbcConnection connection) {
        this.connection = connection;
    }

    protected boolean isDataAvailable() {
        try {
            return this.socketConnection.getMysqlInput().available() > 0;
        } catch (IOException ioEx) {
            throw ExceptionFactory.createCommunicationsException(this.propertySet, this.serverSession, this.getPacketSentTimeHolder().getLastPacketSentTime(),
                    this.getPacketReceivedTimeHolder().getLastPacketReceivedTime(), ioEx, getExceptionInterceptor());
        }
    }

    public PacketPayload getReusablePacket() {
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

    public boolean doesPlatformDbCharsetMatches() {
        return this.platformDbCharsetMatches;
    }

    public String getPasswordCharacterEncoding() {
        String encoding;
        if ((encoding = this.propertySet.getStringReadableProperty(PropertyDefinitions.PNAME_passwordCharacterEncoding).getStringValue()) != null) {
            return encoding;
        }
        if ((encoding = this.propertySet.getStringReadableProperty(PropertyDefinitions.PNAME_characterEncoding).getValue()) != null) {
            return encoding;
        }
        return "UTF-8";

    }

    public boolean versionMeetsMinimum(int major, int minor, int subminor) {
        return this.serverSession.getServerVersion().meetsMinimum(new ServerVersion(major, minor, subminor));
    }

    public static MysqlType findMysqlType(PropertySet propertySet, int mysqlTypeId, short colFlag, long length, LazyString tableName,
            LazyString originalTableName, int collationIndex, String encoding) {

        boolean isUnsigned = ((colFlag & MysqlType.FIELD_FLAG_UNSIGNED) > 0);
        boolean isFromFunction = originalTableName.length() == 0;
        boolean isBinary = ((colFlag & MysqlType.FIELD_FLAG_BINARY) > 0);
        /**
         * Is this field owned by a server-created temporary table?
         */
        boolean isImplicitTemporaryTable = tableName.length() > 0 && tableName.toString().startsWith("#sql_");

        boolean isOpaqueBinary = (isBinary && collationIndex == CharsetMapping.MYSQL_COLLATION_INDEX_binary
                && (mysqlTypeId == MysqlaConstants.FIELD_TYPE_STRING || mysqlTypeId == MysqlaConstants.FIELD_TYPE_VAR_STRING
                        || mysqlTypeId == MysqlaConstants.FIELD_TYPE_VARCHAR)) ?
                                // queries resolved by temp tables also have this 'signature', check for that
                                !isImplicitTemporaryTable : "binary".equalsIgnoreCase(encoding);

        switch (mysqlTypeId) {
            case MysqlaConstants.FIELD_TYPE_DECIMAL:
            case MysqlaConstants.FIELD_TYPE_NEWDECIMAL:
                return isUnsigned ? MysqlType.DECIMAL_UNSIGNED : MysqlType.DECIMAL;

            case MysqlaConstants.FIELD_TYPE_TINY:
                // Adjust for pseudo-boolean
                if (length == 1) {
                    if (propertySet.getBooleanReadableProperty(PropertyDefinitions.PNAME_transformedBitIsBoolean).getValue()) {
                        return MysqlType.BOOLEAN;
                    } else if (propertySet.getBooleanReadableProperty(PropertyDefinitions.PNAME_tinyInt1isBit).getValue()) {
                        return MysqlType.BIT;
                    }
                }
                return isUnsigned ? MysqlType.TINYINT_UNSIGNED : MysqlType.TINYINT;

            case MysqlaConstants.FIELD_TYPE_SHORT:
                return isUnsigned ? MysqlType.SMALLINT_UNSIGNED : MysqlType.SMALLINT;

            case MysqlaConstants.FIELD_TYPE_LONG:
                return isUnsigned ? MysqlType.INT_UNSIGNED : MysqlType.INT;

            case MysqlaConstants.FIELD_TYPE_FLOAT:
                return isUnsigned ? MysqlType.FLOAT_UNSIGNED : MysqlType.FLOAT;

            case MysqlaConstants.FIELD_TYPE_DOUBLE:
                return isUnsigned ? MysqlType.DOUBLE_UNSIGNED : MysqlType.DOUBLE;

            case MysqlaConstants.FIELD_TYPE_NULL:
                return MysqlType.NULL;

            case MysqlaConstants.FIELD_TYPE_TIMESTAMP:
                return MysqlType.TIMESTAMP;

            case MysqlaConstants.FIELD_TYPE_LONGLONG:
                return isUnsigned ? MysqlType.BIGINT_UNSIGNED : MysqlType.BIGINT;

            case MysqlaConstants.FIELD_TYPE_INT24:
                return isUnsigned ? MysqlType.MEDIUMINT_UNSIGNED : MysqlType.MEDIUMINT;

            case MysqlaConstants.FIELD_TYPE_DATE:
                return MysqlType.DATE;

            case MysqlaConstants.FIELD_TYPE_TIME:
                return MysqlType.TIME;

            case MysqlaConstants.FIELD_TYPE_DATETIME:
                return MysqlType.DATETIME;

            case MysqlaConstants.FIELD_TYPE_YEAR:
                return MysqlType.YEAR;

            case MysqlaConstants.FIELD_TYPE_VARCHAR:
            case MysqlaConstants.FIELD_TYPE_VAR_STRING:

                if (isOpaqueBinary
                        && !(isFromFunction && propertySet.getBooleanReadableProperty(PropertyDefinitions.PNAME_functionsNeverReturnBlobs).getValue())) {
                    return MysqlType.VARBINARY;
                }

                return MysqlType.VARCHAR;

            case MysqlaConstants.FIELD_TYPE_BIT:
                //if (length > 1) {
                // we need to pretend this is a full binary blob
                //this.colFlag |= MysqlType.FIELD_FLAG_BINARY;
                //this.colFlag |= MysqlType.FIELD_FLAG_BLOB;
                //return MysqlType.VARBINARY;
                //}
                return MysqlType.BIT;

            case MysqlaConstants.FIELD_TYPE_JSON:
                return MysqlType.JSON;

            case MysqlaConstants.FIELD_TYPE_ENUM:
                return MysqlType.ENUM;

            case MysqlaConstants.FIELD_TYPE_SET:
                return MysqlType.SET;

            case MysqlaConstants.FIELD_TYPE_TINY_BLOB:
                if (!isBinary || collationIndex != CharsetMapping.MYSQL_COLLATION_INDEX_binary
                        || propertySet.getBooleanReadableProperty(PropertyDefinitions.PNAME_blobsAreStrings).getValue()
                        || isFromFunction && (propertySet.getBooleanReadableProperty(PropertyDefinitions.PNAME_functionsNeverReturnBlobs).getValue())) {
                    return MysqlType.TINYTEXT;
                }
                return MysqlType.TINYBLOB;

            case MysqlaConstants.FIELD_TYPE_MEDIUM_BLOB:
                if (!isBinary || collationIndex != CharsetMapping.MYSQL_COLLATION_INDEX_binary
                        || propertySet.getBooleanReadableProperty(PropertyDefinitions.PNAME_blobsAreStrings).getValue()
                        || isFromFunction && (propertySet.getBooleanReadableProperty(PropertyDefinitions.PNAME_functionsNeverReturnBlobs).getValue())) {
                    return MysqlType.MEDIUMTEXT;
                }
                return MysqlType.MEDIUMBLOB;

            case MysqlaConstants.FIELD_TYPE_LONG_BLOB:
                if (!isBinary || collationIndex != CharsetMapping.MYSQL_COLLATION_INDEX_binary
                        || propertySet.getBooleanReadableProperty(PropertyDefinitions.PNAME_blobsAreStrings).getValue()
                        || isFromFunction && (propertySet.getBooleanReadableProperty(PropertyDefinitions.PNAME_functionsNeverReturnBlobs).getValue())) {
                    return MysqlType.LONGTEXT;
                }
                return MysqlType.LONGBLOB;

            case MysqlaConstants.FIELD_TYPE_BLOB:
                // Sometimes MySQL uses this protocol-level type for all possible BLOB variants,
                // we can divine what the actual type is by the length reported

                int newMysqlTypeId = mysqlTypeId;

                // fixing initial type according to length
                if (length <= MysqlType.TINYBLOB.getPrecision()) {
                    newMysqlTypeId = MysqlaConstants.FIELD_TYPE_TINY_BLOB;

                } else if (length <= MysqlType.BLOB.getPrecision()) {
                    if (!isBinary || collationIndex != CharsetMapping.MYSQL_COLLATION_INDEX_binary
                            || propertySet.getBooleanReadableProperty(PropertyDefinitions.PNAME_blobsAreStrings).getValue()
                            || isFromFunction && (propertySet.getBooleanReadableProperty(PropertyDefinitions.PNAME_functionsNeverReturnBlobs).getValue())) {
                        newMysqlTypeId = MysqlaConstants.FIELD_TYPE_VARCHAR;
                        return MysqlType.TEXT;
                    }
                    return MysqlType.BLOB;

                } else if (length <= MysqlType.MEDIUMBLOB.getPrecision()) {
                    newMysqlTypeId = MysqlaConstants.FIELD_TYPE_MEDIUM_BLOB;
                } else {
                    newMysqlTypeId = MysqlaConstants.FIELD_TYPE_LONG_BLOB;
                }

                // call this method again with correct this.mysqlType set
                return findMysqlType(propertySet, newMysqlTypeId, colFlag, length, tableName, originalTableName, collationIndex, encoding);

            case MysqlaConstants.FIELD_TYPE_STRING:
                if (isOpaqueBinary && !propertySet.getBooleanReadableProperty(PropertyDefinitions.PNAME_blobsAreStrings).getValue()) {
                    return MysqlType.BINARY;
                }
                return MysqlType.CHAR;

            case MysqlaConstants.FIELD_TYPE_GEOMETRY:
                return MysqlType.GEOMETRY;

            default:
                return MysqlType.UNKNOWN;
        }
    }

    /*
     * Reading results
     */

    @Override
    public <T extends ProtocolEntity> T read(Class<T> requiredClass, ProtocolEntityFactory<T> protocolEntityFactory) throws IOException {
        @SuppressWarnings("unchecked")
        ProtocolEntityReader<T> sr = (ProtocolEntityReader<T>) this.PROTOCOL_ENTITY_CLASS_TO_TEXT_READER.get(requiredClass);
        if (sr == null) {
            throw ExceptionFactory.createException(FeatureNotAvailableException.class, "ProtocolEntityReader isn't available for class " + requiredClass);
        }
        return sr.read(protocolEntityFactory);
    }

    @Override
    public <T extends ProtocolEntity> T read(Class<Resultset> requiredClass, int maxRows, boolean streamResults, PacketPayload resultPacket,
            boolean isBinaryEncoded, ColumnDefinition metadata, ProtocolEntityFactory<T> protocolEntityFactory) throws IOException {
        @SuppressWarnings("unchecked")
        ProtocolEntityReader<T> sr = isBinaryEncoded ? (ProtocolEntityReader<T>) this.PROTOCOL_ENTITY_CLASS_TO_BINARY_READER.get(requiredClass)
                : (ProtocolEntityReader<T>) this.PROTOCOL_ENTITY_CLASS_TO_TEXT_READER.get(requiredClass);
        if (sr == null) {
            throw ExceptionFactory.createException(FeatureNotAvailableException.class, "ProtocolEntityReader isn't available for class " + requiredClass);
        }
        return sr.read(maxRows, streamResults, resultPacket, metadata, protocolEntityFactory);
    }

    /**
     * Read next result set from multi-result chain.
     * 
     * @param currentProtocolEntity
     * @param maxRows
     * @param streamResults
     * @param isBinaryEncoded
     * @param resultSetFactory
     * @return
     * @throws SQLException
     */
    public <T extends ProtocolEntity> T readNextResultset(T currentProtocolEntity, int maxRows, boolean streamResults, boolean isBinaryEncoded,
            ProtocolEntityFactory<T> resultSetFactory) throws IOException {

        T result = null;
        if (Resultset.class.isAssignableFrom(currentProtocolEntity.getClass()) && this.serverSession.useMultiResults()) {
            if (this.serverSession.hasMoreResults()) {

                T currentResultSet = currentProtocolEntity;
                T newResultSet;
                do {
                    PacketPayload fieldPacket = checkErrorPacket();
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

    public <T extends Resultset> T readAllResults(int maxRows, boolean streamResults, PacketPayload resultPacket, boolean isBinaryEncoded,
            ColumnDefinition metadata, ProtocolEntityFactory<T> resultSetFactory) throws IOException {

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
        reclaimLargeReusablePacket();
        return topLevelResultSet;
    }

    @SuppressWarnings("unchecked")
    public final <T> T readServerStatusForResultSets(PacketPayload rowPacket, boolean saveOldStatus) {
        T result = null;
        if (rowPacket.isEOFPacket()) {
            // read EOF packet
            rowPacket.readInteger(IntegerDataType.INT1); // skips the 'last packet' flag (packet signature)
            this.warningCount = (int) rowPacket.readInteger(IntegerDataType.INT2);
            if (this.warningCount > 0) {
                this.hadWarnings = true; // this is a 'latch', it's reset by sendCommand()
            }

            this.serverSession.setStatusFlags((int) rowPacket.readInteger(IntegerDataType.INT2), saveOldStatus);
            checkTransactionState();
        } else {
            // read OK packet
            OkPacket ok = OkPacket.parse(rowPacket, ((JdbcConnection) this.connection).isReadInfoMsgEnabled(), this.serverSession.getErrorMessageEncoding());
            result = (T) ok;

            this.serverSession.setStatusFlags(ok.getStatusFlags(), saveOldStatus);
            checkTransactionState();

            this.warningCount = ok.getWarningCount();
            if (this.warningCount > 0) {
                this.hadWarnings = true; // this is a 'latch', it's reset by sendCommand()
            }
        }
        setServerSlowQueryFlags();
        return result;
    }

    public InputStream getLocalInfileInputStream() {
        return this.localInfileInputStream;
    }

    public void setLocalInfileInputStream(InputStream stream) {
        this.localInfileInputStream = stream;
    }

    /**
     * Reads and sends a file to the server for LOAD DATA LOCAL INFILE
     * 
     * @param fileName
     *            the file name to send.
     * 
     */
    public final PacketPayload sendFileToServer(String fileName) {

        PacketPayload filePacket = (this.loadFileBufRef == null) ? null : this.loadFileBufRef.get();

        int bigPacketLength = Math.min(this.maxAllowedPacket.getValue() - (MysqlaConstants.HEADER_LENGTH * 3),
                alignPacketSize(this.maxAllowedPacket.getValue() - 16, 4096) - (MysqlaConstants.HEADER_LENGTH * 3));

        int oneMeg = 1024 * 1024;

        int smallerPacketSizeAligned = Math.min(oneMeg - (MysqlaConstants.HEADER_LENGTH * 3),
                alignPacketSize(oneMeg - 16, 4096) - (MysqlaConstants.HEADER_LENGTH * 3));

        int packetLength = Math.min(smallerPacketSizeAligned, bigPacketLength);

        if (filePacket == null) {
            try {
                filePacket = new Buffer(packetLength);
                this.loadFileBufRef = new SoftReference<PacketPayload>(filePacket);
            } catch (OutOfMemoryError oom) {
                throw ExceptionFactory.createException(Messages.getString("MysqlIO.111", new Object[] { packetLength }),
                        SQLError.SQL_STATE_MEMORY_ALLOCATION_ERROR, 0, false, oom, this.exceptionInterceptor);
            }
        }

        filePacket.setPosition(0);

        byte[] fileBuf = new byte[packetLength];

        BufferedInputStream fileIn = null;

        try {
            if (!this.propertySet.getBooleanReadableProperty(PropertyDefinitions.PNAME_allowLoadLocalInfile).getValue()) {
                throw ExceptionFactory.createException(Messages.getString("MysqlIO.LoadDataLocalNotAllowed"), this.exceptionInterceptor);
            }

            InputStream hookedStream = null;

            hookedStream = getLocalInfileInputStream();

            if (hookedStream != null) {
                fileIn = new BufferedInputStream(hookedStream);
            } else if (!this.propertySet.getBooleanReadableProperty(PropertyDefinitions.PNAME_allowUrlInLocalInfile).getValue()) {
                fileIn = new BufferedInputStream(new FileInputStream(fileName));
            } else {
                // First look for ':'
                if (fileName.indexOf(':') != -1) {
                    try {
                        URL urlFromFileName = new URL(fileName);
                        fileIn = new BufferedInputStream(urlFromFileName.openStream());
                    } catch (MalformedURLException badUrlEx) {
                        // we fall back to trying this as a file input stream
                        fileIn = new BufferedInputStream(new FileInputStream(fileName));
                    }
                } else {
                    fileIn = new BufferedInputStream(new FileInputStream(fileName));
                }
            }

            int bytesRead = 0;

            while ((bytesRead = fileIn.read(fileBuf)) != -1) {
                filePacket.setPosition(0);
                filePacket.writeBytes(StringLengthDataType.STRING_FIXED, fileBuf, 0, bytesRead);
                send(filePacket, filePacket.getPosition());
            }
        } catch (IOException ioEx) {
            StringBuilder messageBuf = new StringBuilder(Messages.getString("MysqlIO.60"));

            boolean isParanoid = this.propertySet.getBooleanReadableProperty(PropertyDefinitions.PNAME_paranoid).getValue();
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
                // file open failed, but server needs one packet
                filePacket.setPosition(0);
                send(filePacket, filePacket.getPosition());
                checkErrorPacket(); // to clear response off of queue
            }
        }

        // send empty packet to mark EOF
        filePacket.setPosition(0);
        send(filePacket, filePacket.getPosition());

        return checkErrorPacket();
    }

    private int alignPacketSize(int a, int l) {
        return ((((a) + (l)) - 1) & ~((l) - 1));
    }

    private ResultsetRows streamingData = null;

    public ResultsetRows getStreamingData() {
        return this.streamingData;
    }

    public void setStreamingData(ResultsetRows streamingData) {
        this.streamingData = streamingData;
    }

    public void checkForOutstandingStreamingData() {
        try {
            if (this.streamingData != null) {
                boolean shouldClobber = this.propertySet.getBooleanReadableProperty(PropertyDefinitions.PNAME_clobberStreamingResults).getValue();

                if (!shouldClobber) {
                    throw SQLError.createSQLException(Messages.getString("MysqlIO.39") + this.streamingData + Messages.getString("MysqlIO.40")
                            + Messages.getString("MysqlIO.41") + Messages.getString("MysqlIO.42"), this.exceptionInterceptor);
                }

                // Close the result set
                this.streamingData.getOwner().closeOwner(false);

                // clear any pending data....
                clearInputStream();
            }
        } catch (SQLException ex) {
            throw ExceptionFactory.createException(ex.getMessage(), ex);
        }
    }

    public void closeStreamer(ResultsetRows streamer) {
        if (this.streamingData == null) {
            throw ExceptionFactory.createException(Messages.getString("MysqlIO.17") + streamer + Messages.getString("MysqlIO.18"), this.exceptionInterceptor);
        }

        if (streamer != this.streamingData) {
            throw ExceptionFactory.createException(Messages.getString("MysqlIO.19") + streamer + Messages.getString("MysqlIO.20")
                    + Messages.getString("MysqlIO.21") + Messages.getString("MysqlIO.22"), this.exceptionInterceptor);
        }

        this.streamingData = null;
    }

    public void scanForAndThrowDataTruncation() throws SQLException {
        if ((this.streamingData == null) && this.propertySet.getBooleanReadableProperty(PropertyDefinitions.PNAME_jdbcCompliantTruncation).getValue()
                && getWarningCount() > 0) {
            ResultSetUtil.convertShowWarningsToSQLWarnings(this.connection, getWarningCount(), true);
        }
    }
}
