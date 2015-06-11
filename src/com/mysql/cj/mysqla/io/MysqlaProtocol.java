/*
  Copyright (c) 2015, Oracle and/or its affiliates. All rights reserved.

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

package com.mysql.cj.mysqla.io;

import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.net.SocketException;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import com.mysql.cj.api.MysqlConnection;
import com.mysql.cj.api.ProfilerEvent;
import com.mysql.cj.api.ProfilerEventHandler;
import com.mysql.cj.api.authentication.AuthenticationProvider;
import com.mysql.cj.api.conf.PropertySet;
import com.mysql.cj.api.conf.ReadableProperty;
import com.mysql.cj.api.io.PacketBuffer;
import com.mysql.cj.api.io.Protocol;
import com.mysql.cj.api.io.ServerSession;
import com.mysql.cj.api.io.SocketConnection;
import com.mysql.cj.core.Constants;
import com.mysql.cj.core.Messages;
import com.mysql.cj.core.ServerVersion;
import com.mysql.cj.core.conf.PropertyDefinitions;
import com.mysql.cj.core.exception.CJCommunicationsException;
import com.mysql.cj.core.exception.CJConnectionFeatureNotAvailableException;
import com.mysql.cj.core.exception.CJException;
import com.mysql.cj.core.exception.CJPacketTooBigException;
import com.mysql.cj.core.exception.CJTimeoutException;
import com.mysql.cj.core.exception.ClosedOnExpiredPasswordException;
import com.mysql.cj.core.exception.DataTruncationException;
import com.mysql.cj.core.exception.ExceptionFactory;
import com.mysql.cj.core.exception.FeatureNotAvailableException;
import com.mysql.cj.core.exception.MysqlErrorNumbers;
import com.mysql.cj.core.exception.OperationCancelledException;
import com.mysql.cj.core.exception.PasswordExpiredException;
import com.mysql.cj.core.exception.UnableToConnectException;
import com.mysql.cj.core.exception.WrongArgumentException;
import com.mysql.cj.core.io.AbstractProtocol;
import com.mysql.cj.core.io.Buffer;
import com.mysql.cj.core.io.CompressedInputStream;
import com.mysql.cj.core.io.CompressedPacketSender;
import com.mysql.cj.core.io.DebugBufferingPacketSender;
import com.mysql.cj.core.io.ExportControlled;
import com.mysql.cj.core.io.ProtocolConstants;
import com.mysql.cj.core.io.SimplePacketSender;
import com.mysql.cj.core.io.TimeTrackingPacketSender;
import com.mysql.cj.core.io.TracingPacketSender;
import com.mysql.cj.core.profiler.ProfilerEventHandlerFactory;
import com.mysql.cj.core.profiler.ProfilerEventImpl;
import com.mysql.cj.core.util.LogUtils;
import com.mysql.cj.core.util.StringUtils;
import com.mysql.cj.core.util.TestUtils;
import com.mysql.cj.mysqla.MysqlaConstants;
import com.mysql.cj.mysqla.MysqlaSession;
import com.mysql.cj.mysqla.authentication.MysqlaAuthenticationProvider;
import com.mysql.jdbc.Field;
import com.mysql.jdbc.MysqlIO;
import com.mysql.jdbc.MysqlJdbcConnection;
import com.mysql.jdbc.PreparedStatement;
import com.mysql.jdbc.ResultSetImpl;
import com.mysql.jdbc.ResultSetInternalMethods;
import com.mysql.jdbc.Statement;
import com.mysql.jdbc.StatementImpl;
import com.mysql.jdbc.exceptions.SQLError;
import com.mysql.jdbc.interceptors.StatementInterceptorV2;
import com.mysql.jdbc.util.ResultSetUtil;
import com.mysql.jdbc.util.TimeUtil;

public class MysqlaProtocol extends AbstractProtocol implements Protocol {

    protected static final int INITIAL_PACKET_SIZE = 1024;
    protected static final int COMP_HEADER_LENGTH = 3;
    /** Max number of bytes to dump when tracing the protocol */
    private final static int MAX_PACKET_DUMP_LENGTH = 1024;
    protected static final int MAX_QUERY_SIZE_TO_EXPLAIN = 1024 * 1024; // don't explain queries above 1MB
    private static final String EXPLAINABLE_STATEMENT = "SELECT";
    private static final String[] EXPLAINABLE_STATEMENT_EXTENSION = new String[] { "INSERT", "UPDATE", "REPLACE", "DELETE" };

    protected MysqlJdbcConnection connection;

    protected MysqlaServerSession serverSession;

    /** Track this to manually shut down. */
    protected CompressedPacketSender compressedPacketSender;

    private Buffer sendPacket = null;
    protected Buffer sharedSendPacket = null;
    /** Use this when reading in rows to avoid thousands of new() calls, because the byte arrays just get copied out of the packet anyway */
    protected Buffer reusablePacket = null;

    protected byte packetSequence = 0;
    protected byte readPacketSequence = -1;
    protected boolean checkPacketSequence = false;
    protected boolean useCompression = false;
    protected byte[] packetHeaderBuf = new byte[4];
    private int maxAllowedPacket = 1024 * 1024;
    private boolean packetSequenceReset = false;

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

    // TODO inject ResultsHandler instead of hardcoded class
    protected MysqlIO resultsHandler = null;

    /**
     * Does the character set of this connection match the character set of the
     * platform
     */
    protected boolean platformDbCharsetMatches = true; // changed once we've connected.

    private int statementExecutionDepth = 0;
    private List<StatementInterceptorV2> statementInterceptors;

    private ReadableProperty<Boolean> maintainTimeStats;
    private ReadableProperty<Integer> maxQuerySizeToLog;

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

    public static MysqlaProtocol getInstance(MysqlConnection conn, SocketConnection socketConnection, PropertySet propertySet) {
        MysqlaProtocol protocol = new MysqlaProtocol();
        protocol.init(conn, propertySet.getIntegerReadableProperty(PropertyDefinitions.PNAME_socketTimeout).getValue(), socketConnection, propertySet);
        return protocol;
    }

    @Override
    public void init(MysqlConnection conn, int socketTimeout, SocketConnection phConnection, PropertySet propertySet) {

        this.connection = (MysqlJdbcConnection) conn;
        this.setPropertySet(propertySet);

        this.socketConnection = phConnection;

        if (this.getPropertySet().getBooleanReadableProperty(PropertyDefinitions.PNAME_enablePacketDebug).getValue()) {
            this.packetDebugRingBuffer = new LinkedList<StringBuilder>();
        }

        this.maintainTimeStats = propertySet.getBooleanReadableProperty(PropertyDefinitions.PNAME_maintainTimeStats);
        this.maxQuerySizeToLog = propertySet.getIntegerReadableProperty(PropertyDefinitions.PNAME_maxQuerySizeToLog);

        this.traceProtocol = propertySet.getBooleanReadableProperty(PropertyDefinitions.PNAME_traceProtocol).getValue();

        this.useAutoSlowLog = propertySet.getBooleanReadableProperty(PropertyDefinitions.PNAME_autoSlowLog).getValue();

        this.logSlowQueries = propertySet.getBooleanReadableProperty(PropertyDefinitions.PNAME_logSlowQueries).getValue();

        this.reusablePacket = new Buffer(INITIAL_PACKET_SIZE);
        this.sendPacket = new Buffer(INITIAL_PACKET_SIZE);

        this.exceptionInterceptor = this.socketConnection.getExceptionInterceptor();

        // socket was here

        this.packetSender = new SimplePacketSender(this.socketConnection.getMysqlOutput());

        this.profileSQL = propertySet.getBooleanReadableProperty(PropertyDefinitions.PNAME_profileSQL).getValue();
        this.autoGenerateTestcaseScript = propertySet.getBooleanReadableProperty(PropertyDefinitions.PNAME_autoGenerateTestcaseScript).getValue();

        this.needToGrabQueryFromPacket = (this.profileSQL || this.logSlowQueries || this.autoGenerateTestcaseScript);

        if (propertySet.getBooleanReadableProperty(PropertyDefinitions.PNAME_useNanosForElapsedTime).getValue() && TimeUtil.nanoTimeAvailable()) {
            this.useNanosForElapsedTime = true;

            this.queryTimingUnits = Messages.getString("Nanoseconds");
        } else {
            this.queryTimingUnits = Messages.getString("Milliseconds");
        }

        if (propertySet.getBooleanReadableProperty(PropertyDefinitions.PNAME_logSlowQueries).getValue()) {
            calculateSlowQueryThreshold();
        }

        this.authProvider = new MysqlaAuthenticationProvider();
        this.authProvider.init(conn, this, this.getPropertySet(), this.socketConnection.getExceptionInterceptor());

        // TODO Initialize ResultsHandler properly
        this.resultsHandler = new MysqlIO(this, this.getPropertySet(), this.connection);
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
            throw new CJConnectionFeatureNotAvailableException(this.getPropertySet(), this.serverSession, this.getPacketSentTimeHolder()
                    .getLastPacketSentTime(), null);
        }

        long clientParam = this.serverSession.getClientParam();
        clientParam |= MysqlaServerSession.CLIENT_SSL;
        this.serverSession.setClientParam(clientParam);

        Buffer packet = new Buffer(packLength);
        packet.setPosition(0);

        packet.writeLong(clientParam);
        packet.writeLong(ProtocolConstants.MAX_PACKET_SIZE);
        packet.writeByte(AuthenticationProvider.getCharsetForHandshake(this.authProvider.getEncodingForHandshake(), this.serverSession.getCapabilities()
                .getServerVersion()));
        packet.writeBytesNoNull(new byte[23]);  // Set of bytes reserved for future use.

        send(packet, packet.getPosition());

        try {
            ExportControlled.transformSocketToSSLSocket(this.socketConnection);
        } catch (FeatureNotAvailableException nae) {
            throw new CJConnectionFeatureNotAvailableException(this.getPropertySet(), this.serverSession, this.getPacketSentTimeHolder()
                    .getLastPacketSentTime(), nae);
        } catch (IOException ioEx) {
            throw ExceptionFactory.createCommunicationsException(this.getConnection().getPropertySet(), this.serverSession, this.getPacketSentTimeHolder()
                    .getLastPacketSentTime(), this.lastPacketReceivedTimeMs, ioEx, getExceptionInterceptor());
        }
        // output stream is replaced, build new packet sender
        this.packetSender = new SimplePacketSender(this.socketConnection.getMysqlOutput());
    }

    // TODO: find a better place for method?
    @Override
    public void rejectConnection(String message) {
        try {
            this.connection.close();
        } catch (SQLException e) {
            throw ExceptionFactory.createException(e.getMessage(), e, getExceptionInterceptor());
        }
        this.socketConnection.forceClose();
        throw ExceptionFactory.createException(UnableToConnectException.class, message, getExceptionInterceptor());
    }

    @Override
    public void rejectProtocol(Buffer buf) {
        try {
            this.socketConnection.getMysqlSocket().close();
        } catch (Exception e) {
            // ignore
        }

        int errno = 2000;

        errno = buf.readInt();

        String serverErrorMessage = "";
        try {
            serverErrorMessage = buf.readString("ASCII", getExceptionInterceptor());
        } catch (Exception e) {
            //
        }

        StringBuilder errorBuf = new StringBuilder(Messages.getString("MysqlIO.10"));
        errorBuf.append(serverErrorMessage);
        errorBuf.append("\"");

        String xOpen = SQLError.mysqlToSqlState(errno);

        throw ExceptionFactory.createException(SQLError.get(xOpen) + ", " + errorBuf.toString(), xOpen, errno, false, null, getExceptionInterceptor());
    }

    @Override
    public void beforeHandshake() {
        // Reset packet sequences
        this.checkPacketSequence = false;
        this.readPacketSequence = 0;

        // Create session state
        this.serverSession = new MysqlaServerSession();

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
                && !(this.socketConnection.getMysqlInput() instanceof CompressedInputStream)) {
            this.useCompression = true;
            this.socketConnection.setMysqlInput(new CompressedInputStream(this.connection, this.socketConnection.getMysqlInput(), pset
                    .getBooleanReadableProperty(PropertyDefinitions.PNAME_traceProtocol)));
            this.compressedPacketSender = new CompressedPacketSender(this.socketConnection.getMysqlOutput());
            this.packetSender = this.compressedPacketSender;
        }

        decoratePacketSender();

        try {
            this.socketConnection.setMysqlSocket(this.socketConnection.getSocketFactory().afterHandshake());
        } catch (IOException ioEx) {
            throw ExceptionFactory.createCommunicationsException(this.getPropertySet(), this.serverSession, this.getPacketSentTimeHolder()
                    .getLastPacketSentTime(), this.lastPacketReceivedTimeMs, ioEx, getExceptionInterceptor());
        }
    }

    public MysqlaCapabilities readServerCapabilities() {
        // Read the first packet
        Buffer buf = readPacket();
        MysqlaCapabilities serverCapabilities = new MysqlaCapabilities();
        serverCapabilities.setInitialHandshakePacket(buf, getExceptionInterceptor());

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
                throw ExceptionFactory.createCommunicationsException(this.getPropertySet(), this.serverSession, this.getPacketSentTimeHolder()
                        .getLastPacketSentTime(), this.lastPacketReceivedTimeMs, ex, getExceptionInterceptor());
            }
        }
    }

    @Override
    public final Buffer readPacket() {
        try {

            int lengthRead = readFully(this.socketConnection.getMysqlInput(), this.packetHeaderBuf, 0, 4);

            if (lengthRead < 4) {
                this.socketConnection.forceClose();
                throw new IOException(Messages.getString("MysqlIO.1"));
            }

            int packetLength = (this.packetHeaderBuf[0] & 0xff) + ((this.packetHeaderBuf[1] & 0xff) << 8) + ((this.packetHeaderBuf[2] & 0xff) << 16);

            if (packetLength > this.maxAllowedPacket) {
                throw new CJPacketTooBigException(packetLength, this.maxAllowedPacket);
            }

            if (this.traceProtocol) {
                StringBuilder traceMessageBuf = new StringBuilder();

                traceMessageBuf.append(Messages.getString("MysqlIO.2"));
                traceMessageBuf.append(packetLength);
                traceMessageBuf.append(Messages.getString("MysqlIO.3"));
                traceMessageBuf.append(StringUtils.dumpAsHex(this.packetHeaderBuf, 4));

                this.connection.getLog().logTrace(traceMessageBuf.toString());
            }

            byte multiPacketSeq = this.packetHeaderBuf[3];

            if (!this.packetSequenceReset) {
                if (this.enablePacketDebug && this.checkPacketSequence) {
                    checkPacketSequencing(multiPacketSeq);
                }
            } else {
                this.packetSequenceReset = false;
            }

            this.readPacketSequence = multiPacketSeq;

            // Read data
            byte[] buffer = new byte[packetLength + 1];
            int numBytesRead = readFully(this.socketConnection.getMysqlInput(), buffer, 0, packetLength);

            if (numBytesRead != packetLength) {
                throw new IOException(Messages.getString("MysqlIO.104", new Object[] { packetLength, numBytesRead }));
            }

            buffer[packetLength] = 0;

            Buffer packet = new Buffer(buffer);
            packet.setBufLength(packetLength + 1);

            if (this.traceProtocol) {
                StringBuilder traceMessageBuf = new StringBuilder();

                traceMessageBuf.append(Messages.getString("MysqlIO.4"));
                traceMessageBuf.append(getPacketDumpToLog(packet, packetLength));

                this.connection.getLog().logTrace(traceMessageBuf.toString());
            }

            if (this.enablePacketDebug) {
                enqueuePacketForDebugging(false, false, 0, this.packetHeaderBuf, packet);
            }

            if (this.maintainTimeStats.getValue()) {
                this.lastPacketReceivedTimeMs = System.currentTimeMillis();
            }

            return packet;
        } catch (IOException ioEx) {
            throw ExceptionFactory.createCommunicationsException(this.propertySet, this.serverSession, this.getPacketSentTimeHolder().getLastPacketSentTime(),
                    this.lastPacketReceivedTimeMs, ioEx, getExceptionInterceptor());
        } catch (CJException ex) {
            throw ex;
        } catch (Exception ex) {
            throw ExceptionFactory.createException(ex.getMessage(), ex, getExceptionInterceptor());
        } catch (OutOfMemoryError oom) {
            try {
                this.connection.realClose(false, false, true, oom);
            } catch (Exception ex) {
            }
            throw oom;
        }
    }

    @Override
    public Buffer readNextPacket() {
        // read packet from server and check if it's an ERROR packet
        Buffer packet = checkErrorPacket();
        this.packetSequence++;
        return packet;
    }

    /**
     * @param packet
     * @param packetLen
     *            length of header + payload
     */
    @Override
    public final void send(PacketBuffer packet, int packetLen) {
        try {
            if (this.maxAllowedPacket > 0 && packetLen > this.maxAllowedPacket) {
                throw new CJPacketTooBigException(packetLen, this.maxAllowedPacket);
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
            throw ExceptionFactory.createCommunicationsException(this.getPropertySet(), this.serverSession, this.getPacketSentTimeHolder()
                    .getLastPacketSentTime(), this.lastPacketReceivedTimeMs, ioEx, getExceptionInterceptor());
        }
    }

    @Override
    public final Buffer sendCommand(int command, String extraData, Buffer queryPacket, boolean skipCheck, String extraDataCharEncoding, int timeoutMillis) {
        this.commandCount++;

        //
        // We cache these locally, per-command, as the checks for them are in very 'hot' sections of the I/O code and we save 10-15% in overall performance by
        // doing this...
        //
        this.enablePacketDebug = this.getPropertySet().getBooleanReadableProperty(PropertyDefinitions.PNAME_enablePacketDebug).getValue();
        this.readPacketSequence = 0;

        int oldTimeout = 0;

        if (timeoutMillis != 0) {
            try {
                oldTimeout = this.socketConnection.getMysqlSocket().getSoTimeout();
                this.socketConnection.getMysqlSocket().setSoTimeout(timeoutMillis);
            } catch (SocketException e) {
                throw ExceptionFactory.createCommunicationsException(this.propertySet, this.serverSession, this.getPacketSentTimeHolder()
                        .getLastPacketSentTime(), this.lastPacketReceivedTimeMs, e, getExceptionInterceptor());
            }
        }

        try {

            this.resultsHandler.checkForOutstandingStreamingData();

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
                        this.sendPacket.setPosition(0);
                    }

                    this.packetSequence = -1;
                    this.readPacketSequence = 0;
                    this.checkPacketSequence = true;
                    this.sendPacket.setPosition(0);

                    this.sendPacket.writeByte((byte) command);

                    if ((command == MysqlaConstants.COM_INIT_DB) || (command == MysqlaConstants.COM_CREATE_DB) || (command == MysqlaConstants.COM_DROP_DB)
                            || (command == MysqlaConstants.COM_QUERY) || (command == MysqlaConstants.COM_STMT_PREPARE)) {
                        if (extraDataCharEncoding == null) {
                            this.sendPacket.writeStringNoNull(extraData);
                        } else {
                            this.sendPacket.writeStringNoNull(extraData, extraDataCharEncoding, this.connection);
                        }
                    } else if (command == MysqlaConstants.COM_PROCESS_KILL) {
                        long id = Long.parseLong(extraData);
                        this.sendPacket.writeLong(id);
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
                throw ExceptionFactory.createCommunicationsException(this.propertySet, this.serverSession, this.getPacketSentTimeHolder()
                        .getLastPacketSentTime(), this.lastPacketReceivedTimeMs, ex, getExceptionInterceptor());
            }

            Buffer returnPacket = null;

            if (!skipCheck) {
                if ((command == MysqlaConstants.COM_STMT_EXECUTE) || (command == MysqlaConstants.COM_STMT_RESET)) {
                    this.readPacketSequence = 0;
                    this.packetSequenceReset = true;
                }

                returnPacket = checkErrorPacket(command);
            }

            return returnPacket;
        } catch (IOException ioEx) {
            throw ExceptionFactory.createCommunicationsException(this.propertySet, this.serverSession, this.getPacketSentTimeHolder().getLastPacketSentTime(),
                    this.lastPacketReceivedTimeMs, ioEx, getExceptionInterceptor());
        } finally {
            if (timeoutMillis != 0) {
                try {
                    this.socketConnection.getMysqlSocket().setSoTimeout(oldTimeout);
                } catch (SocketException e) {
                    throw ExceptionFactory.createCommunicationsException(this.propertySet, this.serverSession, this.getPacketSentTimeHolder()
                            .getLastPacketSentTime(), this.lastPacketReceivedTimeMs, e, getExceptionInterceptor());
                }
            }
        }
    }

    public void checkTransactionState() {
        int transState = this.serverSession.getTransactionState();
        try {
            if (transState == ServerSession.TRANSACTION_COMPLETED) {
                this.connection.transactionCompleted();
            } else if (transState == ServerSession.TRANSACTION_STARTED) {
                this.connection.transactionBegun();
            }
        } catch (SQLException e) {
            throw ExceptionFactory.createException(e.getMessage(), e, getExceptionInterceptor());
        }
    }

    /**
     * Sets the buffer size to max-buf
     */
    public void resetMaxBuf() {
        this.maxAllowedPacket = this.propertySet.getIntegerReadableProperty(PropertyDefinitions.PNAME_maxAllowedPacket).getValue();
    }

    public final int readFully(InputStream in, byte[] b, int off, int len) throws IOException {
        if (len < 0) {
            throw new IndexOutOfBoundsException();
        }

        int n = 0;

        while (n < len) {
            int count = in.read(b, off + n, len - n);

            if (count < 0) {
                throw new EOFException(Messages.getString("MysqlIO.EOF", new Object[] { Integer.valueOf(len), Integer.valueOf(n) }));
            }

            n += count;
        }

        return n;
    }

    /**
     * @param multiPacketSeq
     * @throws CJCommunicationsException
     */
    private void checkPacketSequencing(byte multiPacketSeq) {
        if ((multiPacketSeq == -128) && (this.readPacketSequence != 127)) {
            throw ExceptionFactory.createCommunicationsException(this.propertySet, this.serverSession, this.getPacketSentTimeHolder().getLastPacketSentTime(),
                    this.lastPacketReceivedTimeMs, new IOException(Messages.getString("MysqlIO.108", new Object[] { multiPacketSeq })),
                    getExceptionInterceptor());
        }

        if ((this.readPacketSequence == -1) && (multiPacketSeq != 0)) {
            throw ExceptionFactory.createCommunicationsException(this.propertySet, this.serverSession, this.getPacketSentTimeHolder().getLastPacketSentTime(),
                    this.lastPacketReceivedTimeMs, new IOException(Messages.getString("MysqlIO.109", new Object[] { multiPacketSeq })),
                    getExceptionInterceptor());
        }

        if ((multiPacketSeq != -128) && (this.readPacketSequence != -1) && (multiPacketSeq != (this.readPacketSequence + 1))) {
            throw ExceptionFactory.createCommunicationsException(this.propertySet, this.serverSession, this.getPacketSentTimeHolder().getLastPacketSentTime(),
                    this.lastPacketReceivedTimeMs, new IOException(Messages.getString("MysqlIO.110", new Object[] { multiPacketSeq })),
                    getExceptionInterceptor());
        }
    }

    /**
     * Returns the hex dump of the given packet, truncated to
     * MAX_PACKET_DUMP_LENGTH if packetLength exceeds that value.
     * 
     * @param packetToDump
     *            the packet to dump in hex
     * @param packetLength
     *            the number of bytes to dump
     * 
     * @return the hex dump of the given packet
     */
    private final static String getPacketDumpToLog(Buffer packetToDump, int packetLength) {
        if (packetLength < MAX_PACKET_DUMP_LENGTH) {
            return packetToDump.dump(packetLength);
        }

        StringBuilder packetDumpBuf = new StringBuilder(MAX_PACKET_DUMP_LENGTH * 4);
        packetDumpBuf.append(packetToDump.dump(MAX_PACKET_DUMP_LENGTH));
        packetDumpBuf.append(Messages.getString("MysqlIO.36"));
        packetDumpBuf.append(MAX_PACKET_DUMP_LENGTH);
        packetDumpBuf.append(Messages.getString("MysqlIO.37"));

        return packetDumpBuf.toString();
    }

    private void enqueuePacketForDebugging(boolean isPacketBeingSent, boolean isPacketReused, int sendLength, byte[] header, Buffer packet) {
        if ((this.packetDebugRingBuffer.size() + 1) > this.propertySet.getIntegerReadableProperty(PropertyDefinitions.PNAME_packetDebugBufferSize).getValue()) {
            this.packetDebugRingBuffer.removeFirst();
        }

        StringBuilder packetDump = null;

        if (!isPacketBeingSent) {
            int bytesToDump = Math.min(MAX_PACKET_DUMP_LENGTH, packet.getBufLength());

            Buffer packetToDump = new Buffer(4 + bytesToDump);

            packetToDump.setPosition(0);
            packetToDump.writeBytesNoNull(header);
            packetToDump.writeBytesNoNull(packet.getBytes(0, bytesToDump));

            String packetPayload = packetToDump.dump(bytesToDump);

            packetDump = new StringBuilder(96 + packetPayload.length());

            packetDump.append("Server ");

            packetDump.append(isPacketReused ? "(re-used) " : "(new) ");

            packetDump.append(packet.toSuperString());
            packetDump.append(" --------------------> Client\n");
            packetDump.append("\nPacket payload:\n\n");
            packetDump.append(packetPayload);

            if (bytesToDump == MAX_PACKET_DUMP_LENGTH) {
                packetDump.append("\nNote: Packet of " + packet.getBufLength() + " bytes truncated to " + MAX_PACKET_DUMP_LENGTH + " bytes.\n");
            }
        }

        this.packetDebugRingBuffer.addLast(packetDump);
    }

    /**
     * Checks for errors in the reply packet, and if none, returns the reply
     * packet, ready for reading
     * 
     * @return a packet ready for reading.
     * 
     * @throws CJException
     *             is the packet is an error packet
     */
    public Buffer checkErrorPacket() {
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
    private Buffer checkErrorPacket(int command) {
        //int statusCode = 0;
        Buffer resultPacket = null;
        this.serverSession.setStatusFlags(0);

        try {
            // Check return value, if we get a java.io.EOFException, the server has gone away. We'll pass it on up the exception chain and let someone higher up
            // decide what to do (barf, reconnect, etc).
            resultPacket = reuseAndReadPacket(this.reusablePacket);
        } catch (CJException ex) {
            // Don't wrap CJExceptions
            throw ex;
        } catch (Exception fallThru) {
            throw ExceptionFactory.createCommunicationsException(this.propertySet, this.serverSession, this.getPacketSentTimeHolder().getLastPacketSentTime(),
                    this.lastPacketReceivedTimeMs, fallThru, getExceptionInterceptor());
        }

        checkErrorPacket(resultPacket);

        return resultPacket;
    }

    public void checkErrorPacket(Buffer resultPacket) {

        int statusCode = resultPacket.readByte();

        // Error handling
        if (statusCode == (byte) 0xff) {
            String serverErrorMessage;
            int errno = 2000;

            errno = resultPacket.readInt();

            String xOpen = null;

            serverErrorMessage = resultPacket.readString(this.connection.getErrorMessageEncoding(), getExceptionInterceptor());

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
                    errorBuf.append(Messages.getString("MysqlIO.68"));
                }
            }

            errorBuf.append(serverErrorMessage);

            if (!useOnlyServerErrorMessages) {
                if (xOpenErrorMessage != null) {
                    errorBuf.append("\"");
                }
            }

            this.resultsHandler.appendDeadlockStatusInformation(xOpen, errorBuf);

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
                    this.lastPacketReceivedTimeMs, ioEx, getExceptionInterceptor());
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
     * Re-use a packet to read from the MySQL server
     * 
     * @param reuse
     * 
     */
    public final Buffer reuseAndReadPacket(Buffer reuse) {
        return reuseAndReadPacket(reuse, -1);
    }

    public final Buffer reuseAndReadPacket(Buffer reuse, int existingPacketLength) {

        try {
            reuse.setWasMultiPacket(false);
            int packetLength = 0;

            if (existingPacketLength == -1) {
                int lengthRead = readFully(this.socketConnection.getMysqlInput(), this.packetHeaderBuf, 0, 4);
                if (lengthRead < 4) {
                    this.socketConnection.forceClose();
                    throw new IOException(Messages.getString("MysqlIO.43"));
                }

                packetLength = (this.packetHeaderBuf[0] & 0xff) + ((this.packetHeaderBuf[1] & 0xff) << 8) + ((this.packetHeaderBuf[2] & 0xff) << 16);
            } else {
                packetLength = existingPacketLength;
            }

            if (this.traceProtocol) {
                StringBuilder traceMessageBuf = new StringBuilder();

                traceMessageBuf.append(Messages.getString("MysqlIO.44"));
                traceMessageBuf.append(packetLength);
                traceMessageBuf.append(Messages.getString("MysqlIO.45"));
                traceMessageBuf.append(StringUtils.dumpAsHex(this.packetHeaderBuf, 4));

                this.connection.getLog().logTrace(traceMessageBuf.toString());
            }

            byte multiPacketSeq = this.packetHeaderBuf[3];

            if (!this.packetSequenceReset) {
                if (this.enablePacketDebug && this.checkPacketSequence) {
                    checkPacketSequencing(multiPacketSeq);
                }
            } else {
                this.packetSequenceReset = false;
            }

            this.readPacketSequence = multiPacketSeq;

            // Set the Buffer to it's original state
            reuse.setPosition(0);

            // Do we need to re-alloc the byte buffer?
            //
            // Note: We actually check the length of the buffer, rather than getBufLength(), because getBufLength() is not necesarily the actual length of the
            // byte array used as the buffer
            if (reuse.getByteBuffer().length <= packetLength) {
                reuse.setByteBuffer(new byte[packetLength + 1]);
            }

            // Set the new length
            reuse.setBufLength(packetLength);

            // Read the data from the server
            int numBytesRead = readFully(this.socketConnection.getMysqlInput(), reuse.getByteBuffer(), 0, packetLength);

            if (numBytesRead != packetLength) {
                throw new IOException(Messages.getString("MysqlIO.104", new Object[] { packetLength, numBytesRead }));
            }

            if (this.traceProtocol) {
                StringBuilder traceMessageBuf = new StringBuilder();

                traceMessageBuf.append(Messages.getString("MysqlIO.46"));
                traceMessageBuf.append(getPacketDumpToLog(reuse, packetLength));

                this.connection.getLog().logTrace(traceMessageBuf.toString());
            }

            if (this.enablePacketDebug) {
                enqueuePacketForDebugging(false, true, 0, this.packetHeaderBuf, reuse);
            }

            boolean isMultiPacket = false;

            if (packetLength == ProtocolConstants.MAX_PACKET_SIZE) {
                reuse.setPosition(ProtocolConstants.MAX_PACKET_SIZE);

                // it's multi-packet
                isMultiPacket = true;

                packetLength = readRemainingMultiPackets(reuse, multiPacketSeq);
            }

            if (!isMultiPacket) {
                reuse.getByteBuffer()[packetLength] = 0; // Null-termination
            }

            if (this.maintainTimeStats.getValue()) {
                this.lastPacketReceivedTimeMs = System.currentTimeMillis();
            }

            return reuse;
        } catch (IOException ioEx) {
            throw ExceptionFactory.createCommunicationsException(this.propertySet, this.serverSession, this.getPacketSentTimeHolder().getLastPacketSentTime(),
                    this.lastPacketReceivedTimeMs, ioEx, getExceptionInterceptor());
        } catch (Exception ex) {
            throw ExceptionFactory.createException(ex.getMessage(), ex, getExceptionInterceptor());
        } catch (OutOfMemoryError oom) {
            try {
                // _Try_ this
                clearInputStream();
            } catch (Exception ex) {
            }
            try {
                this.connection.realClose(false, false, true, oom);
            } catch (Exception ex) {
            }
            throw oom;
        }

    }

    private int readRemainingMultiPackets(Buffer reuse, byte multiPacketSeq) throws IOException {
        int packetLength = -1;
        Buffer multiPacket = null;

        do {
            final int lengthRead = readFully(this.socketConnection.getMysqlInput(), this.packetHeaderBuf, 0, 4);
            if (lengthRead < 4) {
                this.socketConnection.forceClose();
                throw new IOException(Messages.getString("MysqlIO.47"));
            }

            packetLength = (this.packetHeaderBuf[0] & 0xff) + ((this.packetHeaderBuf[1] & 0xff) << 8) + ((this.packetHeaderBuf[2] & 0xff) << 16);
            if (multiPacket == null) {
                multiPacket = new Buffer(packetLength);
            }

            multiPacketSeq++;
            if (multiPacketSeq != this.packetHeaderBuf[3]) {
                throw new IOException(Messages.getString("MysqlIO.49"));
            }

            // Set the Buffer to it's original state
            multiPacket.setPosition(0);

            // Set the new length
            multiPacket.setBufLength(packetLength);

            // Read the data from the server
            byte[] byteBuf = multiPacket.getByteBuffer();
            int lengthToWrite = packetLength;

            int bytesRead = readFully(this.socketConnection.getMysqlInput(), byteBuf, 0, packetLength);

            if (bytesRead != lengthToWrite) {

                throw ExceptionFactory.createCommunicationsException(this.propertySet, this.serverSession, this.getPacketSentTimeHolder()
                        .getLastPacketSentTime(), this.lastPacketReceivedTimeMs, ExceptionFactory.createException(Messages.getString("MysqlIO.50")
                        + lengthToWrite + Messages.getString("MysqlIO.51") + bytesRead + ".", getExceptionInterceptor()), getExceptionInterceptor());
            }

            reuse.writeBytesNoNull(byteBuf, 0, lengthToWrite);
        } while (packetLength == ProtocolConstants.MAX_PACKET_SIZE);

        reuse.setPosition(0);
        reuse.setWasMultiPacket(true);
        return packetLength;
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
     * @param resultSetType
     * @param resultSetConcurrency
     * @param streamResults
     * @param catalog
     * @param unpackFieldInfo
     *            should we read MYSQL_FIELD info (if available)?
     * 
     */
    public final ResultSetInternalMethods sqlQueryDirect(StatementImpl callingStatement, String query, String characterEncoding, Buffer queryPacket,
            int maxRows, int resultSetType, int resultSetConcurrency, boolean streamResults, String catalog, Field[] cachedMetadata) {
        this.statementExecutionDepth++;

        try {
            if (this.statementInterceptors != null) {
                ResultSetInternalMethods interceptedResults = invokeStatementInterceptorsPre(query, callingStatement, false);

                if (interceptedResults != null) {
                    return interceptedResults;
                }
            }

            long queryStartTime = 0;
            long queryEndTime = 0;

            String statementComment = this.connection.getStatementComment();

            if (this.propertySet.getBooleanReadableProperty(PropertyDefinitions.PNAME_includeThreadNamesAsStatementComment).getValue()) {
                statementComment = (statementComment != null ? statementComment + ", " : "") + "java thread: " + Thread.currentThread().getName();
            }

            if (query != null) {
                // We don't know exactly how many bytes we're going to get from the query. Since we're dealing with Unicode, the max is 2, so pad it
                // (2 * query) + space for headers
                int packLength = 1 + (query.length() * 3) + 2;

                byte[] commentAsBytes = null;

                if (statementComment != null) {
                    commentAsBytes = StringUtils.getBytes(statementComment, null, characterEncoding, getExceptionInterceptor());

                    packLength += commentAsBytes.length;
                    packLength += 6; // for /*[space] [space]*/
                }

                if (this.sendPacket == null) {
                    this.sendPacket = new Buffer(packLength);
                }
                this.sendPacket.setPosition(0);

                this.sendPacket.writeByte((byte) MysqlaConstants.COM_QUERY);

                if (commentAsBytes != null) {
                    this.sendPacket.writeBytesNoNull(Constants.SLASH_STAR_SPACE_AS_BYTES);
                    this.sendPacket.writeBytesNoNull(commentAsBytes);
                    this.sendPacket.writeBytesNoNull(Constants.SPACE_STAR_SLASH_SPACE_AS_BYTES);
                }

                if (characterEncoding != null) {
                    if (this.platformDbCharsetMatches) {
                        this.sendPacket.writeStringNoNull(query, characterEncoding, this.connection);
                    } else {
                        if (StringUtils.startsWithIgnoreCaseAndWs(query, "LOAD DATA")) {
                            this.sendPacket.writeBytesNoNull(StringUtils.getBytes(query));
                        } else {
                            this.sendPacket.writeStringNoNull(query, characterEncoding, this.connection);
                        }
                    }
                } else {
                    this.sendPacket.writeStringNoNull(query);
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
                this.connection.generateConnectionCommentBlock(debugBuf);
                debugBuf.append(testcaseQuery);
                debugBuf.append(';');
                TestUtils.dumpTestcaseQuery(debugBuf.toString());
            }

            // Send query command and sql query string
            Buffer resultPacket = sendCommand(MysqlaConstants.COM_QUERY, null, queryPacket, false, null, 0);

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
                        logSlow = this.connection.isAbonormallyLongQuery(queryTime);

                        this.connection.reportQueryTime(queryTime);
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
                        profileQueryToLog += Messages.getString("MysqlIO.25");
                    }
                }

                fetchBeginTime = queryEndTime;
            }

            ResultSetInternalMethods rs = this.resultsHandler.readAllResults(callingStatement, maxRows, resultSetType, resultSetConcurrency, streamResults,
                    catalog, resultPacket, false, -1L, cachedMetadata);

            if (queryWasSlow && !this.serverQueryWasSlow /* don't log slow queries twice */) {
                StringBuilder mesgBuf = new StringBuilder(48 + profileQueryToLog.length());

                mesgBuf.append(Messages.getString(
                        "MysqlIO.SlowQuery",
                        new Object[] { String.valueOf(this.useAutoSlowLog ? " 95% of all queries " : this.slowQueryThreshold), this.queryTimingUnits,
                                Long.valueOf(queryEndTime - queryStartTime) }));
                mesgBuf.append(profileQueryToLog);

                ProfilerEventHandler eventSink = ProfilerEventHandlerFactory.getInstance(this.connection);

                eventSink.consumeEvent(new ProfilerEventImpl(ProfilerEvent.TYPE_SLOW_QUERY, "", catalog, this.connection.getId(),
                        (callingStatement != null) ? callingStatement.getId() : 999, ((ResultSetImpl) rs).resultId, System.currentTimeMillis(),
                        (int) (queryEndTime - queryStartTime), this.queryTimingUnits, null, LogUtils.findCallingClassAndMethod(new Throwable()), mesgBuf
                                .toString()));

                if (this.propertySet.getBooleanReadableProperty(PropertyDefinitions.PNAME_explainSlowQueries).getValue()) {
                    if (oldPacketPosition < MAX_QUERY_SIZE_TO_EXPLAIN) {
                        explainSlowQuery(queryPacket.getBytes(1, (oldPacketPosition - 1)), profileQueryToLog);
                    } else {
                        this.connection.getLog().logWarn(Messages.getString("MysqlIO.28") + MAX_QUERY_SIZE_TO_EXPLAIN + Messages.getString("MysqlIO.29"));
                    }
                }
            }

            if (this.logSlowQueries) {

                ProfilerEventHandler eventSink = ProfilerEventHandlerFactory.getInstance(this.connection);

                if (this.queryBadIndexUsed && this.profileSQL) {
                    eventSink.consumeEvent(new ProfilerEventImpl(ProfilerEvent.TYPE_SLOW_QUERY, "", catalog, this.connection.getId(),
                            (callingStatement != null) ? callingStatement.getId() : 999, ((ResultSetImpl) rs).resultId, System.currentTimeMillis(),
                            (queryEndTime - queryStartTime), this.queryTimingUnits, null, LogUtils.findCallingClassAndMethod(new Throwable()), Messages
                                    .getString("MysqlIO.33") + profileQueryToLog));
                }

                if (this.queryNoIndexUsed && this.profileSQL) {
                    eventSink.consumeEvent(new ProfilerEventImpl(ProfilerEvent.TYPE_SLOW_QUERY, "", catalog, this.connection.getId(),
                            (callingStatement != null) ? callingStatement.getId() : 999, ((ResultSetImpl) rs).resultId, System.currentTimeMillis(),
                            (queryEndTime - queryStartTime), this.queryTimingUnits, null, LogUtils.findCallingClassAndMethod(new Throwable()), Messages
                                    .getString("MysqlIO.35") + profileQueryToLog));
                }

                if (this.serverQueryWasSlow && this.profileSQL) {
                    eventSink.consumeEvent(new ProfilerEventImpl(ProfilerEvent.TYPE_SLOW_QUERY, "", catalog, this.connection.getId(),
                            (callingStatement != null) ? callingStatement.getId() : 999, ((ResultSetImpl) rs).resultId, System.currentTimeMillis(),
                            (queryEndTime - queryStartTime), this.queryTimingUnits, null, LogUtils.findCallingClassAndMethod(new Throwable()), Messages
                                    .getString("MysqlIO.ServerSlowQuery") + profileQueryToLog));
                }
            }

            if (this.profileSQL) {
                fetchEndTime = getCurrentTimeNanosOrMillis();

                ProfilerEventHandler eventSink = ProfilerEventHandlerFactory.getInstance(this.connection);

                eventSink.consumeEvent(new ProfilerEventImpl(ProfilerEvent.TYPE_QUERY, "", catalog, this.connection.getId(),
                        (callingStatement != null) ? callingStatement.getId() : 999, ((ResultSetImpl) rs).resultId, System.currentTimeMillis(),
                        (queryEndTime - queryStartTime), this.queryTimingUnits, null, LogUtils.findCallingClassAndMethod(new Throwable()), profileQueryToLog));

                eventSink.consumeEvent(new ProfilerEventImpl(ProfilerEvent.TYPE_FETCH, "", catalog, this.connection.getId(),
                        (callingStatement != null) ? callingStatement.getId() : 999, ((ResultSetImpl) rs).resultId, System.currentTimeMillis(),
                        (fetchEndTime - fetchBeginTime), this.queryTimingUnits, null, LogUtils.findCallingClassAndMethod(new Throwable()), null));
            }

            if (this.hadWarnings) {
                this.resultsHandler.scanForAndThrowDataTruncation();
            }

            if (this.statementInterceptors != null) {
                ResultSetInternalMethods interceptedResults = invokeStatementInterceptorsPost(query, callingStatement, rs, false, null);

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

    public boolean shouldIntercept() {
        return this.statementInterceptors != null;
    }

    public ResultSetInternalMethods invokeStatementInterceptorsPre(String sql, Statement interceptedStatement, boolean forceExecute) {
        ResultSetInternalMethods previousResultSet = null;

        for (int i = 0, s = this.statementInterceptors.size(); i < s; i++) {
            StatementInterceptorV2 interceptor = this.statementInterceptors.get(i);

            boolean executeTopLevelOnly = interceptor.executeTopLevelOnly();
            boolean shouldExecute = (executeTopLevelOnly && (this.statementExecutionDepth == 1 || forceExecute)) || (!executeTopLevelOnly);

            if (shouldExecute) {
                String sqlToInterceptor = sql;

                //if (interceptedStatement instanceof PreparedStatement) {
                //  sqlToInterceptor = ((PreparedStatement) interceptedStatement)
                //          .asSql();
                //}

                try {
                    ResultSetInternalMethods interceptedResultSet = interceptor.preProcess(sqlToInterceptor, interceptedStatement, this.connection);

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

    public ResultSetInternalMethods invokeStatementInterceptorsPost(String sql, Statement interceptedStatement, ResultSetInternalMethods originalResultSet,
            boolean forceExecute, Exception statementException) {

        for (int i = 0, s = this.statementInterceptors.size(); i < s; i++) {
            StatementInterceptorV2 interceptor = this.statementInterceptors.get(i);

            boolean executeTopLevelOnly = interceptor.executeTopLevelOnly();
            boolean shouldExecute = (executeTopLevelOnly && (this.statementExecutionDepth == 1 || forceExecute)) || (!executeTopLevelOnly);

            if (shouldExecute) {
                String sqlToInterceptor = sql;

                try {
                    ResultSetInternalMethods interceptedResultSet = interceptor.postProcess(sqlToInterceptor, interceptedStatement, originalResultSet,
                            this.connection, this.getWarningCount(), this.queryNoIndexUsed, this.queryBadIndexUsed, statementException);

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
                || (StringUtils.startsWithIgnoreCaseAndWs(truncatedQuery, EXPLAINABLE_STATEMENT_EXTENSION) != -1)) {

            PreparedStatement stmt = null;
            java.sql.ResultSet rs = null;

            try {
                stmt = (PreparedStatement) this.connection.clientPrepareStatement("EXPLAIN ?");
                stmt.setBytesNoEscapeNoQuotes(1, querySQL);
                rs = stmt.executeQuery();

                StringBuilder explainResults = new StringBuilder(Messages.getString("MysqlIO.8") + truncatedQuery + Messages.getString("MysqlIO.9"));

                ResultSetUtil.appendResultSetSlashGStyle(explainResults, rs);

                this.connection.getLog().logWarn(explainResults.toString());
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

            int lengthRead = readFully(this.socketConnection.getMysqlInput(), this.packetHeaderBuf, 0, 4);

            if (lengthRead < 4) {
                this.socketConnection.forceClose();
                throw new IOException(Messages.getString("MysqlIO.1"));
            }

            int packetLength = (this.packetHeaderBuf[0] & 0xff) + ((this.packetHeaderBuf[1] & 0xff) << 8) + ((this.packetHeaderBuf[2] & 0xff) << 16);

            if (this.traceProtocol) {
                StringBuilder traceMessageBuf = new StringBuilder();

                traceMessageBuf.append(Messages.getString("MysqlIO.2"));
                traceMessageBuf.append(packetLength);
                traceMessageBuf.append(Messages.getString("MysqlIO.3"));
                traceMessageBuf.append(StringUtils.dumpAsHex(this.packetHeaderBuf, 4));

                this.connection.getLog().logTrace(traceMessageBuf.toString());
            }

            byte multiPacketSeq = this.packetHeaderBuf[3];

            if (!this.packetSequenceReset) {
                if (this.enablePacketDebug && this.checkPacketSequence) {
                    checkPacketSequencing(multiPacketSeq);
                }
            } else {
                this.packetSequenceReset = false;
            }

            this.readPacketSequence = multiPacketSeq;

            skipFully(this.socketConnection.getMysqlInput(), packetLength);
        } catch (IOException ioEx) {
            throw ExceptionFactory.createCommunicationsException(this.propertySet, this.serverSession, this.getPacketSentTimeHolder().getLastPacketSentTime(),
                    this.lastPacketReceivedTimeMs, ioEx, getExceptionInterceptor());
        } catch (CJException ex) {
            throw ex;
        } catch (Exception ex) {
            throw ExceptionFactory.createException(ex.getMessage(), ex, getExceptionInterceptor());
        } catch (OutOfMemoryError oom) {
            try {
                this.connection.realClose(false, false, true, oom);
            } catch (Exception ex) {
            }
            throw oom;
        }
    }

    public final long skipFully(InputStream in, long len) throws IOException {
        if (len < 0) {
            throw new IOException(Messages.getString("MysqlIO.105"));
        }

        long n = 0;

        while (n < len) {
            long count = in.skip(len - n);

            if (count < 0) {
                throw new EOFException(Messages.getString("MysqlIO.EOF", new Object[] { Long.valueOf(len), Long.valueOf(n) }));
            }

            n += count;
        }

        return n;
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
                this.connection.getLog().logWarn("Caught while disconnecting...", ioEx);
            }

            Buffer packet = new Buffer(6);
            packet.setPosition(0);
            this.packetSequence = -1;
            packet.writeByte((byte) MysqlaConstants.COM_QUIT);
            send(packet, packet.getPosition());
        } finally {
            this.socketConnection.forceClose();
        }
    }

    /**
     * Returns the packet used for sending data (used by PreparedStatement)
     * Guarded by external synchronization on a mutex.
     * 
     * @return A packet to send data with
     */
    public Buffer getSharedSendPacket() {
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

    /**
     * Apply optional decorators to configured PacketSender.
     */
    protected void decoratePacketSender() {
        TimeTrackingPacketSender ttSender = new TimeTrackingPacketSender(this.packetSender);
        this.setPacketSentTimeHolder(ttSender);
        this.packetSender = ttSender;
        if (this.traceProtocol) {
            this.packetSender = new TracingPacketSender(this.packetSender, this.connection.getLog(), this.socketConnection.getHost(), getServerSession()
                    .getCapabilities().getThreadId());
        }
        if (this.enablePacketDebug) {
            this.packetSender = new DebugBufferingPacketSender(this.packetSender, this.packetDebugRingBuffer);
        }
    }

    public void setStatementInterceptors(List<StatementInterceptorV2> statementInterceptors) {
        this.statementInterceptors = statementInterceptors.isEmpty() ? null : statementInterceptors;
    }

    public void setSocketTimeout(int milliseconds) {
        try {
            this.socketConnection.getMysqlSocket().setSoTimeout(milliseconds);
        } catch (SocketException e) {
            throw ExceptionFactory.createException(WrongArgumentException.class, Messages.getString("MysqlIO.112"), e, getExceptionInterceptor());
        }
    }

    public void releaseResources() {
        if (this.compressedPacketSender != null) {
            this.compressedPacketSender.stop();
        }
    }

    public MysqlaSession getSession(String user, String password, String database) {
        // session creation & initialization happens here

        beforeHandshake();

        this.authProvider.connect(this.serverSession, user, password, database);
        MysqlaSession session = new MysqlaSession(this);

        return session;
    }

    @Override
    public MysqlJdbcConnection getConnection() {
        return this.connection;
    }

    public void setConnection(MysqlJdbcConnection connection) {
        this.connection = connection;
    }

    protected boolean isDataAvailable() {
        try {
            return this.socketConnection.getMysqlInput().available() > 0;
        } catch (IOException ioEx) {
            throw ExceptionFactory.createCommunicationsException(this.propertySet, this.serverSession, this.getPacketSentTimeHolder().getLastPacketSentTime(),
                    this.lastPacketReceivedTimeMs, ioEx, getExceptionInterceptor());
        }
    }

    @Override
    public MysqlIO getResultsHandler() {
        return this.resultsHandler;
    }

    public Buffer getReusablePacket() {
        return this.reusablePacket;
    }

    public void setReusablePacket(Buffer packet) {
        this.reusablePacket = packet;
    }

    public int getWarningCount() {
        return this.warningCount;
    }

    public void setWarningCount(int warningCount) {
        this.warningCount = warningCount;
    }

    public byte[] getPacketHeaderBuf() {
        return this.packetHeaderBuf;
    }

    public void dumpPacketRingBuffer() {
        if ((this.packetDebugRingBuffer != null) && this.getPropertySet().getBooleanReadableProperty(PropertyDefinitions.PNAME_enablePacketDebug).getValue()) {
            StringBuilder dumpBuffer = new StringBuilder();

            dumpBuffer.append("Last " + this.packetDebugRingBuffer.size() + " packets received from server, from oldest->newest:\n");
            dumpBuffer.append("\n");

            for (Iterator<StringBuilder> ringBufIter = this.packetDebugRingBuffer.iterator(); ringBufIter.hasNext();) {
                dumpBuffer.append(ringBufIter.next());
                dumpBuffer.append("\n");
            }

            this.connection.getLog().logTrace(dumpBuffer.toString());
        }
    }

    public void incrementPacketSequence() {
        this.packetSequence++;
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
}
