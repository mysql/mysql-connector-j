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
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.net.SocketException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import com.mysql.cj.api.MysqlConnection;
import com.mysql.cj.api.ProfilerEvent;
import com.mysql.cj.api.ProfilerEventHandler;
import com.mysql.cj.api.SessionState;
import com.mysql.cj.api.conf.PropertySet;
import com.mysql.cj.api.io.PacketBuffer;
import com.mysql.cj.api.io.PhysicalConnection;
import com.mysql.cj.api.io.Protocol;
import com.mysql.cj.core.Constants;
import com.mysql.cj.core.Messages;
import com.mysql.cj.core.ServerVersion;
import com.mysql.cj.core.conf.PropertyDefinitions;
import com.mysql.cj.core.exception.CJConnectionFeatureNotAvailableException;
import com.mysql.cj.core.exception.CJException;
import com.mysql.cj.core.exception.ExceptionFactory;
import com.mysql.cj.core.exception.FeatureNotAvailableException;
import com.mysql.cj.core.exception.UnableToConnectException;
import com.mysql.cj.core.io.AbstractProtocol;
import com.mysql.cj.core.io.Buffer;
import com.mysql.cj.core.io.CompressedInputStream;
import com.mysql.cj.core.io.CompressedPacketSender;
import com.mysql.cj.core.io.ExportControlled;
import com.mysql.cj.core.io.ProtocolConstants;
import com.mysql.cj.core.io.SimplePacketSender;
import com.mysql.cj.core.profiler.ProfilerEventHandlerFactory;
import com.mysql.cj.core.profiler.ProfilerEventImpl;
import com.mysql.cj.core.util.LogUtils;
import com.mysql.cj.core.util.StringUtils;
import com.mysql.cj.core.util.TestUtils;
import com.mysql.cj.core.util.Util;
import com.mysql.jdbc.Field;
import com.mysql.jdbc.MysqlDefs;
import com.mysql.jdbc.MysqlIO;
import com.mysql.jdbc.MysqlJdbcConnection;
import com.mysql.jdbc.PreparedStatement;
import com.mysql.jdbc.ResultSetImpl;
import com.mysql.jdbc.ResultSetInternalMethods;
import com.mysql.jdbc.RowData;
import com.mysql.jdbc.Statement;
import com.mysql.jdbc.StatementImpl;
import com.mysql.jdbc.exceptions.CommunicationsException;
import com.mysql.jdbc.exceptions.MySQLStatementCancelledException;
import com.mysql.jdbc.exceptions.MySQLTimeoutException;
import com.mysql.jdbc.exceptions.MysqlDataTruncation;
import com.mysql.jdbc.exceptions.PacketTooBigException;
import com.mysql.jdbc.exceptions.SQLError;
import com.mysql.jdbc.exceptions.SQLExceptionsMapping;
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

    private MysqlaCapabilities serverCapabilities;

    /** Track this to manually shut down. */
    protected CompressedPacketSender compressedPacketSender;

    /** Data to the server */
    protected RowData streamingData = null;

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

    protected boolean useDirectRowUnpack = true;
    protected int useBufferRowSizeThreshold;

    private int commandCount = 0;

    protected boolean hadWarnings = false;
    protected int warningCount = 0;
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

    @Override
    public void init(MysqlConnection conn, int socketTimeout, PhysicalConnection phConnection) {

        this.connection = (MysqlJdbcConnection) conn;
        this.propertySet = conn.getPropertySet();

        this.physicalConnection = phConnection;

        if (this.propertySet.getBooleanReadableProperty(PropertyDefinitions.PNAME_enablePacketDebug).getValue()) {
            this.packetDebugRingBuffer = new LinkedList<StringBuilder>();
        }
        this.traceProtocol = this.connection.getTraceProtocol();

        this.useAutoSlowLog = this.connection.getAutoSlowLog();

        this.useBufferRowSizeThreshold = this.propertySet.getMemorySizeReadableProperty(PropertyDefinitions.PNAME_largeRowSizeThreshold).getValue();
        this.useDirectRowUnpack = this.connection.getUseDirectRowUnpack();

        this.logSlowQueries = this.connection.getLogSlowQueries();

        this.reusablePacket = new Buffer(INITIAL_PACKET_SIZE);
        this.sendPacket = new Buffer(INITIAL_PACKET_SIZE);

        this.exceptionInterceptor = this.connection.getExceptionInterceptor();

        // socket was here

        this.packetSender = new SimplePacketSender(this.physicalConnection.getMysqlOutput());

        this.profileSQL = this.connection.getProfileSQL();
        this.autoGenerateTestcaseScript = this.connection.getAutoGenerateTestcaseScript();

        this.needToGrabQueryFromPacket = (this.profileSQL || this.logSlowQueries || this.autoGenerateTestcaseScript);

        if (this.connection.getUseNanosForElapsedTime() && TimeUtil.nanoTimeAvailable()) {
            this.useNanosForElapsedTime = true;

            this.queryTimingUnits = Messages.getString("Nanoseconds");
        } else {
            this.queryTimingUnits = Messages.getString("Milliseconds");
        }

        if (this.connection.getLogSlowQueries()) {
            calculateSlowQueryThreshold();
        }
    }

    /**
     * Negotiates the SSL communications channel used when connecting
     * to a MySQL server that understands SSL.
     * 
     * @param user
     * @param password
     * @param database
     * @param packLength
     * @throws SQLException
     * @throws CommunicationsException
     */
    @Override
    public void negotiateSSLConnection(String user, String password, String database, int packLength) {
        if (!ExportControlled.enabled()) {
            throw new CJConnectionFeatureNotAvailableException(this.propertySet, this.connection.getSession(),
                    this.packetSentTimeHolder.getLastPacketSentTime(), null);
        }

        long clientParam = getSession().getSessionState().getClientParam();
        clientParam |= SessionState.CLIENT_SSL;
        getSession().getSessionState().setClientParam(clientParam);

        Buffer packet = new Buffer(packLength);
        packet.setPosition(0);

        packet.writeLong(clientParam);
        packet.writeLong(ProtocolConstants.MAX_PACKET_SIZE);
        getSession().getAuthenticationProvider().appendCharsetByteForHandshake(packet, getSession().getAuthenticationProvider().getEncodingForHandshake());
        packet.writeBytesNoNull(new byte[23]);  // Set of bytes reserved for future use.

        send(packet, packet.getPosition());

        try {
            ExportControlled.transformSocketToSSLSocket(this.physicalConnection);
        } catch (FeatureNotAvailableException nae) {
            throw new CJConnectionFeatureNotAvailableException(this.propertySet, this.connection.getSession(),
                    this.packetSentTimeHolder.getLastPacketSentTime(), nae);
        } catch (IOException ioEx) {
            throw ExceptionFactory.createCommunicationException(this.getConnection().getPropertySet(), this.getConnection().getSession(),
                    this.getLastPacketSentTimeMs(), this.getLastPacketReceivedTimeMs(), ioEx, getExceptionInterceptor());
        }
        // output stream is replaced, build new packet sender
        this.packetSender = new SimplePacketSender(this.physicalConnection.getMysqlOutput());
    }

    // TODO: find a better place for method?
    @Override
    public void rejectConnection(String message) {
        try {
            this.connection.close();
        } catch (SQLException e) {
            throw ExceptionFactory.createException(e.getMessage(), e, getExceptionInterceptor());
        }
        this.physicalConnection.forceClose();
        throw ExceptionFactory.createException(UnableToConnectException.class, message, getExceptionInterceptor());
    }

    @Override
    public void rejectProtocol(Buffer buf) {
        try {
            this.physicalConnection.getMysqlSocket().close();
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

        SQLException ex = SQLError.createSQLException(SQLError.get(xOpen) + ", " + errorBuf.toString(), xOpen, errno, getExceptionInterceptor());
        throw ExceptionFactory.createException(ex.getMessage(), ex, getExceptionInterceptor());
    }

    @Override
    public void beforeHandshake() {
        // Reset packet sequences
        this.checkPacketSequence = false;
        this.readPacketSequence = 0;
    }

    @Override
    public void afterHandshake() {

        try {
            checkTransactionState();
        } catch (SQLException e) {
            throw ExceptionFactory.createException(e.getMessage(), e, getExceptionInterceptor());
        }

        PropertySet pset = this.propertySet;

        //
        // Can't enable compression until after handshake
        //
        if (((getSession().getSessionState().getServerCapabilities() & SessionState.CLIENT_COMPRESS) != 0)
                && pset.getBooleanReadableProperty(PropertyDefinitions.PNAME_useCompression).getValue()
                && !(this.physicalConnection.getMysqlInput() instanceof CompressedInputStream)) {
            this.useCompression = true;
            this.physicalConnection.setMysqlInput(new CompressedInputStream(this.connection, this.physicalConnection.getMysqlInput(), pset
                    .getBooleanReadableProperty(PropertyDefinitions.PNAME_traceProtocol)));
            this.compressedPacketSender = new CompressedPacketSender(this.physicalConnection.getMysqlOutput());
            this.packetSender = this.compressedPacketSender;
        }

        decoratePacketSender();

        try {
            this.physicalConnection.setMysqlSocket(this.physicalConnection.getSocketFactory().afterHandshake());
        } catch (IOException ioEx) {
            throw ExceptionFactory.createCommunicationException(this.propertySet, this.connection.getSession(),
                    this.packetSentTimeHolder.getLastPacketSentTime(), this.lastPacketReceivedTimeMs, ioEx, getExceptionInterceptor());
        }
    }

    @SuppressWarnings("unchecked")
    public MysqlaCapabilities readServerCapabilities() {
        // Read the first packet
        try {
            Buffer buf = readPacket();
            this.serverCapabilities = new MysqlaCapabilities();
            this.serverCapabilities.setInitialHandshakePacket(buf);

        } catch (SQLException e) {
            throw ExceptionFactory.createException(e.getMessage(), e, getExceptionInterceptor());
        }

        return this.serverCapabilities;
    }

    @SuppressWarnings("unchecked")
    public MysqlaCapabilities getServerCapabilities() {
        return this.serverCapabilities;
    }

    @Override
    public void changeDatabase(String database) {
        if (database == null || database.length() == 0) {
            return;
        }

        try {
            sendCommand(MysqlDefs.INIT_DB, database, null, false, null, 0);
        } catch (SQLException | CJException ex) {
            if (this.propertySet.getBooleanReadableProperty(PropertyDefinitions.PNAME_createDatabaseIfNotExist).getValue()) {
                try {
                    sendCommand(MysqlDefs.QUERY, "CREATE DATABASE IF NOT EXISTS " + database, null, false, null, 0);
                    sendCommand(MysqlDefs.INIT_DB, database, null, false, null, 0);
                } catch (SQLException e) {
                    throw ExceptionFactory.createException(e.getMessage(), e, getExceptionInterceptor());
                }
            } else {
                throw ExceptionFactory.createCommunicationException(this.propertySet, this.connection.getSession(),
                        this.packetSentTimeHolder.getLastPacketSentTime(), this.lastPacketReceivedTimeMs, ex, getExceptionInterceptor());
            }
        }
    }

    /**
     * Read one packet from the MySQL server
     * 
     * @return the packet from the server.
     * 
     * @throws SQLException
     * @throws CommunicationsException
     */
    @Override
    public final Buffer readPacket() throws SQLException {
        try {

            int lengthRead = readFully(this.physicalConnection.getMysqlInput(), this.packetHeaderBuf, 0, 4);

            if (lengthRead < 4) {
                this.physicalConnection.forceClose();
                throw new IOException(Messages.getString("MysqlIO.1"));
            }

            int packetLength = (this.packetHeaderBuf[0] & 0xff) + ((this.packetHeaderBuf[1] & 0xff) << 8) + ((this.packetHeaderBuf[2] & 0xff) << 16);

            if (packetLength > this.maxAllowedPacket) {
                throw new PacketTooBigException(packetLength, this.maxAllowedPacket);
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
            int numBytesRead = readFully(this.physicalConnection.getMysqlInput(), buffer, 0, packetLength);

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

            if (this.connection.getMaintainTimeStats()) {
                this.lastPacketReceivedTimeMs = System.currentTimeMillis();
            }

            return packet;
        } catch (IOException ioEx) {
            throw SQLError.createCommunicationsException(this.connection, this.packetSentTimeHolder.getLastPacketSentTime(), this.lastPacketReceivedTimeMs,
                    ioEx, getExceptionInterceptor());
        } catch (SQLException ex) {
            throw ex;
        } catch (Exception ex) {
            throw SQLError.createSQLException(ex.getMessage(), SQLError.SQL_STATE_GENERAL_ERROR, ex, getExceptionInterceptor());
        } catch (OutOfMemoryError oom) {
            try {
                this.connection.realClose(false, false, true, oom);
            } catch (Exception ex) {
            }
            throw oom;
        }
    }

    @Override
    public Buffer readNextPacket() throws SQLException {
        // read packet from server and check if it's an ERROR packet
        Buffer packet = checkErrorPacket();
        this.packetSequence++;
        return packet;
    }

    /**
     * @param packet
     * @param packetLen
     *            length of header + payload
     * @throws SQLException
     */
    @Override
    public final void send(PacketBuffer packet, int packetLen) {
        try {
            if (this.maxAllowedPacket > 0 && packetLen > this.maxAllowedPacket) {
                throw new PacketTooBigException(packetLen, this.maxAllowedPacket);
            }

            this.packetSequence++;
            this.packetSender.send(packet.getByteBuffer(), packetLen, this.packetSequence);

            //
            // Don't hold on to large packets
            //
            if (packet == this.sharedSendPacket) {
                reclaimLargeSharedSendPacket();
            }
        } catch (SQLException e) {
            throw ExceptionFactory.createException(e.getMessage(), e, getExceptionInterceptor());
        } catch (IOException ioEx) {
            throw ExceptionFactory.createCommunicationException(this.propertySet, this.connection.getSession(),
                    this.packetSentTimeHolder.getLastPacketSentTime(), this.lastPacketReceivedTimeMs, ioEx, getExceptionInterceptor());
        }
    }

    /**
     * Send a command to the MySQL server If data is to be sent with command,
     * it should be put in extraData.
     * 
     * Raw packets can be sent by setting queryPacket to something other
     * than null.
     * 
     * @param command
     *            the MySQL protocol 'command' from MysqlDefs
     * @param extraData
     *            any 'string' data for the command
     * @param queryPacket
     *            a packet pre-loaded with data for the protocol (i.e.
     *            from a client-side prepared statement).
     * @param skipCheck
     *            do not call checkErrorPacket() if true
     * @param extraDataCharEncoding
     *            the character encoding of the extraData
     *            parameter.
     * 
     * @return the response packet from the server
     * 
     * @throws SQLException
     *             if an I/O error or SQL error occurs
     */

    @Override
    public final Buffer sendCommand(int command, String extraData, Buffer queryPacket, boolean skipCheck, String extraDataCharEncoding, int timeoutMillis)
            throws SQLException {
        this.commandCount++;

        //
        // We cache these locally, per-command, as the checks for them are in very 'hot' sections of the I/O code and we save 10-15% in overall performance by
        // doing this...
        //
        this.enablePacketDebug = this.propertySet.getBooleanReadableProperty(PropertyDefinitions.PNAME_enablePacketDebug).getValue();
        this.readPacketSequence = 0;

        int oldTimeout = 0;

        if (timeoutMillis != 0) {
            try {
                oldTimeout = this.physicalConnection.getMysqlSocket().getSoTimeout();
                this.physicalConnection.getMysqlSocket().setSoTimeout(timeoutMillis);
            } catch (SocketException e) {
                throw SQLError.createCommunicationsException(this.connection, this.packetSentTimeHolder.getLastPacketSentTime(), this.lastPacketReceivedTimeMs,
                        e, getExceptionInterceptor());
            }
        }

        try {

            checkForOutstandingStreamingData();

            // Clear serverStatus...this value is guarded by an external mutex, as you can only ever be processing one command at a time
            this.getSession().getSessionState().setServerStatus(0, true);
            this.hadWarnings = false;
            this.warningCount = 0;

            this.queryNoIndexUsed = false;
            this.queryBadIndexUsed = false;
            this.serverQueryWasSlow = false;

            //
            // Compressed input stream needs cleared at beginning of each command execution...
            //
            if (this.useCompression) {
                int bytesLeft = this.physicalConnection.getMysqlInput().available();

                if (bytesLeft > 0) {
                    this.physicalConnection.getMysqlInput().skip(bytesLeft);
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

                    if ((command == MysqlDefs.INIT_DB) || (command == MysqlDefs.CREATE_DB) || (command == MysqlDefs.DROP_DB) || (command == MysqlDefs.QUERY)
                            || (command == MysqlDefs.COM_PREPARE)) {
                        if (extraDataCharEncoding == null) {
                            this.sendPacket.writeStringNoNull(extraData);
                        } else {
                            this.sendPacket.writeStringNoNull(extraData, extraDataCharEncoding, this.connection);
                        }
                    } else if (command == MysqlDefs.PROCESS_KILL) {
                        long id = Long.parseLong(extraData);
                        this.sendPacket.writeLong(id);
                    }

                    send(this.sendPacket, this.sendPacket.getPosition());
                } else {
                    this.packetSequence = -1;
                    send(queryPacket, queryPacket.getPosition()); // packet passed by PreparedStatement
                }
            } catch (CJException | SQLException sqlEx) {
                // don't wrap SQLExceptions
                throw SQLExceptionsMapping.translateException(sqlEx, getExceptionInterceptor());
            } catch (Exception ex) {
                throw SQLError.createCommunicationsException(this.connection, this.packetSentTimeHolder.getLastPacketSentTime(), this.lastPacketReceivedTimeMs,
                        ex, getExceptionInterceptor());
            }

            Buffer returnPacket = null;

            if (!skipCheck) {
                if ((command == MysqlDefs.COM_EXECUTE) || (command == MysqlDefs.COM_RESET_STMT)) {
                    this.readPacketSequence = 0;
                    this.packetSequenceReset = true;
                }

                returnPacket = checkErrorPacket(command);
            }

            return returnPacket;
        } catch (IOException ioEx) {
            throw SQLError.createCommunicationsException(this.connection, this.packetSentTimeHolder.getLastPacketSentTime(), this.lastPacketReceivedTimeMs,
                    ioEx, getExceptionInterceptor());
        } finally {
            if (timeoutMillis != 0) {
                try {
                    this.physicalConnection.getMysqlSocket().setSoTimeout(oldTimeout);
                } catch (SocketException e) {
                    throw SQLError.createCommunicationsException(this.connection, this.packetSentTimeHolder.getLastPacketSentTime(),
                            this.lastPacketReceivedTimeMs, e, getExceptionInterceptor());
                }
            }
        }
    }

    @Override
    public void setThreadId(long threadId) {
        this.threadId = threadId;
    }

    // non-interface methods

    public long getThreadId() {
        return this.threadId;
    }

    protected void checkTransactionState() throws SQLException {
        int transState = this.getSession().getSessionState().getTransactionState();
        if (transState == SessionState.TRANSACTION_COMPLETED) {
            this.connection.transactionCompleted();
        } else if (transState == SessionState.TRANSACTION_STARTED) {
            this.connection.transactionBegun();
        }
    }

    /**
     * Sets the buffer size to max-buf
     */
    public void resetMaxBuf() {
        this.maxAllowedPacket = this.connection.getMaxAllowedPacket();
    }

    protected final int readFully(InputStream in, byte[] b, int off, int len) throws IOException {
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
     * @throws CommunicationsException
     */
    private void checkPacketSequencing(byte multiPacketSeq) throws SQLException {
        if ((multiPacketSeq == -128) && (this.readPacketSequence != 127)) {
            throw SQLError.createCommunicationsException(this.connection, this.packetSentTimeHolder.getLastPacketSentTime(), this.lastPacketReceivedTimeMs,
                    new IOException(Messages.getString("MysqlIO.108", new Object[] { multiPacketSeq })), getExceptionInterceptor());
        }

        if ((this.readPacketSequence == -1) && (multiPacketSeq != 0)) {
            throw SQLError.createCommunicationsException(this.connection, this.packetSentTimeHolder.getLastPacketSentTime(), this.lastPacketReceivedTimeMs,
                    new IOException(Messages.getString("MysqlIO.109", new Object[] { multiPacketSeq })), getExceptionInterceptor());
        }

        if ((multiPacketSeq != -128) && (this.readPacketSequence != -1) && (multiPacketSeq != (this.readPacketSequence + 1))) {
            throw SQLError
                    .createCommunicationsException(this.connection, this.packetSentTimeHolder.getLastPacketSentTime(), this.lastPacketReceivedTimeMs,
                            new IOException(Messages.getString("MysqlIO.110", new Object[] { this.readPacketSequence + 1, multiPacketSeq })),
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

    private void enqueuePacketForDebugging(boolean isPacketBeingSent, boolean isPacketReused, int sendLength, byte[] header, Buffer packet) throws SQLException {
        if ((this.packetDebugRingBuffer.size() + 1) > this.connection.getPacketDebugBufferSize()) {
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
     * @throws SQLException
     *             is the packet is an error packet
     */
    protected Buffer checkErrorPacket() throws SQLException {
        return checkErrorPacket(-1);
    }

    /**
     * Checks for errors in the reply packet, and if none, returns the reply
     * packet, ready for reading
     * 
     * @param command
     *            the command being issued (if used)
     * 
     * @throws SQLException
     *             if an error packet was received
     * @throws CommunicationsException
     */
    private Buffer checkErrorPacket(int command) throws SQLException {
        //int statusCode = 0;
        Buffer resultPacket = null;
        this.getSession().getSessionState().setServerStatus(0);

        try {
            // Check return value, if we get a java.io.EOFException, the server has gone away. We'll pass it on up the exception chain and let someone higher up
            // decide what to do (barf, reconnect, etc).
            resultPacket = reuseAndReadPacket(this.reusablePacket);
        } catch (SQLException sqlEx) {
            // Don't wrap SQL Exceptions
            throw sqlEx;
        } catch (Exception fallThru) {
            throw SQLError.createCommunicationsException(this.connection, this.packetSentTimeHolder.getLastPacketSentTime(), this.lastPacketReceivedTimeMs,
                    fallThru, getExceptionInterceptor());
        }

        checkErrorPacket(resultPacket);

        return resultPacket;
    }

    protected void checkErrorPacket(Buffer resultPacket) throws SQLException {

        int statusCode = resultPacket.readByte();

        // Error handling
        if (statusCode == (byte) 0xff) {
            String serverErrorMessage;
            int errno = 2000;

            errno = resultPacket.readInt();

            String xOpen = null;

            try {
                serverErrorMessage = resultPacket.readString(this.connection.getErrorMessageEncoding(), getExceptionInterceptor());
            } catch (CJException e) {
                throw SQLExceptionsMapping.translateException(e, getExceptionInterceptor());
            }

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

            if (!this.connection.getUseOnlyServerErrorMessages()) {
                if (xOpenErrorMessage != null) {
                    errorBuf.append(xOpenErrorMessage);
                    errorBuf.append(Messages.getString("MysqlIO.68"));
                }
            }

            errorBuf.append(serverErrorMessage);

            if (!this.connection.getUseOnlyServerErrorMessages()) {
                if (xOpenErrorMessage != null) {
                    errorBuf.append("\"");
                }
            }

            appendDeadlockStatusInformation(xOpen, errorBuf);

            if (xOpen != null && xOpen.startsWith("22")) {
                throw new MysqlDataTruncation(errorBuf.toString(), 0, true, false, 0, 0, errno);
            }
            throw SQLError.createSQLException(errorBuf.toString(), xOpen, errno, false, getExceptionInterceptor(), this.connection);

        }
    }

    private void reclaimLargeSharedSendPacket() {
        if ((this.sharedSendPacket != null) && (this.sharedSendPacket.getCapacity() > 1048576)) {
            this.sharedSendPacket = new Buffer(INITIAL_PACKET_SIZE);
        }
    }

    private void checkForOutstandingStreamingData() throws SQLException {
        if (this.streamingData != null) {
            boolean shouldClobber = this.connection.getClobberStreamingResults();

            if (!shouldClobber) {
                throw SQLError.createSQLException(
                        Messages.getString("MysqlIO.39") + this.streamingData + Messages.getString("MysqlIO.40") + Messages.getString("MysqlIO.41")
                                + Messages.getString("MysqlIO.42"), getExceptionInterceptor());
            }

            // Close the result set
            this.streamingData.getOwner().realClose(false);

            // clear any pending data....
            clearInputStream();
        }
    }

    public void clearInputStream() throws SQLException {
        try {
            int len;

            // Due to a bug in some older Linux kernels (fixed after the patch "tcp: fix FIONREAD/SIOCINQ"), our SocketInputStream.available() may return 1 even
            // if there is no data in the Stream, so, we need to check if InputStream.skip() actually skipped anything.
            while ((len = this.physicalConnection.getMysqlInput().available()) > 0 && this.physicalConnection.getMysqlInput().skip(len) > 0) {
                continue;
            }
        } catch (IOException ioEx) {
            throw SQLError.createCommunicationsException(this.connection, this.packetSentTimeHolder.getLastPacketSentTime(), this.lastPacketReceivedTimeMs,
                    ioEx, getExceptionInterceptor());
        }
    }

    /**
     * Don't hold on to overly-large packets
     */
    protected void reclaimLargeReusablePacket() {
        if ((this.reusablePacket != null) && (this.reusablePacket.getCapacity() > 1048576)) {
            this.reusablePacket = new Buffer(INITIAL_PACKET_SIZE);
        }
    }

    /**
     * Re-use a packet to read from the MySQL server
     * 
     * @param reuse
     * 
     * @throws SQLException
     */
    protected final Buffer reuseAndReadPacket(Buffer reuse) throws SQLException {
        return reuseAndReadPacket(reuse, -1);
    }

    protected final Buffer reuseAndReadPacket(Buffer reuse, int existingPacketLength) throws SQLException {

        try {
            reuse.setWasMultiPacket(false);
            int packetLength = 0;

            if (existingPacketLength == -1) {
                int lengthRead = readFully(this.physicalConnection.getMysqlInput(), this.packetHeaderBuf, 0, 4);
                if (lengthRead < 4) {
                    this.physicalConnection.forceClose();
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
            int numBytesRead = readFully(this.physicalConnection.getMysqlInput(), reuse.getByteBuffer(), 0, packetLength);

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

            if (this.connection.getMaintainTimeStats()) {
                this.lastPacketReceivedTimeMs = System.currentTimeMillis();
            }

            return reuse;
        } catch (IOException ioEx) {
            throw SQLError.createCommunicationsException(this.connection, this.packetSentTimeHolder.getLastPacketSentTime(), this.lastPacketReceivedTimeMs,
                    ioEx, getExceptionInterceptor());
        } catch (SQLException ex) {
            throw ex;
        } catch (Exception ex) {
            throw SQLError.createSQLException(ex.getMessage(), SQLError.SQL_STATE_GENERAL_ERROR, ex, getExceptionInterceptor());
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

    private int readRemainingMultiPackets(Buffer reuse, byte multiPacketSeq) throws IOException, SQLException {
        int packetLength = -1;
        Buffer multiPacket = null;

        do {
            final int lengthRead = readFully(this.physicalConnection.getMysqlInput(), this.packetHeaderBuf, 0, 4);
            if (lengthRead < 4) {
                this.physicalConnection.forceClose();
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

            int bytesRead = readFully(this.physicalConnection.getMysqlInput(), byteBuf, 0, packetLength);

            if (bytesRead != lengthToWrite) {

                throw SQLError.createCommunicationsException(this.connection, this.packetSentTimeHolder.getLastPacketSentTime(), this.lastPacketReceivedTimeMs,
                        SQLError.createSQLException(Messages.getString("MysqlIO.50") + lengthToWrite + Messages.getString("MysqlIO.51") + bytesRead + ".",
                                getExceptionInterceptor()), getExceptionInterceptor());
            }

            reuse.writeBytesNoNull(byteBuf, 0, lengthToWrite);
        } while (packetLength == ProtocolConstants.MAX_PACKET_SIZE);

        reuse.setPosition(0);
        reuse.setWasMultiPacket(true);
        return packetLength;
    }

    private void appendDeadlockStatusInformation(String xOpen, StringBuilder errorBuf) throws SQLException {
        if (this.connection.getIncludeInnodbStatusInDeadlockExceptions() && xOpen != null && (xOpen.startsWith("40") || xOpen.startsWith("41"))
                && this.streamingData == null) {
            ResultSet rs = null;

            try {
                rs = sqlQueryDirect(null, "SHOW ENGINE INNODB STATUS", this.connection.getCharacterEncoding(), null, -1, ResultSet.TYPE_FORWARD_ONLY,
                        ResultSet.CONCUR_READ_ONLY, false, this.connection.getCatalog(), null);

                if (rs.next()) {
                    errorBuf.append("\n\n");
                    errorBuf.append(rs.getString("Status"));
                } else {
                    errorBuf.append("\n\n");
                    errorBuf.append(Messages.getString("MysqlIO.NoInnoDBStatusFound"));
                }
            } catch (SQLException | CJException ex) {
                errorBuf.append("\n\n");
                errorBuf.append(Messages.getString("MysqlIO.InnoDBStatusFailed"));
                errorBuf.append("\n\n");
                errorBuf.append(Util.stackTraceToString(ex));
            } finally {
                if (rs != null) {
                    rs.close();
                }
            }
        }

        if (this.connection.getIncludeThreadDumpInDeadlockExceptions()) {
            errorBuf.append("\n\n*** Java threads running at time of deadlock ***\n\n");

            ThreadMXBean threadMBean = ManagementFactory.getThreadMXBean();
            long[] threadIds = threadMBean.getAllThreadIds();

            ThreadInfo[] threads = threadMBean.getThreadInfo(threadIds, Integer.MAX_VALUE);
            List<ThreadInfo> activeThreads = new ArrayList<ThreadInfo>();

            for (ThreadInfo info : threads) {
                if (info != null) {
                    activeThreads.add(info);
                }
            }

            for (ThreadInfo threadInfo : activeThreads) {
                // "Thread-60" daemon prio=1 tid=0x093569c0 nid=0x1b99 in Object.wait()

                errorBuf.append('"');
                errorBuf.append(threadInfo.getThreadName());
                errorBuf.append("\" tid=");
                errorBuf.append(threadInfo.getThreadId());
                errorBuf.append(" ");
                errorBuf.append(threadInfo.getThreadState());

                if (threadInfo.getLockName() != null) {
                    errorBuf.append(" on lock=" + threadInfo.getLockName());
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
                    errorBuf.append(stackTrace[0].getClassName());
                    errorBuf.append(".");
                    errorBuf.append(stackTrace[0].getMethodName());
                    errorBuf.append("()");
                }

                errorBuf.append("\n");

                if (threadInfo.getLockOwnerName() != null) {
                    errorBuf.append("\t owned by " + threadInfo.getLockOwnerName() + " Id=" + threadInfo.getLockOwnerId());
                    errorBuf.append("\n");
                }

                for (int j = 0; j < stackTrace.length; j++) {
                    StackTraceElement ste = stackTrace[j];
                    errorBuf.append("\tat " + ste.toString());
                    errorBuf.append("\n");
                }
            }
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
     * @param resultSetType
     * @param resultSetConcurrency
     * @param streamResults
     * @param catalog
     * @param unpackFieldInfo
     *            should we read MYSQL_FIELD info (if available)?
     * 
     * @throws SQLException
     */
    public final ResultSetInternalMethods sqlQueryDirect(StatementImpl callingStatement, String query, String characterEncoding, Buffer queryPacket,
            int maxRows, int resultSetType, int resultSetConcurrency, boolean streamResults, String catalog, Field[] cachedMetadata) throws SQLException {
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

            if (this.connection.getIncludeThreadNamesAsStatementComment()) {
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

                this.sendPacket.writeByte((byte) MysqlDefs.QUERY);

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
            Buffer resultPacket = sendCommand(MysqlDefs.QUERY, null, queryPacket, false, null, 0);

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
                        logSlow = queryTime > this.connection.getSlowQueryThresholdMillis();
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

                    if (oldPacketPosition > this.connection.getMaxQuerySizeToLog()) {
                        extractPosition = this.connection.getMaxQuerySizeToLog() + 1;
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

                if (this.connection.getExplainSlowQueries()) {
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
                scanForAndThrowDataTruncation();
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
                        SQLException cause = null;

                        if (callingStatement.wasCancelledByTimeout) {
                            cause = new MySQLTimeoutException();
                        } else {
                            cause = new MySQLStatementCancelledException();
                        }

                        callingStatement.resetCancelledState();

                        throw cause;
                    }
                }
            }

            throw sqlEx;
        } finally {
            this.statementExecutionDepth--;
        }
    }

    public boolean shouldIntercept() {
        return this.statementInterceptors != null;
    }

    public ResultSetInternalMethods invokeStatementInterceptorsPre(String sql, Statement interceptedStatement, boolean forceExecute) throws SQLException {
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

                ResultSetInternalMethods interceptedResultSet = interceptor.preProcess(sqlToInterceptor, interceptedStatement, this.connection);

                if (interceptedResultSet != null) {
                    previousResultSet = interceptedResultSet;
                }
            }
        }

        return previousResultSet;
    }

    public ResultSetInternalMethods invokeStatementInterceptorsPost(String sql, Statement interceptedStatement, ResultSetInternalMethods originalResultSet,
            boolean forceExecute, Exception statementException) throws SQLException {

        for (int i = 0, s = this.statementInterceptors.size(); i < s; i++) {
            StatementInterceptorV2 interceptor = this.statementInterceptors.get(i);

            boolean executeTopLevelOnly = interceptor.executeTopLevelOnly();
            boolean shouldExecute = (executeTopLevelOnly && (this.statementExecutionDepth == 1 || forceExecute)) || (!executeTopLevelOnly);

            if (shouldExecute) {
                String sqlToInterceptor = sql;

                ResultSetInternalMethods interceptedResultSet = interceptor.postProcess(sqlToInterceptor, interceptedStatement, originalResultSet,
                        this.connection, this.warningCount, this.queryNoIndexUsed, this.queryBadIndexUsed, statementException);

                if (interceptedResultSet != null) {
                    originalResultSet = interceptedResultSet;
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

    public void scanForAndThrowDataTruncation() throws SQLException {
        if ((this.streamingData == null) && this.connection.getJdbcCompliantTruncation() && this.warningCount > 0) {
            SQLError.convertShowWarningsToSQLWarnings(this.connection, this.warningCount, true);
        }
    }

    /**
     * Runs an 'EXPLAIN' on the given query and dumps the results to the log
     * 
     * @param querySQL
     * @param truncatedQuery
     * 
     * @throws SQLException
     */
    public void explainSlowQuery(byte[] querySQL, String truncatedQuery) throws SQLException {
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
            } catch (SQLException sqlEx) {
            } catch (Exception ex) {
                throw SQLError.createSQLException(ex.getMessage(), SQLError.SQL_STATE_GENERAL_ERROR, ex, getExceptionInterceptor());
            } finally {
                if (rs != null) {
                    rs.close();
                }

                if (stmt != null) {
                    stmt.close();
                }
            }
        }
    }

    /**
     * Reads and discards a single MySQL packet from the input stream.
     * 
     * @throws SQLException
     *             if the network fails while skipping the
     *             packet.
     */
    protected final void skipPacket() throws SQLException {
        try {

            int lengthRead = readFully(this.physicalConnection.getMysqlInput(), this.packetHeaderBuf, 0, 4);

            if (lengthRead < 4) {
                this.physicalConnection.forceClose();
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

            skipFully(this.physicalConnection.getMysqlInput(), packetLength);
        } catch (IOException ioEx) {
            throw SQLError.createCommunicationsException(this.connection, this.packetSentTimeHolder.getLastPacketSentTime(), this.lastPacketReceivedTimeMs,
                    ioEx, getExceptionInterceptor());
        } catch (SQLException ex) {
            throw ex;
        } catch (Exception ex) {
            throw SQLError.createSQLException(ex.getMessage(), SQLError.SQL_STATE_GENERAL_ERROR, ex, getExceptionInterceptor());
        } catch (OutOfMemoryError oom) {
            try {
                this.connection.realClose(false, false, true, oom);
            } catch (Exception ex) {
            }
            throw oom;
        }
    }

    protected final long skipFully(InputStream in, long len) throws IOException {
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
     * @throws SQLException
     */
    public final void quit() throws SQLException {
        try {
            // we're not going to read the response, fixes BUG#56979 Improper connection closing logic leads to TIME_WAIT sockets on server

            try {
                if (!this.physicalConnection.getMysqlSocket().isClosed()) {
                    try {
                        this.physicalConnection.getMysqlSocket().shutdownInput();
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
            packet.writeByte((byte) MysqlDefs.QUIT);
            send(packet, packet.getPosition());
        } finally {
            this.physicalConnection.forceClose();
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
        this.slowQueryThreshold = this.connection.getSlowQueryThresholdMillis();

        if (this.connection.getUseNanosForElapsedTime()) {
            long nanosThreshold = this.connection.getSlowQueryThresholdNanos();

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
     * @param userName
     * @param password
     * @param database
     * 
     */
    public void changeUser(String userName, String password, String database) {
        this.packetSequence = -1;

        this.getSession().changeUser(userName, password, database);
    }

    /**
     * Determines if the database charset is the same as the platform charset
     */
    public void checkForCharsetMismatch() {
        if (this.connection.getCharacterEncoding() != null) {
            String encodingToCheck = jvmPlatformCharset;

            if (encodingToCheck == null) {
                encodingToCheck = Constants.PLATFORM_ENCODING;
            }

            if (encodingToCheck == null) {
                this.platformDbCharsetMatches = false;
            } else {
                this.platformDbCharsetMatches = encodingToCheck.equals(this.connection.getCharacterEncoding());
            }
        }
    }

    /**
     * Get the version of the MySQL server we are talking to.
     */
    public final ServerVersion getServerVersion() {
        return this.getSession().getSessionState().getServerVersion();
    }

    /**
     * Is the version of the MySQL server we are connected to the given
     * version?
     * 
     * @param version
     *            the version to check for
     * 
     * @return true if the version of the MySQL server we are connected is the
     *         given version
     */
    boolean isVersion(ServerVersion version) {
        return this.getServerVersion().equals(version);
    }

    /**
     * Does the version of the MySQL server we are connected to meet the given
     * minimums?
     * 
     * @param major
     * @param minor
     * @param subminor
     */
    public boolean versionMeetsMinimum(int major, int minor, int subminor) {
        return this.getServerVersion().meetsMinimum(new ServerVersion(major, minor, subminor));
    }

    protected void setServerSlowQueryFlags() {
        SessionState state = this.getSession().getSessionState();
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

    public void setStatementInterceptors(List<StatementInterceptorV2> statementInterceptors) {
        this.statementInterceptors = statementInterceptors.isEmpty() ? null : statementInterceptors;
    }

    public void setSocketTimeout(int milliseconds) throws SQLException {
        try {
            this.physicalConnection.getMysqlSocket().setSoTimeout(milliseconds);
        } catch (SocketException e) {
            SQLException sqlEx = SQLError.createSQLException(Messages.getString("MysqlIO.112"), SQLError.SQL_STATE_ILLEGAL_ARGUMENT, e,
                    getExceptionInterceptor());
            throw sqlEx;
        }
    }

    public void releaseResources() {
        if (this.compressedPacketSender != null) {
            this.compressedPacketSender.stop();
        }
    }

    @Override
    public MysqlJdbcConnection getConnection() {
        return this.connection;
    }

    public void setConnection(MysqlJdbcConnection connection) {
        this.connection = connection;
    }

}
