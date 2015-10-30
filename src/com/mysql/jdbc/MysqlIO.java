/*
  Copyright (c) 2002, 2015, Oracle and/or its affiliates. All rights reserved.

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

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.lang.ref.SoftReference;
import java.math.BigInteger;
import java.net.MalformedURLException;
import java.net.Socket;
import java.net.SocketException;
import java.net.URL;
import java.security.NoSuchAlgorithmException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.zip.Deflater;

import com.mysql.jdbc.authentication.MysqlClearPasswordPlugin;
import com.mysql.jdbc.authentication.MysqlNativePasswordPlugin;
import com.mysql.jdbc.authentication.MysqlOldPasswordPlugin;
import com.mysql.jdbc.authentication.Sha256PasswordPlugin;
import com.mysql.jdbc.exceptions.MySQLStatementCancelledException;
import com.mysql.jdbc.exceptions.MySQLTimeoutException;
import com.mysql.jdbc.log.LogUtils;
import com.mysql.jdbc.profiler.ProfilerEvent;
import com.mysql.jdbc.profiler.ProfilerEventHandler;
import com.mysql.jdbc.util.ReadAheadInputStream;
import com.mysql.jdbc.util.ResultSetUtil;

/**
 * This class is used by Connection for communicating with the MySQL server.
 */
public class MysqlIO {
    private static final String CODE_PAGE_1252 = "Cp1252";
    protected static final int NULL_LENGTH = ~0;
    protected static final int COMP_HEADER_LENGTH = 3;
    protected static final int MIN_COMPRESS_LEN = 50;
    protected static final int HEADER_LENGTH = 4;
    protected static final int AUTH_411_OVERHEAD = 33;
    public static final int SEED_LENGTH = 20;
    private static int maxBufferSize = 65535;

    private static final String NONE = "none";

    private static final int CLIENT_LONG_PASSWORD = 0x00000001; /* new more secure passwords */
    private static final int CLIENT_FOUND_ROWS = 0x00000002;
    private static final int CLIENT_LONG_FLAG = 0x00000004; /* Get all column flags */
    protected static final int CLIENT_CONNECT_WITH_DB = 0x00000008;
    private static final int CLIENT_COMPRESS = 0x00000020; /* Can use compression protcol */
    private static final int CLIENT_LOCAL_FILES = 0x00000080; /* Can use LOAD DATA LOCAL */
    private static final int CLIENT_PROTOCOL_41 = 0x00000200; // for > 4.1.1
    private static final int CLIENT_INTERACTIVE = 0x00000400;
    protected static final int CLIENT_SSL = 0x00000800;
    private static final int CLIENT_TRANSACTIONS = 0x00002000; // Client knows about transactions
    protected static final int CLIENT_RESERVED = 0x00004000; // for 4.1.0 only
    protected static final int CLIENT_SECURE_CONNECTION = 0x00008000;
    private static final int CLIENT_MULTI_STATEMENTS = 0x00010000; // Enable/disable multiquery support
    private static final int CLIENT_MULTI_RESULTS = 0x00020000; // Enable/disable multi-results
    private static final int CLIENT_PLUGIN_AUTH = 0x00080000;
    private static final int CLIENT_CONNECT_ATTRS = 0x00100000;
    private static final int CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA = 0x00200000;
    private static final int CLIENT_CAN_HANDLE_EXPIRED_PASSWORD = 0x00400000;

    private static final int SERVER_STATUS_IN_TRANS = 1;
    private static final int SERVER_STATUS_AUTOCOMMIT = 2; // Server in auto_commit mode
    static final int SERVER_MORE_RESULTS_EXISTS = 8; // Multi query - next query exists
    private static final int SERVER_QUERY_NO_GOOD_INDEX_USED = 16;
    private static final int SERVER_QUERY_NO_INDEX_USED = 32;
    private static final int SERVER_QUERY_WAS_SLOW = 2048;
    private static final int SERVER_STATUS_CURSOR_EXISTS = 64;
    private static final String FALSE_SCRAMBLE = "xxxxxxxx";
    protected static final int MAX_QUERY_SIZE_TO_LOG = 1024; // truncate logging of queries at 1K
    protected static final int MAX_QUERY_SIZE_TO_EXPLAIN = 1024 * 1024; // don't explain queries above 1MB
    protected static final int INITIAL_PACKET_SIZE = 1024;
    /**
     * We store the platform 'encoding' here, only used to avoid munging filenames for LOAD DATA LOCAL INFILE...
     */
    private static String jvmPlatformCharset = null;

    /**
     * We need to have a 'marker' for all-zero datetimes so that ResultSet can decide what to do based on connection setting
     */
    protected final static String ZERO_DATE_VALUE_MARKER = "0000-00-00";
    protected final static String ZERO_DATETIME_VALUE_MARKER = "0000-00-00 00:00:00";

    private static final String EXPLAINABLE_STATEMENT = "SELECT";
    private static final String[] EXPLAINABLE_STATEMENT_EXTENSION = new String[] { "INSERT", "UPDATE", "REPLACE", "DELETE" };

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

    /** Max number of bytes to dump when tracing the protocol */
    private final static int MAX_PACKET_DUMP_LENGTH = 1024;
    private boolean packetSequenceReset = false;
    protected int serverCharsetIndex;

    //
    // Use this when reading in rows to avoid thousands of new() calls, because the byte arrays just get copied out of the packet anyway
    //
    private Buffer reusablePacket = null;
    private Buffer sendPacket = null;
    private Buffer sharedSendPacket = null;

    /** Data to the server */
    protected BufferedOutputStream mysqlOutput = null;
    protected MySQLConnection connection;
    private Deflater deflater = null;
    protected InputStream mysqlInput = null;
    private LinkedList<StringBuilder> packetDebugRingBuffer = null;
    private RowData streamingData = null;

    /** The connection to the server */
    public Socket mysqlConnection = null;
    protected SocketFactory socketFactory = null;

    //
    // Packet used for 'LOAD DATA LOCAL INFILE'
    //
    // We use a SoftReference, so that we don't penalize intermittent use of this feature
    //
    private SoftReference<Buffer> loadFileBufRef;

    //
    // Used to send large packets to the server versions 4+
    // We use a SoftReference, so that we don't penalize intermittent use of this feature
    //
    private SoftReference<Buffer> splitBufRef;
    private SoftReference<Buffer> compressBufRef;
    protected String host = null;
    protected String seed;
    private String serverVersion = null;
    private String socketFactoryClassName = null;
    private byte[] packetHeaderBuf = new byte[4];
    private boolean colDecimalNeedsBump = false; // do we need to increment the colDecimal flag?
    private boolean hadWarnings = false;
    private boolean has41NewNewProt = false;

    /** Does the server support long column info? */
    private boolean hasLongColumnInfo = false;
    private boolean isInteractiveClient = false;
    private boolean logSlowQueries = false;

    /**
     * Does the character set of this connection match the character set of the
     * platform
     */
    private boolean platformDbCharsetMatches = true; // changed once we've connected.
    private boolean profileSql = false;
    private boolean queryBadIndexUsed = false;
    private boolean queryNoIndexUsed = false;
    private boolean serverQueryWasSlow = false;

    /** Should we use 4.1 protocol extensions? */
    private boolean use41Extensions = false;
    private boolean useCompression = false;
    private boolean useNewLargePackets = false;
    private boolean useNewUpdateCounts = false; // should we use the new larger update counts?
    private byte packetSequence = 0;
    private byte compressedPacketSequence = 0;
    private byte readPacketSequence = -1;
    private boolean checkPacketSequence = false;
    private byte protocolVersion = 0;
    private int maxAllowedPacket = 1024 * 1024;
    protected int maxThreeBytes = 255 * 255 * 255;
    protected int port = 3306;
    protected int serverCapabilities;
    private int serverMajorVersion = 0;
    private int serverMinorVersion = 0;
    private int oldServerStatus = 0;
    private int serverStatus = 0;
    private int serverSubMinorVersion = 0;
    private int warningCount = 0;
    protected long clientParam = 0;
    protected long lastPacketSentTimeMs = 0;
    protected long lastPacketReceivedTimeMs = 0;
    private boolean traceProtocol = false;
    private boolean enablePacketDebug = false;
    private boolean useConnectWithDb;
    private boolean needToGrabQueryFromPacket;
    private boolean autoGenerateTestcaseScript;
    private long threadId;
    private boolean useNanosForElapsedTime;
    private long slowQueryThreshold;
    private String queryTimingUnits;
    private boolean useDirectRowUnpack = true;
    private int useBufferRowSizeThreshold;
    private int commandCount = 0;
    private List<StatementInterceptorV2> statementInterceptors;
    private ExceptionInterceptor exceptionInterceptor;
    private int authPluginDataLength = 0;

    /**
     * Constructor: Connect to the MySQL server and setup a stream connection.
     * 
     * @param host
     *            the hostname to connect to
     * @param port
     *            the port number that the server is listening on
     * @param props
     *            the Properties from DriverManager.getConnection()
     * @param socketFactoryClassName
     *            the socket factory to use
     * @param conn
     *            the Connection that is creating us
     * @param socketTimeout
     *            the timeout to set for the socket (0 means no
     *            timeout)
     * 
     * @throws IOException
     *             if an IOException occurs during connect.
     * @throws SQLException
     *             if a database access error occurs.
     */
    public MysqlIO(String host, int port, Properties props, String socketFactoryClassName, MySQLConnection conn, int socketTimeout,
            int useBufferRowSizeThreshold) throws IOException, SQLException {
        this.connection = conn;

        if (this.connection.getEnablePacketDebug()) {
            this.packetDebugRingBuffer = new LinkedList<StringBuilder>();
        }
        this.traceProtocol = this.connection.getTraceProtocol();

        this.useAutoSlowLog = this.connection.getAutoSlowLog();

        this.useBufferRowSizeThreshold = useBufferRowSizeThreshold;
        this.useDirectRowUnpack = this.connection.getUseDirectRowUnpack();

        this.logSlowQueries = this.connection.getLogSlowQueries();

        this.reusablePacket = new Buffer(INITIAL_PACKET_SIZE);
        this.sendPacket = new Buffer(INITIAL_PACKET_SIZE);

        this.port = port;
        this.host = host;

        this.socketFactoryClassName = socketFactoryClassName;
        this.socketFactory = createSocketFactory();
        this.exceptionInterceptor = this.connection.getExceptionInterceptor();

        try {
            this.mysqlConnection = this.socketFactory.connect(this.host, this.port, props);

            if (socketTimeout != 0) {
                try {
                    this.mysqlConnection.setSoTimeout(socketTimeout);
                } catch (Exception ex) {
                    /* Ignore if the platform does not support it */
                }
            }

            this.mysqlConnection = this.socketFactory.beforeHandshake();

            if (this.connection.getUseReadAheadInput()) {
                this.mysqlInput = new ReadAheadInputStream(this.mysqlConnection.getInputStream(), 16384, this.connection.getTraceProtocol(),
                        this.connection.getLog());
            } else if (this.connection.useUnbufferedInput()) {
                this.mysqlInput = this.mysqlConnection.getInputStream();
            } else {
                this.mysqlInput = new BufferedInputStream(this.mysqlConnection.getInputStream(), 16384);
            }

            this.mysqlOutput = new BufferedOutputStream(this.mysqlConnection.getOutputStream(), 16384);

            this.isInteractiveClient = this.connection.getInteractiveClient();
            this.profileSql = this.connection.getProfileSql();
            this.autoGenerateTestcaseScript = this.connection.getAutoGenerateTestcaseScript();

            this.needToGrabQueryFromPacket = (this.profileSql || this.logSlowQueries || this.autoGenerateTestcaseScript);

            if (this.connection.getUseNanosForElapsedTime() && TimeUtil.nanoTimeAvailable()) {
                this.useNanosForElapsedTime = true;

                this.queryTimingUnits = Messages.getString("Nanoseconds");
            } else {
                this.queryTimingUnits = Messages.getString("Milliseconds");
            }

            if (this.connection.getLogSlowQueries()) {
                calculateSlowQueryThreshold();
            }
        } catch (IOException ioEx) {
            throw SQLError.createCommunicationsException(this.connection, 0, 0, ioEx, getExceptionInterceptor());
        }
    }

    /**
     * Does the server send back extra column info?
     * 
     * @return true if so
     */
    public boolean hasLongColumnInfo() {
        return this.hasLongColumnInfo;
    }

    protected boolean isDataAvailable() throws SQLException {
        try {
            return this.mysqlInput.available() > 0;
        } catch (IOException ioEx) {
            throw SQLError.createCommunicationsException(this.connection, this.lastPacketSentTimeMs, this.lastPacketReceivedTimeMs, ioEx,
                    getExceptionInterceptor());
        }
    }

    /**
     * @return Returns the lastPacketSentTimeMs.
     */
    protected long getLastPacketSentTimeMs() {
        return this.lastPacketSentTimeMs;
    }

    protected long getLastPacketReceivedTimeMs() {
        return this.lastPacketReceivedTimeMs;
    }

    /**
     * Build a result set. Delegates to buildResultSetWithRows() to build a
     * JDBC-version-specific ResultSet, given rows as byte data, and field
     * information.
     * 
     * @param callingStatement
     * @param columnCount
     *            the number of columns in the result set
     * @param maxRows
     *            the maximum number of rows to read (-1 means all rows)
     * @param resultSetType
     *            (TYPE_FORWARD_ONLY, TYPE_SCROLL_????)
     * @param resultSetConcurrency
     *            the type of result set (CONCUR_UPDATABLE or
     *            READ_ONLY)
     * @param streamResults
     *            should the result set be read all at once, or
     *            streamed?
     * @param catalog
     *            the database name in use when the result set was created
     * @param isBinaryEncoded
     *            is this result set in native encoding?
     * @param unpackFieldInfo
     *            should we read MYSQL_FIELD info (if available)?
     * 
     * @return a result set
     * 
     * @throws SQLException
     *             if a database access error occurs
     */
    protected ResultSetImpl getResultSet(StatementImpl callingStatement, long columnCount, int maxRows, int resultSetType, int resultSetConcurrency,
            boolean streamResults, String catalog, boolean isBinaryEncoded, Field[] metadataFromCache) throws SQLException {
        Buffer packet; // The packet from the server
        Field[] fields = null;

        // Read in the column information

        if (metadataFromCache == null /* we want the metadata from the server */) {
            fields = new Field[(int) columnCount];

            for (int i = 0; i < columnCount; i++) {
                Buffer fieldPacket = null;

                fieldPacket = readPacket();
                fields[i] = unpackField(fieldPacket, false);
            }
        } else {
            for (int i = 0; i < columnCount; i++) {
                skipPacket();
            }
        }

        packet = reuseAndReadPacket(this.reusablePacket);

        readServerStatusForResultSets(packet);

        //
        // Handle cursor-based fetch first
        //

        if (this.connection.versionMeetsMinimum(5, 0, 2) && this.connection.getUseCursorFetch() && isBinaryEncoded && callingStatement != null
                && callingStatement.getFetchSize() != 0 && callingStatement.getResultSetType() == ResultSet.TYPE_FORWARD_ONLY) {
            ServerPreparedStatement prepStmt = (com.mysql.jdbc.ServerPreparedStatement) callingStatement;

            boolean usingCursor = true;

            //
            // Server versions 5.0.5 or newer will only open a cursor and set this flag if they can, otherwise they punt and go back to mysql_store_results()
            // behavior
            //

            if (this.connection.versionMeetsMinimum(5, 0, 5)) {
                usingCursor = (this.serverStatus & SERVER_STATUS_CURSOR_EXISTS) != 0;
            }

            if (usingCursor) {
                RowData rows = new RowDataCursor(this, prepStmt, fields);

                ResultSetImpl rs = buildResultSetWithRows(callingStatement, catalog, fields, rows, resultSetType, resultSetConcurrency, isBinaryEncoded);

                if (usingCursor) {
                    rs.setFetchSize(callingStatement.getFetchSize());
                }

                return rs;
            }
        }

        RowData rowData = null;

        if (!streamResults) {
            rowData = readSingleRowSet(columnCount, maxRows, resultSetConcurrency, isBinaryEncoded, (metadataFromCache == null) ? fields : metadataFromCache);
        } else {
            rowData = new RowDataDynamic(this, (int) columnCount, (metadataFromCache == null) ? fields : metadataFromCache, isBinaryEncoded);
            this.streamingData = rowData;
        }

        ResultSetImpl rs = buildResultSetWithRows(callingStatement, catalog, (metadataFromCache == null) ? fields : metadataFromCache, rowData, resultSetType,
                resultSetConcurrency, isBinaryEncoded);

        return rs;
    }

    // We do this to break the chain between MysqlIO and Connection, so that we can have PhantomReferences on connections that let the driver clean up the
    // socket connection without having to use finalize() somewhere (which although more straightforward, is horribly inefficent).
    protected NetworkResources getNetworkResources() {
        return new NetworkResources(this.mysqlConnection, this.mysqlInput, this.mysqlOutput);
    }

    /**
     * Forcibly closes the underlying socket to MySQL.
     */
    protected final void forceClose() {
        try {
            getNetworkResources().forceClose();
        } finally {
            this.mysqlConnection = null;
            this.mysqlInput = null;
            this.mysqlOutput = null;
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

            int lengthRead = readFully(this.mysqlInput, this.packetHeaderBuf, 0, 4);

            if (lengthRead < 4) {
                forceClose();
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

            skipFully(this.mysqlInput, packetLength);
        } catch (IOException ioEx) {
            throw SQLError.createCommunicationsException(this.connection, this.lastPacketSentTimeMs, this.lastPacketReceivedTimeMs, ioEx,
                    getExceptionInterceptor());
        } catch (OutOfMemoryError oom) {
            try {
                this.connection.realClose(false, false, true, oom);
            } catch (Exception ex) {
            }
            throw oom;
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
    protected final Buffer readPacket() throws SQLException {
        try {

            int lengthRead = readFully(this.mysqlInput, this.packetHeaderBuf, 0, 4);

            if (lengthRead < 4) {
                forceClose();
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
            int numBytesRead = readFully(this.mysqlInput, buffer, 0, packetLength);

            if (numBytesRead != packetLength) {
                throw new IOException("Short read, expected " + packetLength + " bytes, only read " + numBytesRead);
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
            throw SQLError.createCommunicationsException(this.connection, this.lastPacketSentTimeMs, this.lastPacketReceivedTimeMs, ioEx,
                    getExceptionInterceptor());
        } catch (OutOfMemoryError oom) {
            try {
                this.connection.realClose(false, false, true, oom);
            } catch (Exception ex) {
            }
            throw oom;
        }
    }

    /**
     * Unpacks the Field information from the given packet. Understands pre 4.1
     * and post 4.1 server version field packet structures.
     * 
     * @param packet
     *            the packet containing the field information
     * @param extractDefaultValues
     *            should default values be extracted?
     * 
     * @return the unpacked field
     * 
     * @throws SQLException
     */
    protected final Field unpackField(Buffer packet, boolean extractDefaultValues) throws SQLException {
        if (this.use41Extensions) {
            // we only store the position of the string and
            // materialize only if needed...
            if (this.has41NewNewProt) {
                // Not used yet, 5.0?
                int catalogNameStart = packet.getPosition() + 1;
                int catalogNameLength = packet.fastSkipLenString();
                catalogNameStart = adjustStartForFieldLength(catalogNameStart, catalogNameLength);
            }

            int databaseNameStart = packet.getPosition() + 1;
            int databaseNameLength = packet.fastSkipLenString();
            databaseNameStart = adjustStartForFieldLength(databaseNameStart, databaseNameLength);

            int tableNameStart = packet.getPosition() + 1;
            int tableNameLength = packet.fastSkipLenString();
            tableNameStart = adjustStartForFieldLength(tableNameStart, tableNameLength);

            // orgTableName is never used so skip
            int originalTableNameStart = packet.getPosition() + 1;
            int originalTableNameLength = packet.fastSkipLenString();
            originalTableNameStart = adjustStartForFieldLength(originalTableNameStart, originalTableNameLength);

            // we only store the position again...
            int nameStart = packet.getPosition() + 1;
            int nameLength = packet.fastSkipLenString();

            nameStart = adjustStartForFieldLength(nameStart, nameLength);

            // orgColName is not required so skip...
            int originalColumnNameStart = packet.getPosition() + 1;
            int originalColumnNameLength = packet.fastSkipLenString();
            originalColumnNameStart = adjustStartForFieldLength(originalColumnNameStart, originalColumnNameLength);

            packet.readByte();

            short charSetNumber = (short) packet.readInt();

            long colLength = 0;

            if (this.has41NewNewProt) {
                colLength = packet.readLong();
            } else {
                colLength = packet.readLongInt();
            }

            int colType = packet.readByte() & 0xff;

            short colFlag = 0;

            if (this.hasLongColumnInfo) {
                colFlag = (short) packet.readInt();
            } else {
                colFlag = (short) (packet.readByte() & 0xff);
            }

            int colDecimals = packet.readByte() & 0xff;

            int defaultValueStart = -1;
            int defaultValueLength = -1;

            if (extractDefaultValues) {
                defaultValueStart = packet.getPosition() + 1;
                defaultValueLength = packet.fastSkipLenString();
            }

            Field field = new Field(this.connection, packet.getByteBuffer(), databaseNameStart, databaseNameLength, tableNameStart, tableNameLength,
                    originalTableNameStart, originalTableNameLength, nameStart, nameLength, originalColumnNameStart, originalColumnNameLength, colLength,
                    colType, colFlag, colDecimals, defaultValueStart, defaultValueLength, charSetNumber);

            return field;
        }

        int tableNameStart = packet.getPosition() + 1;
        int tableNameLength = packet.fastSkipLenString();
        tableNameStart = adjustStartForFieldLength(tableNameStart, tableNameLength);

        int nameStart = packet.getPosition() + 1;
        int nameLength = packet.fastSkipLenString();
        nameStart = adjustStartForFieldLength(nameStart, nameLength);

        int colLength = packet.readnBytes();
        int colType = packet.readnBytes();
        packet.readByte(); // We know it's currently 2

        short colFlag = 0;

        if (this.hasLongColumnInfo) {
            colFlag = (short) (packet.readInt());
        } else {
            colFlag = (short) (packet.readByte() & 0xff);
        }

        int colDecimals = (packet.readByte() & 0xff);

        if (this.colDecimalNeedsBump) {
            colDecimals++;
        }

        Field field = new Field(this.connection, packet.getByteBuffer(), nameStart, nameLength, tableNameStart, tableNameLength, colLength, colType, colFlag,
                colDecimals);

        return field;
    }

    private int adjustStartForFieldLength(int nameStart, int nameLength) {
        if (nameLength < 251) {
            return nameStart;
        }

        if (nameLength >= 251 && nameLength < 65536) {
            return nameStart + 2;
        }

        if (nameLength >= 65536 && nameLength < 16777216) {
            return nameStart + 3;
        }

        return nameStart + 8;
    }

    protected boolean isSetNeededForAutoCommitMode(boolean autoCommitFlag) {
        if (this.use41Extensions && this.connection.getElideSetAutoCommits()) {
            boolean autoCommitModeOnServer = ((this.serverStatus & SERVER_STATUS_AUTOCOMMIT) != 0);

            if (!autoCommitFlag && versionMeetsMinimum(5, 0, 0)) {
                // Just to be safe, check if a transaction is in progress on the server....
                // if so, then we must be in autoCommit == false
                // therefore return the opposite of transaction status
                boolean inTransactionOnServer = ((this.serverStatus & SERVER_STATUS_IN_TRANS) != 0);

                return !inTransactionOnServer;
            }

            return autoCommitModeOnServer != autoCommitFlag;
        }

        return true;
    }

    protected boolean inTransactionOnServer() {
        return (this.serverStatus & SERVER_STATUS_IN_TRANS) != 0;
    }

    /**
     * Re-authenticates as the given user and password
     * 
     * @param userName
     * @param password
     * @param database
     * 
     * @throws SQLException
     */
    protected void changeUser(String userName, String password, String database) throws SQLException {
        this.packetSequence = -1;
        this.compressedPacketSequence = -1;

        int passwordLength = 16;
        int userLength = (userName != null) ? userName.length() : 0;
        int databaseLength = (database != null) ? database.length() : 0;

        int packLength = ((userLength + passwordLength + databaseLength) * 3) + 7 + HEADER_LENGTH + AUTH_411_OVERHEAD;

        if ((this.serverCapabilities & CLIENT_PLUGIN_AUTH) != 0) {

            proceedHandshakeWithPluggableAuthentication(userName, password, database, null);

        } else if ((this.serverCapabilities & CLIENT_SECURE_CONNECTION) != 0) {
            Buffer changeUserPacket = new Buffer(packLength + 1);
            changeUserPacket.writeByte((byte) MysqlDefs.COM_CHANGE_USER);

            if (versionMeetsMinimum(4, 1, 1)) {
                secureAuth411(changeUserPacket, packLength, userName, password, database, false);
            } else {
                secureAuth(changeUserPacket, packLength, userName, password, database, false);
            }
        } else {
            // Passwords can be 16 chars long
            Buffer packet = new Buffer(packLength);
            packet.writeByte((byte) MysqlDefs.COM_CHANGE_USER);

            // User/Password data
            packet.writeString(userName);

            if (this.protocolVersion > 9) {
                packet.writeString(Util.newCrypt(password, this.seed, this.connection.getPasswordCharacterEncoding()));
            } else {
                packet.writeString(Util.oldCrypt(password, this.seed));
            }

            boolean localUseConnectWithDb = this.useConnectWithDb && (database != null && database.length() > 0);

            if (localUseConnectWithDb) {
                packet.writeString(database);
            } else {
                //Not needed, old server does not require \0
                //packet.writeString("");
            }

            send(packet, packet.getPosition());
            checkErrorPacket();

            if (!localUseConnectWithDb) {
                changeDatabaseTo(database);
            }
        }
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
     * Determines if the database charset is the same as the platform charset
     */
    protected void checkForCharsetMismatch() {
        if (this.connection.getUseUnicode() && (this.connection.getEncoding() != null)) {
            String encodingToCheck = jvmPlatformCharset;

            if (encodingToCheck == null) {
                encodingToCheck = System.getProperty("file.encoding");
            }

            if (encodingToCheck == null) {
                this.platformDbCharsetMatches = false;
            } else {
                this.platformDbCharsetMatches = encodingToCheck.equals(this.connection.getEncoding());
            }
        }
    }

    protected void clearInputStream() throws SQLException {
        try {
            int len;

            // Due to a bug in some older Linux kernels (fixed after the patch "tcp: fix FIONREAD/SIOCINQ"), our SocketInputStream.available() may return 1 even
            // if there is no data in the Stream, so, we need to check if InputStream.skip() actually skipped anything.
            while ((len = this.mysqlInput.available()) > 0 && this.mysqlInput.skip(len) > 0) {
                continue;
            }
        } catch (IOException ioEx) {
            throw SQLError.createCommunicationsException(this.connection, this.lastPacketSentTimeMs, this.lastPacketReceivedTimeMs, ioEx,
                    getExceptionInterceptor());
        }
    }

    protected void resetReadPacketSequence() {
        this.readPacketSequence = 0;
    }

    protected void dumpPacketRingBuffer() throws SQLException {
        if ((this.packetDebugRingBuffer != null) && this.connection.getEnablePacketDebug()) {
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

    /**
     * Runs an 'EXPLAIN' on the given query and dumps the results to the log
     * 
     * @param querySQL
     * @param truncatedQuery
     * 
     * @throws SQLException
     */
    protected void explainSlowQuery(byte[] querySQL, String truncatedQuery) throws SQLException {
        if (StringUtils.startsWithIgnoreCaseAndWs(truncatedQuery, EXPLAINABLE_STATEMENT)
                || (versionMeetsMinimum(5, 6, 3) && StringUtils.startsWithIgnoreCaseAndWs(truncatedQuery, EXPLAINABLE_STATEMENT_EXTENSION) != -1)) {

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

    static int getMaxBuf() {
        return maxBufferSize;
    }

    /**
     * Get the major version of the MySQL server we are talking to.
     */
    final int getServerMajorVersion() {
        return this.serverMajorVersion;
    }

    /**
     * Get the minor version of the MySQL server we are talking to.
     */
    final int getServerMinorVersion() {
        return this.serverMinorVersion;
    }

    /**
     * Get the sub-minor version of the MySQL server we are talking to.
     */
    final int getServerSubMinorVersion() {
        return this.serverSubMinorVersion;
    }

    /**
     * Get the version string of the server we are talking to
     */
    String getServerVersion() {
        return this.serverVersion;
    }

    /**
     * Initialize communications with the MySQL server. Handles logging on, and
     * handling initial connection errors.
     * 
     * @param user
     * @param password
     * @param database
     * 
     * @throws SQLException
     * @throws CommunicationsException
     */
    void doHandshake(String user, String password, String database) throws SQLException {
        // Read the first packet
        this.checkPacketSequence = false;
        this.readPacketSequence = 0;

        Buffer buf = readPacket();

        // Get the protocol version
        this.protocolVersion = buf.readByte();

        if (this.protocolVersion == -1) {
            try {
                this.mysqlConnection.close();
            } catch (Exception e) {
                // ignore
            }

            int errno = 2000;

            errno = buf.readInt();

            String serverErrorMessage = buf.readString("ASCII", getExceptionInterceptor());

            StringBuilder errorBuf = new StringBuilder(Messages.getString("MysqlIO.10"));
            errorBuf.append(serverErrorMessage);
            errorBuf.append("\"");

            String xOpen = SQLError.mysqlToSqlState(errno, this.connection.getUseSqlStateCodes());

            throw SQLError.createSQLException(SQLError.get(xOpen) + ", " + errorBuf.toString(), xOpen, errno, getExceptionInterceptor());
        }

        this.serverVersion = buf.readString("ASCII", getExceptionInterceptor());

        // Parse the server version into major/minor/subminor
        int point = this.serverVersion.indexOf('.');

        if (point != -1) {
            try {
                int n = Integer.parseInt(this.serverVersion.substring(0, point));
                this.serverMajorVersion = n;
            } catch (NumberFormatException NFE1) {
                // ignore
            }

            String remaining = this.serverVersion.substring(point + 1, this.serverVersion.length());
            point = remaining.indexOf('.');

            if (point != -1) {
                try {
                    int n = Integer.parseInt(remaining.substring(0, point));
                    this.serverMinorVersion = n;
                } catch (NumberFormatException nfe) {
                    // ignore
                }

                remaining = remaining.substring(point + 1, remaining.length());

                int pos = 0;

                while (pos < remaining.length()) {
                    if ((remaining.charAt(pos) < '0') || (remaining.charAt(pos) > '9')) {
                        break;
                    }

                    pos++;
                }

                try {
                    int n = Integer.parseInt(remaining.substring(0, pos));
                    this.serverSubMinorVersion = n;
                } catch (NumberFormatException nfe) {
                    // ignore
                }
            }
        }

        if (versionMeetsMinimum(4, 0, 8)) {
            this.maxThreeBytes = (256 * 256 * 256) - 1;
            this.useNewLargePackets = true;
        } else {
            this.maxThreeBytes = 255 * 255 * 255;
            this.useNewLargePackets = false;
        }

        this.colDecimalNeedsBump = versionMeetsMinimum(3, 23, 0);
        this.colDecimalNeedsBump = !versionMeetsMinimum(3, 23, 15); // guess? Not noted in changelog
        this.useNewUpdateCounts = versionMeetsMinimum(3, 22, 5);

        // read connection id
        this.threadId = buf.readLong();

        if (this.protocolVersion > 9) {
            // read auth-plugin-data-part-1 (string[8])
            this.seed = buf.readString("ASCII", getExceptionInterceptor(), 8);
            // read filler ([00])
            buf.readByte();
        } else {
            // read scramble (string[NUL])
            this.seed = buf.readString("ASCII", getExceptionInterceptor());
        }

        this.serverCapabilities = 0;

        // read capability flags (lower 2 bytes)
        if (buf.getPosition() < buf.getBufLength()) {
            this.serverCapabilities = buf.readInt();
        }

        if ((versionMeetsMinimum(4, 1, 1) || ((this.protocolVersion > 9) && (this.serverCapabilities & CLIENT_PROTOCOL_41) != 0))) {

            /* New protocol with 16 bytes to describe server characteristics */
            // read character set (1 byte)
            this.serverCharsetIndex = buf.readByte() & 0xff;
            // read status flags (2 bytes)
            this.serverStatus = buf.readInt();
            checkTransactionState(0);

            // read capability flags (upper 2 bytes)
            this.serverCapabilities |= buf.readInt() << 16;

            if ((this.serverCapabilities & CLIENT_PLUGIN_AUTH) != 0) {
                // read length of auth-plugin-data (1 byte)
                this.authPluginDataLength = buf.readByte() & 0xff;
            } else {
                // read filler ([00])
                buf.readByte();
            }
            // next 10 bytes are reserved (all [00])
            buf.setPosition(buf.getPosition() + 10);

            if ((this.serverCapabilities & CLIENT_SECURE_CONNECTION) != 0) {
                String seedPart2;
                StringBuilder newSeed;
                // read string[$len] auth-plugin-data-part-2 ($len=MAX(13, length of auth-plugin-data - 8))
                if (this.authPluginDataLength > 0) {
                    // TODO: disabled the following check for further clarification
                    //         			if (this.authPluginDataLength < 21) {
                    //                      forceClose();
                    //                      throw SQLError.createSQLException(Messages.getString("MysqlIO.103"), 
                    //                          SQLError.SQL_STATE_UNABLE_TO_CONNECT_TO_DATASOURCE, getExceptionInterceptor());
                    //         			}
                    seedPart2 = buf.readString("ASCII", getExceptionInterceptor(), this.authPluginDataLength - 8);
                    newSeed = new StringBuilder(this.authPluginDataLength);
                } else {
                    seedPart2 = buf.readString("ASCII", getExceptionInterceptor());
                    newSeed = new StringBuilder(SEED_LENGTH);
                }
                newSeed.append(this.seed);
                newSeed.append(seedPart2);
                this.seed = newSeed.toString();
            }
        }

        if (((this.serverCapabilities & CLIENT_COMPRESS) != 0) && this.connection.getUseCompression()) {
            this.clientParam |= CLIENT_COMPRESS;
        }

        this.useConnectWithDb = (database != null) && (database.length() > 0) && !this.connection.getCreateDatabaseIfNotExist();

        if (this.useConnectWithDb) {
            this.clientParam |= CLIENT_CONNECT_WITH_DB;
        }

        // Changing SSL defaults for 5.7+ server: useSSL=true, requireSSL=false, verifyServerCertificate=false
        if (versionMeetsMinimum(5, 7, 0) && !this.connection.getUseSSL() && !this.connection.isUseSSLExplicit()) {
            this.connection.setUseSSL(true);
            this.connection.setVerifyServerCertificate(false);
            this.connection.getLog().logWarn(Messages.getString("MysqlIO.SSLWarning"));
        }

        // check SSL availability
        if (((this.serverCapabilities & CLIENT_SSL) == 0) && this.connection.getUseSSL()) {
            if (this.connection.getRequireSSL()) {
                this.connection.close();
                forceClose();
                throw SQLError.createSQLException(Messages.getString("MysqlIO.15"), SQLError.SQL_STATE_UNABLE_TO_CONNECT_TO_DATASOURCE,
                        getExceptionInterceptor());
            }

            this.connection.setUseSSL(false);
        }

        if ((this.serverCapabilities & CLIENT_LONG_FLAG) != 0) {
            // We understand other column flags, as well
            this.clientParam |= CLIENT_LONG_FLAG;
            this.hasLongColumnInfo = true;
        }

        // return FOUND rows
        if (!this.connection.getUseAffectedRows()) {
            this.clientParam |= CLIENT_FOUND_ROWS;
        }

        if (this.connection.getAllowLoadLocalInfile()) {
            this.clientParam |= CLIENT_LOCAL_FILES;
        }

        if (this.isInteractiveClient) {
            this.clientParam |= CLIENT_INTERACTIVE;
        }

        //
        // switch to pluggable authentication if available
        //
        if ((this.serverCapabilities & CLIENT_PLUGIN_AUTH) != 0) {
            proceedHandshakeWithPluggableAuthentication(user, password, database, buf);
            return;
        }

        // Authenticate
        if (this.protocolVersion > 9) {
            this.clientParam |= CLIENT_LONG_PASSWORD; // for long passwords
        } else {
            this.clientParam &= ~CLIENT_LONG_PASSWORD;
        }

        //
        // 4.1 has some differences in the protocol
        //
        if ((versionMeetsMinimum(4, 1, 0) || ((this.protocolVersion > 9) && (this.serverCapabilities & CLIENT_RESERVED) != 0))) {
            if ((versionMeetsMinimum(4, 1, 1) || ((this.protocolVersion > 9) && (this.serverCapabilities & CLIENT_PROTOCOL_41) != 0))) {
                this.clientParam |= CLIENT_PROTOCOL_41;
                this.has41NewNewProt = true;

                // Need this to get server status values
                this.clientParam |= CLIENT_TRANSACTIONS;

                // We always allow multiple result sets
                this.clientParam |= CLIENT_MULTI_RESULTS;

                // We allow the user to configure whether
                // or not they want to support multiple queries
                // (by default, this is disabled).
                if (this.connection.getAllowMultiQueries()) {
                    this.clientParam |= CLIENT_MULTI_STATEMENTS;
                }
            } else {
                this.clientParam |= CLIENT_RESERVED;
                this.has41NewNewProt = false;
            }

            this.use41Extensions = true;
        }

        int passwordLength = 16;
        int userLength = (user != null) ? user.length() : 0;
        int databaseLength = (database != null) ? database.length() : 0;

        int packLength = ((userLength + passwordLength + databaseLength) * 3) + 7 + HEADER_LENGTH + AUTH_411_OVERHEAD;

        Buffer packet = null;

        if (!this.connection.getUseSSL()) {
            if ((this.serverCapabilities & CLIENT_SECURE_CONNECTION) != 0) {
                this.clientParam |= CLIENT_SECURE_CONNECTION;

                if ((versionMeetsMinimum(4, 1, 1) || ((this.protocolVersion > 9) && (this.serverCapabilities & CLIENT_PROTOCOL_41) != 0))) {
                    secureAuth411(null, packLength, user, password, database, true);
                } else {
                    secureAuth(null, packLength, user, password, database, true);
                }
            } else {
                // Passwords can be 16 chars long
                packet = new Buffer(packLength);

                if ((this.clientParam & CLIENT_RESERVED) != 0) {
                    if ((versionMeetsMinimum(4, 1, 1) || ((this.protocolVersion > 9) && (this.serverCapabilities & CLIENT_PROTOCOL_41) != 0))) {
                        packet.writeLong(this.clientParam);
                        packet.writeLong(this.maxThreeBytes);

                        // charset, JDBC will connect as 'latin1', and use 'SET NAMES' to change to the desired charset after the connection is established.
                        packet.writeByte((byte) 8);

                        // Set of bytes reserved for future use.
                        packet.writeBytesNoNull(new byte[23]);
                    } else {
                        packet.writeLong(this.clientParam);
                        packet.writeLong(this.maxThreeBytes);
                    }
                } else {
                    packet.writeInt((int) this.clientParam);
                    packet.writeLongInt(this.maxThreeBytes);
                }

                // User/Password data
                packet.writeString(user, CODE_PAGE_1252, this.connection);

                if (this.protocolVersion > 9) {
                    packet.writeString(Util.newCrypt(password, this.seed, this.connection.getPasswordCharacterEncoding()), CODE_PAGE_1252, this.connection);
                } else {
                    packet.writeString(Util.oldCrypt(password, this.seed), CODE_PAGE_1252, this.connection);
                }

                if (this.useConnectWithDb) {
                    packet.writeString(database, CODE_PAGE_1252, this.connection);
                }

                send(packet, packet.getPosition());
            }
        } else {
            negotiateSSLConnection(user, password, database, packLength);

            if ((this.serverCapabilities & CLIENT_SECURE_CONNECTION) != 0) {
                if (versionMeetsMinimum(4, 1, 1)) {
                    secureAuth411(null, packLength, user, password, database, true);
                } else {
                    secureAuth411(null, packLength, user, password, database, true);
                }
            } else {

                packet = new Buffer(packLength);

                if (this.use41Extensions) {
                    packet.writeLong(this.clientParam);
                    packet.writeLong(this.maxThreeBytes);
                } else {
                    packet.writeInt((int) this.clientParam);
                    packet.writeLongInt(this.maxThreeBytes);
                }

                // User/Password data
                packet.writeString(user);

                if (this.protocolVersion > 9) {
                    packet.writeString(Util.newCrypt(password, this.seed, this.connection.getPasswordCharacterEncoding()));
                } else {
                    packet.writeString(Util.oldCrypt(password, this.seed));
                }

                if (((this.serverCapabilities & CLIENT_CONNECT_WITH_DB) != 0) && (database != null) && (database.length() > 0)) {
                    packet.writeString(database);
                }

                send(packet, packet.getPosition());
            }
        }

        // Check for errors, not for 4.1.1 or newer, as the new auth protocol doesn't work that way (see secureAuth411() for more details...)
        //if (!versionMeetsMinimum(4, 1, 1)) {
        if (!(versionMeetsMinimum(4, 1, 1) || !((this.protocolVersion > 9) && (this.serverCapabilities & CLIENT_PROTOCOL_41) != 0))) {
            checkErrorPacket();
        }

        //
        // Can't enable compression until after handshake
        //
        if (((this.serverCapabilities & CLIENT_COMPRESS) != 0) && this.connection.getUseCompression() && !(this.mysqlInput instanceof CompressedInputStream)) {
            // The following matches with ZLIB's compress()
            this.deflater = new Deflater();
            this.useCompression = true;
            this.mysqlInput = new CompressedInputStream(this.connection, this.mysqlInput);
        }

        if (!this.useConnectWithDb) {
            changeDatabaseTo(database);
        }

        try {
            this.mysqlConnection = this.socketFactory.afterHandshake();
        } catch (IOException ioEx) {
            throw SQLError.createCommunicationsException(this.connection, this.lastPacketSentTimeMs, this.lastPacketReceivedTimeMs, ioEx,
                    getExceptionInterceptor());
        }
    }

    /**
     * Contains instances of authentication plugins which implements {@link AuthenticationPlugin} interface. Key values are mysql
     * protocol plugin names, for example "mysql_native_password" and
     * "mysql_old_password" for built-in plugins.
     */
    private Map<String, AuthenticationPlugin> authenticationPlugins = null;
    /**
     * Contains names of classes or mechanisms ("mysql_native_password"
     * for example) of authentication plugins which must be disabled.
     */
    private List<String> disabledAuthenticationPlugins = null;
    /**
     * Name of class for default authentication plugin in client
     */
    private String clientDefaultAuthenticationPlugin = null;
    /**
     * Protocol name of default authentication plugin in client
     */
    private String clientDefaultAuthenticationPluginName = null;
    /**
     * Protocol name of default authentication plugin in server
     */
    private String serverDefaultAuthenticationPluginName = null;

    /**
     * Fill the {@link MysqlIO#authenticationPlugins} map.
     * First this method fill the map with instances of {@link MysqlOldPasswordPlugin}, {@link MysqlNativePasswordPlugin}, {@link MysqlClearPasswordPlugin} and
     * {@link Sha256PasswordPlugin}.
     * Then it gets instances of plugins listed in "authenticationPlugins" connection property by
     * {@link Util#loadExtensions(Connection, Properties, String, String, ExceptionInterceptor)} call and adds them to the map too.
     * 
     * The key for the map entry is getted by {@link AuthenticationPlugin#getProtocolPluginName()}.
     * Thus it is possible to replace built-in plugin with custom one, to do it custom plugin should return value
     * "mysql_native_password", "mysql_old_password", "mysql_clear_password" or "sha256_password" from it's own getProtocolPluginName() method.
     * 
     * All plugin instances in the map are initialized by {@link Extension#init(Connection, Properties)} call
     * with this.connection and this.connection.getProperties() values.
     * 
     * @throws SQLException
     */
    private void loadAuthenticationPlugins() throws SQLException {

        // default plugin
        this.clientDefaultAuthenticationPlugin = this.connection.getDefaultAuthenticationPlugin();
        if (this.clientDefaultAuthenticationPlugin == null || "".equals(this.clientDefaultAuthenticationPlugin.trim())) {
            throw SQLError.createSQLException(
                    Messages.getString("Connection.BadDefaultAuthenticationPlugin", new Object[] { this.clientDefaultAuthenticationPlugin }),
                    getExceptionInterceptor());
        }

        // disabled plugins
        String disabledPlugins = this.connection.getDisabledAuthenticationPlugins();
        if (disabledPlugins != null && !"".equals(disabledPlugins)) {
            this.disabledAuthenticationPlugins = new ArrayList<String>();
            List<String> pluginsToDisable = StringUtils.split(disabledPlugins, ",", true);
            Iterator<String> iter = pluginsToDisable.iterator();
            while (iter.hasNext()) {
                this.disabledAuthenticationPlugins.add(iter.next());
            }
        }

        this.authenticationPlugins = new HashMap<String, AuthenticationPlugin>();

        // embedded plugins
        AuthenticationPlugin plugin = new MysqlOldPasswordPlugin();
        plugin.init(this.connection, this.connection.getProperties());
        boolean defaultIsFound = addAuthenticationPlugin(plugin);

        plugin = new MysqlNativePasswordPlugin();
        plugin.init(this.connection, this.connection.getProperties());
        if (addAuthenticationPlugin(plugin)) {
            defaultIsFound = true;
        }

        plugin = new MysqlClearPasswordPlugin();
        plugin.init(this.connection, this.connection.getProperties());
        if (addAuthenticationPlugin(plugin)) {
            defaultIsFound = true;
        }

        plugin = new Sha256PasswordPlugin();
        plugin.init(this.connection, this.connection.getProperties());
        if (addAuthenticationPlugin(plugin)) {
            defaultIsFound = true;
        }

        // plugins from authenticationPluginClasses connection parameter
        String authenticationPluginClasses = this.connection.getAuthenticationPlugins();
        if (authenticationPluginClasses != null && !"".equals(authenticationPluginClasses)) {

            List<Extension> plugins = Util.loadExtensions(this.connection, this.connection.getProperties(), authenticationPluginClasses,
                    "Connection.BadAuthenticationPlugin", getExceptionInterceptor());

            for (Extension object : plugins) {
                plugin = (AuthenticationPlugin) object;
                if (addAuthenticationPlugin(plugin)) {
                    defaultIsFound = true;
                }
            }
        }

        // check if default plugin is listed
        if (!defaultIsFound) {
            throw SQLError.createSQLException(
                    Messages.getString("Connection.DefaultAuthenticationPluginIsNotListed", new Object[] { this.clientDefaultAuthenticationPlugin }),
                    getExceptionInterceptor());
        }

    }

    /**
     * Add plugin to {@link MysqlIO#authenticationPlugins} if it is not disabled by
     * "disabledAuthenticationPlugins" property, check is it a default plugin.
     * 
     * @param plugin
     *            Instance of AuthenticationPlugin
     * @return True if plugin is default, false if plugin is not default.
     * @throws SQLException
     *             if plugin is default but disabled.
     */
    private boolean addAuthenticationPlugin(AuthenticationPlugin plugin) throws SQLException {
        boolean isDefault = false;
        String pluginClassName = plugin.getClass().getName();
        String pluginProtocolName = plugin.getProtocolPluginName();
        boolean disabledByClassName = this.disabledAuthenticationPlugins != null && this.disabledAuthenticationPlugins.contains(pluginClassName);
        boolean disabledByMechanism = this.disabledAuthenticationPlugins != null && this.disabledAuthenticationPlugins.contains(pluginProtocolName);

        if (disabledByClassName || disabledByMechanism) {
            // if disabled then check is it default					
            if (this.clientDefaultAuthenticationPlugin.equals(pluginClassName)) {
                throw SQLError.createSQLException(Messages.getString("Connection.BadDisabledAuthenticationPlugin",
                        new Object[] { disabledByClassName ? pluginClassName : pluginProtocolName }), getExceptionInterceptor());
            }
        } else {
            this.authenticationPlugins.put(pluginProtocolName, plugin);
            if (this.clientDefaultAuthenticationPlugin.equals(pluginClassName)) {
                this.clientDefaultAuthenticationPluginName = pluginProtocolName;
                isDefault = true;
            }
        }
        return isDefault;
    }

    /**
     * Get authentication plugin instance from {@link MysqlIO#authenticationPlugins} map by
     * pluginName key. If such plugin is found it's {@link AuthenticationPlugin#isReusable()} method
     * is checked, when it's false this method returns a new instance of plugin
     * and the same instance otherwise.
     * 
     * If plugin is not found method returns null, in such case the subsequent behavior
     * of handshake process depends on type of last packet received from server:
     * if it was Auth Challenge Packet then handshake will proceed with default plugin,
     * if it was Auth Method Switch Request Packet then handshake will be interrupted with exception.
     * 
     * @param pluginName
     *            mysql protocol plugin names, for example "mysql_native_password" and "mysql_old_password" for built-in plugins
     * @return null if plugin is not found or authentication plugin instance initialized with current connection properties
     * @throws SQLException
     */
    private AuthenticationPlugin getAuthenticationPlugin(String pluginName) throws SQLException {

        AuthenticationPlugin plugin = this.authenticationPlugins.get(pluginName);

        if (plugin != null && !plugin.isReusable()) {
            try {
                plugin = plugin.getClass().newInstance();
                plugin.init(this.connection, this.connection.getProperties());
            } catch (Throwable t) {
                SQLException sqlEx = SQLError.createSQLException(
                        Messages.getString("Connection.BadAuthenticationPlugin", new Object[] { plugin.getClass().getName() }), getExceptionInterceptor());
                sqlEx.initCause(t);
                throw sqlEx;
            }
        }

        return plugin;
    }

    /**
     * Check if given plugin requires confidentiality, but connection is without SSL
     * 
     * @param plugin
     * @throws SQLException
     */
    private void checkConfidentiality(AuthenticationPlugin plugin) throws SQLException {
        if (plugin.requiresConfidentiality() && !isSSLEstablished()) {
            throw SQLError.createSQLException(
                    Messages.getString("Connection.AuthenticationPluginRequiresSSL", new Object[] { plugin.getProtocolPluginName() }),
                    getExceptionInterceptor());
        }
    }

    /**
     * Performs an authentication handshake to authorize connection to a
     * given database as a given MySQL user. This can happen upon initial
     * connection to the server, after receiving Auth Challenge Packet, or
     * at any moment during the connection life-time via a Change User
     * request.
     * 
     * This method is aware of pluggable authentication and will use
     * registered authentication plugins as requested by the server.
     * 
     * @param user
     *            the MySQL user account to log into
     * @param password
     *            authentication data for the user account (depends
     *            on authentication method used - can be empty)
     * @param database
     *            database to connect to (can be empty)
     * @param challenge
     *            the Auth Challenge Packet received from server if
     *            this method is used during the initial connection.
     *            Otherwise null.
     * 
     * @throws SQLException
     */
    private void proceedHandshakeWithPluggableAuthentication(String user, String password, String database, Buffer challenge) throws SQLException {
        if (this.authenticationPlugins == null) {
            loadAuthenticationPlugins();
        }

        boolean skipPassword = false;
        int passwordLength = 16;
        int userLength = (user != null) ? user.length() : 0;
        int databaseLength = (database != null) ? database.length() : 0;

        int packLength = ((userLength + passwordLength + databaseLength) * 3) + 7 + HEADER_LENGTH + AUTH_411_OVERHEAD;

        AuthenticationPlugin plugin = null;
        Buffer fromServer = null;
        ArrayList<Buffer> toServer = new ArrayList<Buffer>();
        boolean done = false;
        Buffer last_sent = null;

        boolean old_raw_challenge = false;

        int counter = 100;

        while (0 < counter--) {

            if (!done) {

                if (challenge != null) {
                    // read Auth Challenge Packet

                    this.clientParam |= CLIENT_PLUGIN_AUTH | CLIENT_LONG_PASSWORD | CLIENT_PROTOCOL_41 | CLIENT_TRANSACTIONS // Need this to get server status values
                            | CLIENT_MULTI_RESULTS // We always allow multiple result sets
                            | CLIENT_SECURE_CONNECTION; // protocol with pluggable authentication always support this

                    // We allow the user to configure whether or not they want to support multiple queries (by default, this is disabled).
                    if (this.connection.getAllowMultiQueries()) {
                        this.clientParam |= CLIENT_MULTI_STATEMENTS;
                    }

                    if (((this.serverCapabilities & CLIENT_CAN_HANDLE_EXPIRED_PASSWORD) != 0) && !this.connection.getDisconnectOnExpiredPasswords()) {
                        this.clientParam |= CLIENT_CAN_HANDLE_EXPIRED_PASSWORD;
                    }
                    if (((this.serverCapabilities & CLIENT_CONNECT_ATTRS) != 0) && !NONE.equals(this.connection.getConnectionAttributes())) {
                        this.clientParam |= CLIENT_CONNECT_ATTRS;
                    }
                    if ((this.serverCapabilities & CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA) != 0) {
                        this.clientParam |= CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA;
                    }

                    this.has41NewNewProt = true;
                    this.use41Extensions = true;

                    if (this.connection.getUseSSL()) {
                        negotiateSSLConnection(user, password, database, packLength);
                    }

                    String pluginName = null;
                    // Due to Bug#59453 the auth-plugin-name is missing the terminating NUL-char in versions prior to 5.5.10 and 5.6.2.
                    if ((this.serverCapabilities & CLIENT_PLUGIN_AUTH) != 0) {
                        if (!versionMeetsMinimum(5, 5, 10) || versionMeetsMinimum(5, 6, 0) && !versionMeetsMinimum(5, 6, 2)) {
                            pluginName = challenge.readString("ASCII", getExceptionInterceptor(), this.authPluginDataLength);
                        } else {
                            pluginName = challenge.readString("ASCII", getExceptionInterceptor());
                        }
                    }

                    plugin = getAuthenticationPlugin(pluginName);
                    if (plugin == null) {
                        /*
                         * Use default if there is no plugin for pluginName.
                         */
                        plugin = getAuthenticationPlugin(this.clientDefaultAuthenticationPluginName);
                    } else if (pluginName.equals(Sha256PasswordPlugin.PLUGIN_NAME) && !isSSLEstablished()
                            && this.connection.getServerRSAPublicKeyFile() == null && !this.connection.getAllowPublicKeyRetrieval()) {
                        /*
                         * Fall back to default if plugin is 'sha256_password' but required conditions for this to work aren't met. If default is other than
                         * 'sha256_password' this will result in an immediate authentication switch request, allowing for other plugins to authenticate
                         * successfully. If default is 'sha256_password' then the authentication will fail as expected. In both cases user's password won't be
                         * sent to avoid subjecting it to lesser security levels.
                         */
                        plugin = getAuthenticationPlugin(this.clientDefaultAuthenticationPluginName);
                        skipPassword = !this.clientDefaultAuthenticationPluginName.equals(pluginName);
                    }

                    this.serverDefaultAuthenticationPluginName = plugin.getProtocolPluginName();

                    checkConfidentiality(plugin);
                    fromServer = new Buffer(StringUtils.getBytes(this.seed));
                } else {
                    // no challenge so this is a changeUser call
                    plugin = getAuthenticationPlugin(this.serverDefaultAuthenticationPluginName == null ? this.clientDefaultAuthenticationPluginName
                            : this.serverDefaultAuthenticationPluginName);

                    checkConfidentiality(plugin);

                    // Servers not affected by Bug#70865 expect the Change User Request containing a correct answer
                    // to seed sent by the server during the initial handshake, thus we reuse it here.
                    // Servers affected by Bug#70865 will just ignore it and send the Auth Switch.
                    fromServer = new Buffer(StringUtils.getBytes(this.seed));
                }

            } else {

                // read packet from server and check if it's an ERROR packet
                challenge = checkErrorPacket();
                old_raw_challenge = false;
                this.packetSequence++;
                this.compressedPacketSequence++;

                if (challenge.isOKPacket()) {
                    // if OK packet then finish handshake
                    if (!done) {
                        throw SQLError.createSQLException(
                                Messages.getString("Connection.UnexpectedAuthenticationApproval", new Object[] { plugin.getProtocolPluginName() }),
                                getExceptionInterceptor());
                    }
                    plugin.destroy();
                    break;

                } else if (challenge.isAuthMethodSwitchRequestPacket()) {
                    skipPassword = false;

                    // read Auth Method Switch Request Packet
                    String pluginName = challenge.readString("ASCII", getExceptionInterceptor());

                    // get new plugin
                    if (plugin != null && !plugin.getProtocolPluginName().equals(pluginName)) {
                        plugin.destroy();
                        plugin = getAuthenticationPlugin(pluginName);
                        // if plugin is not found for pluginName throw exception
                        if (plugin == null) {
                            throw SQLError.createSQLException(Messages.getString("Connection.BadAuthenticationPlugin", new Object[] { pluginName }),
                                    getExceptionInterceptor());
                        }
                    }

                    checkConfidentiality(plugin);
                    fromServer = new Buffer(StringUtils.getBytes(challenge.readString("ASCII", getExceptionInterceptor())));

                } else {
                    // read raw packet
                    if (versionMeetsMinimum(5, 5, 16)) {
                        fromServer = new Buffer(challenge.getBytes(challenge.getPosition(), challenge.getBufLength() - challenge.getPosition()));
                    } else {
                        old_raw_challenge = true;
                        fromServer = new Buffer(challenge.getBytes(challenge.getPosition() - 1, challenge.getBufLength() - challenge.getPosition() + 1));
                    }
                }

            }

            // call plugin
            try {
                plugin.setAuthenticationParameters(user, skipPassword ? null : password);
                done = plugin.nextAuthenticationStep(fromServer, toServer);
            } catch (SQLException e) {
                throw SQLError.createSQLException(e.getMessage(), e.getSQLState(), e, getExceptionInterceptor());
            }

            // send response
            if (toServer.size() > 0) {
                if (challenge == null) {
                    String enc = getEncodingForHandshake();

                    // write COM_CHANGE_USER Packet
                    last_sent = new Buffer(packLength + 1);
                    last_sent.writeByte((byte) MysqlDefs.COM_CHANGE_USER);

                    // User/Password data
                    last_sent.writeString(user, enc, this.connection);

                    // 'auth-response-len' is limited to one Byte but, in case of success, COM_CHANGE_USER will be followed by an AuthSwitchRequest anyway
                    if (toServer.get(0).getBufLength() < 256) {
                        // non-mysql servers may use this information to authenticate without requiring another round-trip
                        last_sent.writeByte((byte) toServer.get(0).getBufLength());
                        last_sent.writeBytesNoNull(toServer.get(0).getByteBuffer(), 0, toServer.get(0).getBufLength());
                    } else {
                        last_sent.writeByte((byte) 0);
                    }

                    if (this.useConnectWithDb) {
                        last_sent.writeString(database, enc, this.connection);
                    } else {
                        /* For empty database */
                        last_sent.writeByte((byte) 0);
                    }

                    appendCharsetByteForHandshake(last_sent, enc);
                    // two (little-endian) bytes for charset in this packet
                    last_sent.writeByte((byte) 0);

                    // plugin name
                    if ((this.serverCapabilities & CLIENT_PLUGIN_AUTH) != 0) {
                        last_sent.writeString(plugin.getProtocolPluginName(), enc, this.connection);
                    }

                    // connection attributes
                    if ((this.clientParam & CLIENT_CONNECT_ATTRS) != 0) {
                        sendConnectionAttributes(last_sent, enc, this.connection);
                        last_sent.writeByte((byte) 0);
                    }

                    send(last_sent, last_sent.getPosition());

                } else if (challenge.isAuthMethodSwitchRequestPacket()) {
                    // write Auth Method Switch Response Packet
                    last_sent = new Buffer(toServer.get(0).getBufLength() + HEADER_LENGTH);
                    last_sent.writeBytesNoNull(toServer.get(0).getByteBuffer(), 0, toServer.get(0).getBufLength());
                    send(last_sent, last_sent.getPosition());

                } else if (challenge.isRawPacket() || old_raw_challenge) {
                    // write raw packet(s)
                    for (Buffer buffer : toServer) {
                        last_sent = new Buffer(buffer.getBufLength() + HEADER_LENGTH);
                        last_sent.writeBytesNoNull(buffer.getByteBuffer(), 0, toServer.get(0).getBufLength());
                        send(last_sent, last_sent.getPosition());
                    }

                } else {
                    // write Auth Response Packet
                    String enc = getEncodingForHandshake();

                    last_sent = new Buffer(packLength);
                    last_sent.writeLong(this.clientParam);
                    last_sent.writeLong(this.maxThreeBytes);

                    appendCharsetByteForHandshake(last_sent, enc);

                    last_sent.writeBytesNoNull(new byte[23]);	// Set of bytes reserved for future use.

                    // User/Password data
                    last_sent.writeString(user, enc, this.connection);

                    if ((this.serverCapabilities & CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA) != 0) {
                        // send lenenc-int length of auth-response and string[n] auth-response
                        last_sent.writeLenBytes(toServer.get(0).getBytes(toServer.get(0).getBufLength()));
                    } else {
                        // send 1 byte length of auth-response and string[n] auth-response
                        last_sent.writeByte((byte) toServer.get(0).getBufLength());
                        last_sent.writeBytesNoNull(toServer.get(0).getByteBuffer(), 0, toServer.get(0).getBufLength());
                    }

                    if (this.useConnectWithDb) {
                        last_sent.writeString(database, enc, this.connection);
                    } else {
                        /* For empty database */
                        last_sent.writeByte((byte) 0);
                    }

                    if ((this.serverCapabilities & CLIENT_PLUGIN_AUTH) != 0) {
                        last_sent.writeString(plugin.getProtocolPluginName(), enc, this.connection);
                    }

                    // connection attributes
                    if (((this.clientParam & CLIENT_CONNECT_ATTRS) != 0)) {
                        sendConnectionAttributes(last_sent, enc, this.connection);
                    }

                    send(last_sent, last_sent.getPosition());
                }

            }

        }

        if (counter == 0) {
            throw SQLError.createSQLException(Messages.getString("CommunicationsException.TooManyAuthenticationPluginNegotiations"), getExceptionInterceptor());
        }

        //
        // Can't enable compression until after handshake
        //
        if (((this.serverCapabilities & CLIENT_COMPRESS) != 0) && this.connection.getUseCompression() && !(this.mysqlInput instanceof CompressedInputStream)) {
            // The following matches with ZLIB's compress()
            this.deflater = new Deflater();
            this.useCompression = true;
            this.mysqlInput = new CompressedInputStream(this.connection, this.mysqlInput);
        }

        if (!this.useConnectWithDb) {
            changeDatabaseTo(database);
        }

        try {
            this.mysqlConnection = this.socketFactory.afterHandshake();
        } catch (IOException ioEx) {
            throw SQLError.createCommunicationsException(this.connection, this.lastPacketSentTimeMs, this.lastPacketReceivedTimeMs, ioEx,
                    getExceptionInterceptor());
        }
    }

    private Properties getConnectionAttributesAsProperties(String atts) throws SQLException {

        Properties props = new Properties();

        if (atts != null) {
            String[] pairs = atts.split(",");
            for (String pair : pairs) {
                int keyEnd = pair.indexOf(":");
                if (keyEnd > 0 && (keyEnd + 1) < pair.length()) {
                    props.setProperty(pair.substring(0, keyEnd), pair.substring(keyEnd + 1));
                }
            }
        }

        // Leaving disabled until standard values are defined
        // props.setProperty("_os", NonRegisteringDriver.OS);
        // props.setProperty("_platform", NonRegisteringDriver.PLATFORM);
        props.setProperty("_client_name", NonRegisteringDriver.NAME);
        props.setProperty("_client_version", NonRegisteringDriver.VERSION);
        props.setProperty("_runtime_vendor", NonRegisteringDriver.RUNTIME_VENDOR);
        props.setProperty("_runtime_version", NonRegisteringDriver.RUNTIME_VERSION);
        props.setProperty("_client_license", NonRegisteringDriver.LICENSE);

        return props;
    }

    private void sendConnectionAttributes(Buffer buf, String enc, MySQLConnection conn) throws SQLException {
        String atts = conn.getConnectionAttributes();

        Buffer lb = new Buffer(100);
        try {

            Properties props = getConnectionAttributesAsProperties(atts);

            for (Object key : props.keySet()) {
                lb.writeLenString((String) key, enc, conn.getServerCharset(), null, conn.parserKnowsUnicode(), conn);
                lb.writeLenString(props.getProperty((String) key), enc, conn.getServerCharset(), null, conn.parserKnowsUnicode(), conn);
            }

        } catch (UnsupportedEncodingException e) {

        }

        buf.writeByte((byte) (lb.getPosition() - 4));
        buf.writeBytesNoNull(lb.getByteBuffer(), 4, lb.getBufLength() - 4);

    }

    private void changeDatabaseTo(String database) throws SQLException {
        if (database == null || database.length() == 0) {
            return;
        }

        try {
            sendCommand(MysqlDefs.INIT_DB, database, null, false, null, 0);
        } catch (Exception ex) {
            if (this.connection.getCreateDatabaseIfNotExist()) {
                sendCommand(MysqlDefs.QUERY, "CREATE DATABASE IF NOT EXISTS " + database, null, false, null, 0);
                sendCommand(MysqlDefs.INIT_DB, database, null, false, null, 0);
            } else {
                throw SQLError.createCommunicationsException(this.connection, this.lastPacketSentTimeMs, this.lastPacketReceivedTimeMs, ex,
                        getExceptionInterceptor());
            }
        }
    }

    /**
     * Retrieve one row from the MySQL server. Note: this method is not
     * thread-safe, but it is only called from methods that are guarded by
     * synchronizing on this object.
     * 
     * @param fields
     * @param columnCount
     * @param isBinaryEncoded
     * @param resultSetConcurrency
     * @param b
     * 
     * @throws SQLException
     */
    final ResultSetRow nextRow(Field[] fields, int columnCount, boolean isBinaryEncoded, int resultSetConcurrency, boolean useBufferRowIfPossible,
            boolean useBufferRowExplicit, boolean canReuseRowPacketForBufferRow, Buffer existingRowPacket) throws SQLException {

        if (this.useDirectRowUnpack && existingRowPacket == null && !isBinaryEncoded && !useBufferRowIfPossible && !useBufferRowExplicit) {
            return nextRowFast(fields, columnCount, isBinaryEncoded, resultSetConcurrency, useBufferRowIfPossible, useBufferRowExplicit,
                    canReuseRowPacketForBufferRow);
        }

        Buffer rowPacket = null;

        if (existingRowPacket == null) {
            rowPacket = checkErrorPacket();

            if (!useBufferRowExplicit && useBufferRowIfPossible) {
                if (rowPacket.getBufLength() > this.useBufferRowSizeThreshold) {
                    useBufferRowExplicit = true;
                }
            }
        } else {
            // We attempted to do nextRowFast(), but the packet was a multipacket, so we couldn't unpack it directly
            rowPacket = existingRowPacket;
            checkErrorPacket(existingRowPacket);
        }

        if (!isBinaryEncoded) {
            //
            // Didn't read an error, so re-position to beginning of packet in order to read result set data
            //
            rowPacket.setPosition(rowPacket.getPosition() - 1);

            if (!rowPacket.isLastDataPacket()) {
                if (resultSetConcurrency == ResultSet.CONCUR_UPDATABLE || (!useBufferRowIfPossible && !useBufferRowExplicit)) {

                    byte[][] rowData = new byte[columnCount][];

                    for (int i = 0; i < columnCount; i++) {
                        rowData[i] = rowPacket.readLenByteArray(0);
                    }

                    return new ByteArrayRow(rowData, getExceptionInterceptor());
                }

                if (!canReuseRowPacketForBufferRow) {
                    this.reusablePacket = new Buffer(rowPacket.getBufLength());
                }

                return new BufferRow(rowPacket, fields, false, getExceptionInterceptor());

            }

            readServerStatusForResultSets(rowPacket);

            return null;
        }

        //
        // Handle binary-encoded data for server-side PreparedStatements...
        //
        if (!rowPacket.isLastDataPacket()) {
            if (resultSetConcurrency == ResultSet.CONCUR_UPDATABLE || (!useBufferRowIfPossible && !useBufferRowExplicit)) {
                return unpackBinaryResultSetRow(fields, rowPacket, resultSetConcurrency);
            }

            if (!canReuseRowPacketForBufferRow) {
                this.reusablePacket = new Buffer(rowPacket.getBufLength());
            }

            return new BufferRow(rowPacket, fields, true, getExceptionInterceptor());
        }

        rowPacket.setPosition(rowPacket.getPosition() - 1);
        readServerStatusForResultSets(rowPacket);

        return null;
    }

    final ResultSetRow nextRowFast(Field[] fields, int columnCount, boolean isBinaryEncoded, int resultSetConcurrency, boolean useBufferRowIfPossible,
            boolean useBufferRowExplicit, boolean canReuseRowPacket) throws SQLException {
        try {
            int lengthRead = readFully(this.mysqlInput, this.packetHeaderBuf, 0, 4);

            if (lengthRead < 4) {
                forceClose();
                throw new RuntimeException(Messages.getString("MysqlIO.43"));
            }

            int packetLength = (this.packetHeaderBuf[0] & 0xff) + ((this.packetHeaderBuf[1] & 0xff) << 8) + ((this.packetHeaderBuf[2] & 0xff) << 16);

            // Have we stumbled upon a multi-packet?
            if (packetLength == this.maxThreeBytes) {
                reuseAndReadPacket(this.reusablePacket, packetLength);

                // Go back to "old" way which uses packets
                return nextRow(fields, columnCount, isBinaryEncoded, resultSetConcurrency, useBufferRowIfPossible, useBufferRowExplicit, canReuseRowPacket,
                        this.reusablePacket);
            }

            // Does this go over the threshold where we should use a BufferRow?

            if (packetLength > this.useBufferRowSizeThreshold) {
                reuseAndReadPacket(this.reusablePacket, packetLength);

                // Go back to "old" way which uses packets
                return nextRow(fields, columnCount, isBinaryEncoded, resultSetConcurrency, true, true, false, this.reusablePacket);
            }

            int remaining = packetLength;

            boolean firstTime = true;

            byte[][] rowData = null;

            for (int i = 0; i < columnCount; i++) {

                int sw = this.mysqlInput.read() & 0xff;
                remaining--;

                if (firstTime) {
                    if (sw == 255) {
                        // error packet - we assemble it whole for "fidelity" in case we ever need an entire packet in checkErrorPacket() but we could've gotten
                        // away with just writing the error code and message in it (for now).
                        Buffer errorPacket = new Buffer(packetLength + HEADER_LENGTH);
                        errorPacket.setPosition(0);
                        errorPacket.writeByte(this.packetHeaderBuf[0]);
                        errorPacket.writeByte(this.packetHeaderBuf[1]);
                        errorPacket.writeByte(this.packetHeaderBuf[2]);
                        errorPacket.writeByte((byte) 1);
                        errorPacket.writeByte((byte) sw);
                        readFully(this.mysqlInput, errorPacket.getByteBuffer(), 5, packetLength - 1);
                        errorPacket.setPosition(4);
                        checkErrorPacket(errorPacket);
                    }

                    if (sw == 254 && packetLength < 9) {
                        if (this.use41Extensions) {
                            this.warningCount = (this.mysqlInput.read() & 0xff) | ((this.mysqlInput.read() & 0xff) << 8);
                            remaining -= 2;

                            if (this.warningCount > 0) {
                                this.hadWarnings = true; // this is a 'latch', it's reset by sendCommand()
                            }

                            this.oldServerStatus = this.serverStatus;

                            this.serverStatus = (this.mysqlInput.read() & 0xff) | ((this.mysqlInput.read() & 0xff) << 8);
                            checkTransactionState(this.oldServerStatus);

                            remaining -= 2;

                            if (remaining > 0) {
                                skipFully(this.mysqlInput, remaining);
                            }
                        }

                        return null; // last data packet
                    }

                    rowData = new byte[columnCount][];

                    firstTime = false;
                }

                int len = 0;

                switch (sw) {
                    case 251:
                        len = NULL_LENGTH;
                        break;

                    case 252:
                        len = (this.mysqlInput.read() & 0xff) | ((this.mysqlInput.read() & 0xff) << 8);
                        remaining -= 2;
                        break;

                    case 253:
                        len = (this.mysqlInput.read() & 0xff) | ((this.mysqlInput.read() & 0xff) << 8) | ((this.mysqlInput.read() & 0xff) << 16);

                        remaining -= 3;
                        break;

                    case 254:
                        len = (int) ((this.mysqlInput.read() & 0xff) | ((long) (this.mysqlInput.read() & 0xff) << 8)
                                | ((long) (this.mysqlInput.read() & 0xff) << 16) | ((long) (this.mysqlInput.read() & 0xff) << 24)
                                | ((long) (this.mysqlInput.read() & 0xff) << 32) | ((long) (this.mysqlInput.read() & 0xff) << 40)
                                | ((long) (this.mysqlInput.read() & 0xff) << 48) | ((long) (this.mysqlInput.read() & 0xff) << 56));
                        remaining -= 8;
                        break;

                    default:
                        len = sw;
                }

                if (len == NULL_LENGTH) {
                    rowData[i] = null;
                } else if (len == 0) {
                    rowData[i] = Constants.EMPTY_BYTE_ARRAY;
                } else {
                    rowData[i] = new byte[len];

                    int bytesRead = readFully(this.mysqlInput, rowData[i], 0, len);

                    if (bytesRead != len) {
                        throw SQLError.createCommunicationsException(this.connection, this.lastPacketSentTimeMs, this.lastPacketReceivedTimeMs,
                                new IOException(Messages.getString("MysqlIO.43")), getExceptionInterceptor());
                    }

                    remaining -= bytesRead;
                }
            }

            if (remaining > 0) {
                skipFully(this.mysqlInput, remaining);
            }

            return new ByteArrayRow(rowData, getExceptionInterceptor());
        } catch (IOException ioEx) {
            throw SQLError.createCommunicationsException(this.connection, this.lastPacketSentTimeMs, this.lastPacketReceivedTimeMs, ioEx,
                    getExceptionInterceptor());
        }
    }

    /**
     * Log-off of the MySQL server and close the socket.
     * 
     * @throws SQLException
     */
    final void quit() throws SQLException {
        try {
            // we're not going to read the response, fixes BUG#56979 Improper connection closing logic leads to TIME_WAIT sockets on server

            try {
                if (!this.mysqlConnection.isClosed()) {
                    try {
                        this.mysqlConnection.shutdownInput();
                    } catch (UnsupportedOperationException ex) {
                        // ignore, some sockets do not support this method
                    }
                }
            } catch (IOException ioEx) {
                this.connection.getLog().logWarn("Caught while disconnecting...", ioEx);
            }

            Buffer packet = new Buffer(6);
            this.packetSequence = -1;
            this.compressedPacketSequence = -1;
            packet.writeByte((byte) MysqlDefs.QUIT);
            send(packet, packet.getPosition());
        } finally {
            forceClose();
        }
    }

    /**
     * Returns the packet used for sending data (used by PreparedStatement)
     * Guarded by external synchronization on a mutex.
     * 
     * @return A packet to send data with
     */
    Buffer getSharedSendPacket() {
        if (this.sharedSendPacket == null) {
            this.sharedSendPacket = new Buffer(INITIAL_PACKET_SIZE);
        }

        return this.sharedSendPacket;
    }

    void closeStreamer(RowData streamer) throws SQLException {
        if (this.streamingData == null) {
            throw SQLError.createSQLException(Messages.getString("MysqlIO.17") + streamer + Messages.getString("MysqlIO.18"), getExceptionInterceptor());
        }

        if (streamer != this.streamingData) {
            throw SQLError.createSQLException(Messages.getString("MysqlIO.19") + streamer + Messages.getString("MysqlIO.20") + Messages.getString("MysqlIO.21")
                    + Messages.getString("MysqlIO.22"), getExceptionInterceptor());
        }

        this.streamingData = null;
    }

    boolean tackOnMoreStreamingResults(ResultSetImpl addingTo) throws SQLException {
        if ((this.serverStatus & SERVER_MORE_RESULTS_EXISTS) != 0) {

            boolean moreRowSetsExist = true;
            ResultSetImpl currentResultSet = addingTo;
            boolean firstTime = true;

            while (moreRowSetsExist) {
                if (!firstTime && currentResultSet.reallyResult()) {
                    break;
                }

                firstTime = false;

                Buffer fieldPacket = checkErrorPacket();
                fieldPacket.setPosition(0);

                java.sql.Statement owningStatement = addingTo.getStatement();

                int maxRows = owningStatement.getMaxRows();

                // fixme for catalog, isBinary

                ResultSetImpl newResultSet = readResultsForQueryOrUpdate((StatementImpl) owningStatement, maxRows, owningStatement.getResultSetType(),
                        owningStatement.getResultSetConcurrency(), true, owningStatement.getConnection().getCatalog(), fieldPacket, addingTo.isBinaryEncoded,
                        -1L, null);

                currentResultSet.setNextResultSet(newResultSet);

                currentResultSet = newResultSet;

                moreRowSetsExist = (this.serverStatus & MysqlIO.SERVER_MORE_RESULTS_EXISTS) != 0;

                if (!currentResultSet.reallyResult() && !moreRowSetsExist) {
                    // special case, we can stop "streaming"
                    return false;
                }
            }

            return true;
        }

        return false;
    }

    ResultSetImpl readAllResults(StatementImpl callingStatement, int maxRows, int resultSetType, int resultSetConcurrency, boolean streamResults,
            String catalog, Buffer resultPacket, boolean isBinaryEncoded, long preSentColumnCount, Field[] metadataFromCache) throws SQLException {
        resultPacket.setPosition(resultPacket.getPosition() - 1);

        ResultSetImpl topLevelResultSet = readResultsForQueryOrUpdate(callingStatement, maxRows, resultSetType, resultSetConcurrency, streamResults, catalog,
                resultPacket, isBinaryEncoded, preSentColumnCount, metadataFromCache);

        ResultSetImpl currentResultSet = topLevelResultSet;

        boolean checkForMoreResults = ((this.clientParam & CLIENT_MULTI_RESULTS) != 0);

        boolean serverHasMoreResults = (this.serverStatus & SERVER_MORE_RESULTS_EXISTS) != 0;

        //
        // TODO: We need to support streaming of multiple result sets
        //
        if (serverHasMoreResults && streamResults) {
            //clearInputStream();
            //
            //throw SQLError.createSQLException(Messages.getString("MysqlIO.23"), 
            //SQLError.SQL_STATE_DRIVER_NOT_CAPABLE);
            if (topLevelResultSet.getUpdateCount() != -1) {
                tackOnMoreStreamingResults(topLevelResultSet);
            }

            reclaimLargeReusablePacket();

            return topLevelResultSet;
        }

        boolean moreRowSetsExist = checkForMoreResults & serverHasMoreResults;

        while (moreRowSetsExist) {
            Buffer fieldPacket = checkErrorPacket();
            fieldPacket.setPosition(0);

            ResultSetImpl newResultSet = readResultsForQueryOrUpdate(callingStatement, maxRows, resultSetType, resultSetConcurrency, streamResults, catalog,
                    fieldPacket, isBinaryEncoded, preSentColumnCount, metadataFromCache);

            currentResultSet.setNextResultSet(newResultSet);

            currentResultSet = newResultSet;

            moreRowSetsExist = (this.serverStatus & SERVER_MORE_RESULTS_EXISTS) != 0;
        }

        if (!streamResults) {
            clearInputStream();
        }

        reclaimLargeReusablePacket();

        return topLevelResultSet;
    }

    /**
     * Sets the buffer size to max-buf
     */
    void resetMaxBuf() {
        this.maxAllowedPacket = this.connection.getMaxAllowedPacket();
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

    final Buffer sendCommand(int command, String extraData, Buffer queryPacket, boolean skipCheck, String extraDataCharEncoding, int timeoutMillis)
            throws SQLException {
        this.commandCount++;

        //
        // We cache these locally, per-command, as the checks for them are in very 'hot' sections of the I/O code and we save 10-15% in overall performance by
        // doing this...
        //
        this.enablePacketDebug = this.connection.getEnablePacketDebug();
        this.readPacketSequence = 0;

        int oldTimeout = 0;

        if (timeoutMillis != 0) {
            try {
                oldTimeout = this.mysqlConnection.getSoTimeout();
                this.mysqlConnection.setSoTimeout(timeoutMillis);
            } catch (SocketException e) {
                throw SQLError.createCommunicationsException(this.connection, this.lastPacketSentTimeMs, this.lastPacketReceivedTimeMs, e,
                        getExceptionInterceptor());
            }
        }

        try {

            checkForOutstandingStreamingData();

            // Clear serverStatus...this value is guarded by an external mutex, as you can only ever be processing one command at a time
            this.oldServerStatus = this.serverStatus;
            this.serverStatus = 0;
            this.hadWarnings = false;
            this.warningCount = 0;

            this.queryNoIndexUsed = false;
            this.queryBadIndexUsed = false;
            this.serverQueryWasSlow = false;

            //
            // Compressed input stream needs cleared at beginning of each command execution...
            //
            if (this.useCompression) {
                int bytesLeft = this.mysqlInput.available();

                if (bytesLeft > 0) {
                    this.mysqlInput.skip(bytesLeft);
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
                    int packLength = HEADER_LENGTH + COMP_HEADER_LENGTH + 1 + ((extraData != null) ? extraData.length() : 0) + 2;

                    if (this.sendPacket == null) {
                        this.sendPacket = new Buffer(packLength);
                    }

                    this.packetSequence = -1;
                    this.compressedPacketSequence = -1;
                    this.readPacketSequence = 0;
                    this.checkPacketSequence = true;
                    this.sendPacket.clear();

                    this.sendPacket.writeByte((byte) command);

                    if ((command == MysqlDefs.INIT_DB) || (command == MysqlDefs.CREATE_DB) || (command == MysqlDefs.DROP_DB) || (command == MysqlDefs.QUERY)
                            || (command == MysqlDefs.COM_PREPARE)) {
                        if (extraDataCharEncoding == null) {
                            this.sendPacket.writeStringNoNull(extraData);
                        } else {
                            this.sendPacket.writeStringNoNull(extraData, extraDataCharEncoding, this.connection.getServerCharset(),
                                    this.connection.parserKnowsUnicode(), this.connection);
                        }
                    } else if (command == MysqlDefs.PROCESS_KILL) {
                        long id = Long.parseLong(extraData);
                        this.sendPacket.writeLong(id);
                    }

                    send(this.sendPacket, this.sendPacket.getPosition());
                } else {
                    this.packetSequence = -1;
                    this.compressedPacketSequence = -1;
                    send(queryPacket, queryPacket.getPosition()); // packet passed by PreparedStatement
                }
            } catch (SQLException sqlEx) {
                // don't wrap SQLExceptions
                throw sqlEx;
            } catch (Exception ex) {
                throw SQLError.createCommunicationsException(this.connection, this.lastPacketSentTimeMs, this.lastPacketReceivedTimeMs, ex,
                        getExceptionInterceptor());
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
            throw SQLError.createCommunicationsException(this.connection, this.lastPacketSentTimeMs, this.lastPacketReceivedTimeMs, ioEx,
                    getExceptionInterceptor());
        } finally {
            if (timeoutMillis != 0) {
                try {
                    this.mysqlConnection.setSoTimeout(oldTimeout);
                } catch (SocketException e) {
                    throw SQLError.createCommunicationsException(this.connection, this.lastPacketSentTimeMs, this.lastPacketReceivedTimeMs, e,
                            getExceptionInterceptor());
                }
            }
        }
    }

    private int statementExecutionDepth = 0;
    private boolean useAutoSlowLog;

    protected boolean shouldIntercept() {
        return this.statementInterceptors != null;
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
     * @throws Exception
     */
    final ResultSetInternalMethods sqlQueryDirect(StatementImpl callingStatement, String query, String characterEncoding, Buffer queryPacket, int maxRows,
            int resultSetType, int resultSetConcurrency, boolean streamResults, String catalog, Field[] cachedMetadata) throws Exception {
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
                int packLength = HEADER_LENGTH + 1 + (query.length() * 3) + 2;

                byte[] commentAsBytes = null;

                if (statementComment != null) {
                    commentAsBytes = StringUtils.getBytes(statementComment, null, characterEncoding, this.connection.getServerCharset(),
                            this.connection.parserKnowsUnicode(), getExceptionInterceptor());

                    packLength += commentAsBytes.length;
                    packLength += 6; // for /*[space] [space]*/
                }

                if (this.sendPacket == null) {
                    this.sendPacket = new Buffer(packLength);
                } else {
                    this.sendPacket.clear();
                }

                this.sendPacket.writeByte((byte) MysqlDefs.QUERY);

                if (commentAsBytes != null) {
                    this.sendPacket.writeBytesNoNull(Constants.SLASH_STAR_SPACE_AS_BYTES);
                    this.sendPacket.writeBytesNoNull(commentAsBytes);
                    this.sendPacket.writeBytesNoNull(Constants.SPACE_STAR_SLASH_SPACE_AS_BYTES);
                }

                if (characterEncoding != null) {
                    if (this.platformDbCharsetMatches) {
                        this.sendPacket.writeStringNoNull(query, characterEncoding, this.connection.getServerCharset(), this.connection.parserKnowsUnicode(),
                                this.connection);
                    } else {
                        if (StringUtils.startsWithIgnoreCaseAndWs(query, "LOAD DATA")) {
                            this.sendPacket.writeBytesNoNull(StringUtils.getBytes(query));
                        } else {
                            this.sendPacket.writeStringNoNull(query, characterEncoding, this.connection.getServerCharset(),
                                    this.connection.parserKnowsUnicode(), this.connection);
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
                    testcaseQuery = StringUtils.toString(queryBuf, 5, (oldPacketPosition - 5));
                }

                StringBuilder debugBuf = new StringBuilder(testcaseQuery.length() + 32);
                this.connection.generateConnectionCommentBlock(debugBuf);
                debugBuf.append(testcaseQuery);
                debugBuf.append(';');
                this.connection.dumpTestcaseQuery(debugBuf.toString());
            }

            // Send query command and sql query string
            Buffer resultPacket = sendCommand(MysqlDefs.QUERY, null, queryPacket, false, null, 0);

            long fetchBeginTime = 0;
            long fetchEndTime = 0;

            String profileQueryToLog = null;

            boolean queryWasSlow = false;

            if (this.profileSql || this.logSlowQueries) {
                queryEndTime = getCurrentTimeNanosOrMillis();

                boolean shouldExtractQuery = false;

                if (this.profileSql) {
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
                        extractPosition = this.connection.getMaxQuerySizeToLog() + 5;
                        truncated = true;
                    }

                    profileQueryToLog = StringUtils.toString(queryBuf, 5, (extractPosition - 5));

                    if (truncated) {
                        profileQueryToLog += Messages.getString("MysqlIO.25");
                    }
                }

                fetchBeginTime = queryEndTime;
            }

            ResultSetInternalMethods rs = readAllResults(callingStatement, maxRows, resultSetType, resultSetConcurrency, streamResults, catalog, resultPacket,
                    false, -1L, cachedMetadata);

            if (queryWasSlow && !this.serverQueryWasSlow /* don't log slow queries twice */) {
                StringBuilder mesgBuf = new StringBuilder(48 + profileQueryToLog.length());

                mesgBuf.append(Messages.getString(
                        "MysqlIO.SlowQuery",
                        new Object[] { String.valueOf(this.useAutoSlowLog ? " 95% of all queries " : this.slowQueryThreshold), this.queryTimingUnits,
                                Long.valueOf(queryEndTime - queryStartTime) }));
                mesgBuf.append(profileQueryToLog);

                ProfilerEventHandler eventSink = ProfilerEventHandlerFactory.getInstance(this.connection);

                eventSink.consumeEvent(new ProfilerEvent(ProfilerEvent.TYPE_SLOW_QUERY, "", catalog, this.connection.getId(),
                        (callingStatement != null) ? callingStatement.getId() : 999, ((ResultSetImpl) rs).resultId, System.currentTimeMillis(),
                        (int) (queryEndTime - queryStartTime), this.queryTimingUnits, null, LogUtils.findCallingClassAndMethod(new Throwable()), mesgBuf
                                .toString()));

                if (this.connection.getExplainSlowQueries()) {
                    if (oldPacketPosition < MAX_QUERY_SIZE_TO_EXPLAIN) {
                        explainSlowQuery(queryPacket.getBytes(5, (oldPacketPosition - 5)), profileQueryToLog);
                    } else {
                        this.connection.getLog().logWarn(Messages.getString("MysqlIO.28") + MAX_QUERY_SIZE_TO_EXPLAIN + Messages.getString("MysqlIO.29"));
                    }
                }
            }

            if (this.logSlowQueries) {

                ProfilerEventHandler eventSink = ProfilerEventHandlerFactory.getInstance(this.connection);

                if (this.queryBadIndexUsed && this.profileSql) {
                    eventSink.consumeEvent(new ProfilerEvent(ProfilerEvent.TYPE_SLOW_QUERY, "", catalog, this.connection.getId(),
                            (callingStatement != null) ? callingStatement.getId() : 999, ((ResultSetImpl) rs).resultId, System.currentTimeMillis(),
                            (queryEndTime - queryStartTime), this.queryTimingUnits, null, LogUtils.findCallingClassAndMethod(new Throwable()), Messages
                                    .getString("MysqlIO.33") + profileQueryToLog));
                }

                if (this.queryNoIndexUsed && this.profileSql) {
                    eventSink.consumeEvent(new ProfilerEvent(ProfilerEvent.TYPE_SLOW_QUERY, "", catalog, this.connection.getId(),
                            (callingStatement != null) ? callingStatement.getId() : 999, ((ResultSetImpl) rs).resultId, System.currentTimeMillis(),
                            (queryEndTime - queryStartTime), this.queryTimingUnits, null, LogUtils.findCallingClassAndMethod(new Throwable()), Messages
                                    .getString("MysqlIO.35") + profileQueryToLog));
                }

                if (this.serverQueryWasSlow && this.profileSql) {
                    eventSink.consumeEvent(new ProfilerEvent(ProfilerEvent.TYPE_SLOW_QUERY, "", catalog, this.connection.getId(),
                            (callingStatement != null) ? callingStatement.getId() : 999, ((ResultSetImpl) rs).resultId, System.currentTimeMillis(),
                            (queryEndTime - queryStartTime), this.queryTimingUnits, null, LogUtils.findCallingClassAndMethod(new Throwable()), Messages
                                    .getString("MysqlIO.ServerSlowQuery") + profileQueryToLog));
                }
            }

            if (this.profileSql) {
                fetchEndTime = getCurrentTimeNanosOrMillis();

                ProfilerEventHandler eventSink = ProfilerEventHandlerFactory.getInstance(this.connection);

                eventSink.consumeEvent(new ProfilerEvent(ProfilerEvent.TYPE_QUERY, "", catalog, this.connection.getId(),
                        (callingStatement != null) ? callingStatement.getId() : 999, ((ResultSetImpl) rs).resultId, System.currentTimeMillis(),
                        (queryEndTime - queryStartTime), this.queryTimingUnits, null, LogUtils.findCallingClassAndMethod(new Throwable()), profileQueryToLog));

                eventSink.consumeEvent(new ProfilerEvent(ProfilerEvent.TYPE_FETCH, "", catalog, this.connection.getId(),
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
        } catch (SQLException sqlEx) {
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

    ResultSetInternalMethods invokeStatementInterceptorsPre(String sql, Statement interceptedStatement, boolean forceExecute) throws SQLException {
        ResultSetInternalMethods previousResultSet = null;

        for (int i = 0, s = this.statementInterceptors.size(); i < s; i++) {
            StatementInterceptorV2 interceptor = this.statementInterceptors.get(i);

            boolean executeTopLevelOnly = interceptor.executeTopLevelOnly();
            boolean shouldExecute = (executeTopLevelOnly && (this.statementExecutionDepth == 1 || forceExecute)) || (!executeTopLevelOnly);

            if (shouldExecute) {
                String sqlToInterceptor = sql;

                //if (interceptedStatement instanceof PreparedStatement) {
                //	sqlToInterceptor = ((PreparedStatement) interceptedStatement)
                //			.asSql();
                //}

                ResultSetInternalMethods interceptedResultSet = interceptor.preProcess(sqlToInterceptor, interceptedStatement, this.connection);

                if (interceptedResultSet != null) {
                    previousResultSet = interceptedResultSet;
                }
            }
        }

        return previousResultSet;
    }

    ResultSetInternalMethods invokeStatementInterceptorsPost(String sql, Statement interceptedStatement, ResultSetInternalMethods originalResultSet,
            boolean forceExecute, SQLException statementException) throws SQLException {

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

    protected long getCurrentTimeNanosOrMillis() {
        if (this.useNanosForElapsedTime) {
            return TimeUtil.getCurrentTimeNanosOrMillis();
        }

        return System.currentTimeMillis();
    }

    /**
     * Returns the host this IO is connected to
     */
    String getHost() {
        return this.host;
    }

    /**
     * Is the version of the MySQL server we are connected to the given
     * version?
     * 
     * @param major
     *            the major version
     * @param minor
     *            the minor version
     * @param subminor
     *            the subminor version
     * 
     * @return true if the version of the MySQL server we are connected is the
     *         given version
     */
    boolean isVersion(int major, int minor, int subminor) {
        return ((major == getServerMajorVersion()) && (minor == getServerMinorVersion()) && (subminor == getServerSubMinorVersion()));
    }

    /**
     * Does the version of the MySQL server we are connected to meet the given
     * minimums?
     * 
     * @param major
     * @param minor
     * @param subminor
     */
    boolean versionMeetsMinimum(int major, int minor, int subminor) {
        if (getServerMajorVersion() >= major) {
            if (getServerMajorVersion() == major) {
                if (getServerMinorVersion() >= minor) {
                    if (getServerMinorVersion() == minor) {
                        return (getServerSubMinorVersion() >= subminor);
                    }

                    // newer than major.minor
                    return true;
                }

                // older than major.minor
                return false;
            }

            // newer than major
            return true;
        }

        return false;
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

    private final int readFully(InputStream in, byte[] b, int off, int len) throws IOException {
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

    private final long skipFully(InputStream in, long len) throws IOException {
        if (len < 0) {
            throw new IOException("Negative skip length not allowed");
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
     * Reads one result set off of the wire, if the result is actually an
     * update count, creates an update-count only result set.
     * 
     * @param callingStatement
     * @param maxRows
     *            the maximum rows to return in the result set.
     * @param resultSetType
     *            scrollability
     * @param resultSetConcurrency
     *            updatability
     * @param streamResults
     *            should the driver leave the results on the wire,
     *            and read them only when needed?
     * @param catalog
     *            the catalog in use
     * @param resultPacket
     *            the first packet of information in the result set
     * @param isBinaryEncoded
     *            is this result set from a prepared statement?
     * @param preSentColumnCount
     *            do we already know the number of columns?
     * @param unpackFieldInfo
     *            should we unpack the field information?
     * 
     * @return a result set that either represents the rows, or an update count
     * 
     * @throws SQLException
     *             if an error occurs while reading the rows
     */
    protected final ResultSetImpl readResultsForQueryOrUpdate(StatementImpl callingStatement, int maxRows, int resultSetType, int resultSetConcurrency,
            boolean streamResults, String catalog, Buffer resultPacket, boolean isBinaryEncoded, long preSentColumnCount, Field[] metadataFromCache)
            throws SQLException {
        long columnCount = resultPacket.readFieldLength();

        if (columnCount == 0) {
            return buildResultSetWithUpdates(callingStatement, resultPacket);
        } else if (columnCount == Buffer.NULL_LENGTH) {
            String charEncoding = null;

            if (this.connection.getUseUnicode()) {
                charEncoding = this.connection.getEncoding();
            }

            String fileName = null;

            if (this.platformDbCharsetMatches) {
                fileName = ((charEncoding != null) ? resultPacket.readString(charEncoding, getExceptionInterceptor()) : resultPacket.readString());
            } else {
                fileName = resultPacket.readString();
            }

            return sendFileToServer(callingStatement, fileName);
        } else {
            com.mysql.jdbc.ResultSetImpl results = getResultSet(callingStatement, columnCount, maxRows, resultSetType, resultSetConcurrency, streamResults,
                    catalog, isBinaryEncoded, metadataFromCache);

            return results;
        }
    }

    private int alignPacketSize(int a, int l) {
        return ((((a) + (l)) - 1) & ~((l) - 1));
    }

    private com.mysql.jdbc.ResultSetImpl buildResultSetWithRows(StatementImpl callingStatement, String catalog, com.mysql.jdbc.Field[] fields, RowData rows,
            int resultSetType, int resultSetConcurrency, boolean isBinaryEncoded) throws SQLException {
        ResultSetImpl rs = null;

        switch (resultSetConcurrency) {
            case java.sql.ResultSet.CONCUR_READ_ONLY:
                rs = com.mysql.jdbc.ResultSetImpl.getInstance(catalog, fields, rows, this.connection, callingStatement, false);

                if (isBinaryEncoded) {
                    rs.setBinaryEncoded();
                }

                break;

            case java.sql.ResultSet.CONCUR_UPDATABLE:
                rs = com.mysql.jdbc.ResultSetImpl.getInstance(catalog, fields, rows, this.connection, callingStatement, true);

                break;

            default:
                return com.mysql.jdbc.ResultSetImpl.getInstance(catalog, fields, rows, this.connection, callingStatement, false);
        }

        rs.setResultSetType(resultSetType);
        rs.setResultSetConcurrency(resultSetConcurrency);

        return rs;
    }

    private com.mysql.jdbc.ResultSetImpl buildResultSetWithUpdates(StatementImpl callingStatement, Buffer resultPacket) throws SQLException {
        long updateCount = -1;
        long updateID = -1;
        String info = null;

        try {
            if (this.useNewUpdateCounts) {
                updateCount = resultPacket.newReadLength();
                updateID = resultPacket.newReadLength();
            } else {
                updateCount = resultPacket.readLength();
                updateID = resultPacket.readLength();
            }

            if (this.use41Extensions) {
                // oldStatus set in sendCommand()
                this.serverStatus = resultPacket.readInt();

                checkTransactionState(this.oldServerStatus);

                this.warningCount = resultPacket.readInt();

                if (this.warningCount > 0) {
                    this.hadWarnings = true; // this is a 'latch', it's reset by sendCommand()
                }

                resultPacket.readByte(); // advance pointer

                setServerSlowQueryFlags();
            }

            if (this.connection.isReadInfoMsgEnabled()) {
                info = resultPacket.readString(this.connection.getErrorMessageEncoding(), getExceptionInterceptor());
            }
        } catch (Exception ex) {
            SQLException sqlEx = SQLError.createSQLException(SQLError.get(SQLError.SQL_STATE_GENERAL_ERROR), SQLError.SQL_STATE_GENERAL_ERROR, -1,
                    getExceptionInterceptor());
            sqlEx.initCause(ex);

            throw sqlEx;
        }

        ResultSetInternalMethods updateRs = com.mysql.jdbc.ResultSetImpl.getInstance(updateCount, updateID, this.connection, callingStatement);

        if (info != null) {
            ((com.mysql.jdbc.ResultSetImpl) updateRs).setServerInfo(info);
        }

        return (com.mysql.jdbc.ResultSetImpl) updateRs;
    }

    private void setServerSlowQueryFlags() {
        this.queryBadIndexUsed = (this.serverStatus & SERVER_QUERY_NO_GOOD_INDEX_USED) != 0;
        this.queryNoIndexUsed = (this.serverStatus & SERVER_QUERY_NO_INDEX_USED) != 0;
        this.serverQueryWasSlow = (this.serverStatus & SERVER_QUERY_WAS_SLOW) != 0;
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

    /**
     * @param packet
     *            original uncompressed MySQL packet
     * @param offset
     *            begin of MySQL packet header
     * @param packetLen
     *            real length of packet
     * @return compressed packet with header
     * @throws SQLException
     */
    private Buffer compressPacket(Buffer packet, int offset, int packetLen) throws SQLException {

        // uncompressed payload by default
        int compressedLength = packetLen;
        int uncompressedLength = 0;
        byte[] compressedBytes = null;
        int offsetWrite = offset;

        if (packetLen < MIN_COMPRESS_LEN) {
            compressedBytes = packet.getByteBuffer();

        } else {
            byte[] bytesToCompress = packet.getByteBuffer();
            compressedBytes = new byte[bytesToCompress.length * 2];

            if (this.deflater == null) {
                this.deflater = new Deflater();
            }
            this.deflater.reset();
            this.deflater.setInput(bytesToCompress, offset, packetLen);
            this.deflater.finish();

            compressedLength = this.deflater.deflate(compressedBytes);

            if (compressedLength > packetLen) {
                // if compressed data is greater then uncompressed then send uncompressed
                compressedBytes = packet.getByteBuffer();
                compressedLength = packetLen;
            } else {
                uncompressedLength = packetLen;
                offsetWrite = 0;
            }
        }

        Buffer compressedPacket = new Buffer(HEADER_LENGTH + COMP_HEADER_LENGTH + compressedLength);

        compressedPacket.setPosition(0);
        compressedPacket.writeLongInt(compressedLength);
        compressedPacket.writeByte(this.compressedPacketSequence);
        compressedPacket.writeLongInt(uncompressedLength);
        compressedPacket.writeBytesNoNull(compressedBytes, offsetWrite, compressedLength);

        return compressedPacket;
    }

    private final void readServerStatusForResultSets(Buffer rowPacket) throws SQLException {
        if (this.use41Extensions) {
            rowPacket.readByte(); // skips the 'last packet' flag

            this.warningCount = rowPacket.readInt();

            if (this.warningCount > 0) {
                this.hadWarnings = true; // this is a 'latch', it's reset by sendCommand()
            }

            this.oldServerStatus = this.serverStatus;
            this.serverStatus = rowPacket.readInt();
            checkTransactionState(this.oldServerStatus);

            setServerSlowQueryFlags();
        }
    }

    private SocketFactory createSocketFactory() throws SQLException {
        try {
            if (this.socketFactoryClassName == null) {
                throw SQLError.createSQLException(Messages.getString("MysqlIO.75"), SQLError.SQL_STATE_UNABLE_TO_CONNECT_TO_DATASOURCE,
                        getExceptionInterceptor());
            }

            return (SocketFactory) (Class.forName(this.socketFactoryClassName).newInstance());
        } catch (Exception ex) {
            SQLException sqlEx = SQLError.createSQLException(Messages.getString("MysqlIO.76") + this.socketFactoryClassName + Messages.getString("MysqlIO.77"),
                    SQLError.SQL_STATE_UNABLE_TO_CONNECT_TO_DATASOURCE, getExceptionInterceptor());

            sqlEx.initCause(ex);

            throw sqlEx;
        }
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
        } else {
            int bytesToDump = Math.min(MAX_PACKET_DUMP_LENGTH, sendLength);

            String packetPayload = packet.dump(bytesToDump);

            packetDump = new StringBuilder(64 + 4 + packetPayload.length());

            packetDump.append("Client ");
            packetDump.append(packet.toSuperString());
            packetDump.append("--------------------> Server\n");
            packetDump.append("\nPacket payload:\n\n");
            packetDump.append(packetPayload);

            if (bytesToDump == MAX_PACKET_DUMP_LENGTH) {
                packetDump.append("\nNote: Packet of " + sendLength + " bytes truncated to " + MAX_PACKET_DUMP_LENGTH + " bytes.\n");
            }
        }

        this.packetDebugRingBuffer.addLast(packetDump);
    }

    private RowData readSingleRowSet(long columnCount, int maxRows, int resultSetConcurrency, boolean isBinaryEncoded, Field[] fields) throws SQLException {
        RowData rowData;
        ArrayList<ResultSetRow> rows = new ArrayList<ResultSetRow>();

        boolean useBufferRowExplicit = useBufferRowExplicit(fields);

        // Now read the data
        ResultSetRow row = nextRow(fields, (int) columnCount, isBinaryEncoded, resultSetConcurrency, false, useBufferRowExplicit, false, null);

        int rowCount = 0;

        if (row != null) {
            rows.add(row);
            rowCount = 1;
        }

        while (row != null) {
            row = nextRow(fields, (int) columnCount, isBinaryEncoded, resultSetConcurrency, false, useBufferRowExplicit, false, null);

            if (row != null) {
                if ((maxRows == -1) || (rowCount < maxRows)) {
                    rows.add(row);
                    rowCount++;
                }
            }
        }

        rowData = new RowDataStatic(rows);

        return rowData;
    }

    public static boolean useBufferRowExplicit(Field[] fields) {
        if (fields == null) {
            return false;
        }

        for (int i = 0; i < fields.length; i++) {
            switch (fields[i].getSQLType()) {
                case Types.BLOB:
                case Types.CLOB:
                case Types.LONGVARBINARY:
                case Types.LONGVARCHAR:
                    return true;
            }
        }

        return false;
    }

    /**
     * Don't hold on to overly-large packets
     */
    private void reclaimLargeReusablePacket() {
        if ((this.reusablePacket != null) && (this.reusablePacket.getCapacity() > 1048576)) {
            this.reusablePacket = new Buffer(INITIAL_PACKET_SIZE);
        }
    }

    /**
     * Re-use a packet to read from the MySQL server
     * 
     * @param reuse
     * @throws SQLException
     */
    private final Buffer reuseAndReadPacket(Buffer reuse) throws SQLException {
        return reuseAndReadPacket(reuse, -1);
    }

    private final Buffer reuseAndReadPacket(Buffer reuse, int existingPacketLength) throws SQLException {

        try {
            reuse.setWasMultiPacket(false);
            int packetLength = 0;

            if (existingPacketLength == -1) {
                int lengthRead = readFully(this.mysqlInput, this.packetHeaderBuf, 0, 4);

                if (lengthRead < 4) {
                    forceClose();
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
            int numBytesRead = readFully(this.mysqlInput, reuse.getByteBuffer(), 0, packetLength);

            if (numBytesRead != packetLength) {
                throw new IOException("Short read, expected " + packetLength + " bytes, only read " + numBytesRead);
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

            if (packetLength == this.maxThreeBytes) {
                reuse.setPosition(this.maxThreeBytes);

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
            throw SQLError.createCommunicationsException(this.connection, this.lastPacketSentTimeMs, this.lastPacketReceivedTimeMs, ioEx,
                    getExceptionInterceptor());
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
            final int lengthRead = readFully(this.mysqlInput, this.packetHeaderBuf, 0, 4);
            if (lengthRead < 4) {
                forceClose();
                throw new IOException(Messages.getString("MysqlIO.47"));
            }

            packetLength = (this.packetHeaderBuf[0] & 0xff) + ((this.packetHeaderBuf[1] & 0xff) << 8) + ((this.packetHeaderBuf[2] & 0xff) << 16);
            if (multiPacket == null) {
                multiPacket = new Buffer(packetLength);
            }

            if (!this.useNewLargePackets && (packetLength == 1)) {
                clearInputStream();
                break;
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

            int bytesRead = readFully(this.mysqlInput, byteBuf, 0, packetLength);

            if (bytesRead != lengthToWrite) {
                throw SQLError.createCommunicationsException(this.connection, this.lastPacketSentTimeMs, this.lastPacketReceivedTimeMs, SQLError
                        .createSQLException(Messages.getString("MysqlIO.50") + lengthToWrite + Messages.getString("MysqlIO.51") + bytesRead + ".",
                                getExceptionInterceptor()), getExceptionInterceptor());
            }

            reuse.writeBytesNoNull(byteBuf, 0, lengthToWrite);
        } while (packetLength == this.maxThreeBytes);

        reuse.setPosition(0);
        reuse.setWasMultiPacket(true);
        return packetLength;
    }

    /**
     * @param multiPacketSeq
     * @throws CommunicationsException
     */
    private void checkPacketSequencing(byte multiPacketSeq) throws SQLException {
        if ((multiPacketSeq == -128) && (this.readPacketSequence != 127)) {
            throw SQLError.createCommunicationsException(this.connection, this.lastPacketSentTimeMs, this.lastPacketReceivedTimeMs, new IOException(
                    "Packets out of order, expected packet # -128, but received packet # " + multiPacketSeq), getExceptionInterceptor());
        }

        if ((this.readPacketSequence == -1) && (multiPacketSeq != 0)) {
            throw SQLError.createCommunicationsException(this.connection, this.lastPacketSentTimeMs, this.lastPacketReceivedTimeMs, new IOException(
                    "Packets out of order, expected packet # -1, but received packet # " + multiPacketSeq), getExceptionInterceptor());
        }

        if ((multiPacketSeq != -128) && (this.readPacketSequence != -1) && (multiPacketSeq != (this.readPacketSequence + 1))) {
            throw SQLError.createCommunicationsException(this.connection, this.lastPacketSentTimeMs, this.lastPacketReceivedTimeMs, new IOException(
                    "Packets out of order, expected packet # " + (this.readPacketSequence + 1) + ", but received packet # " + multiPacketSeq),
                    getExceptionInterceptor());
        }
    }

    void enableMultiQueries() throws SQLException {
        Buffer buf = getSharedSendPacket();

        buf.clear();
        buf.writeByte((byte) MysqlDefs.COM_SET_OPTION);
        buf.writeInt(0);
        sendCommand(MysqlDefs.COM_SET_OPTION, null, buf, false, null, 0);
    }

    void disableMultiQueries() throws SQLException {
        Buffer buf = getSharedSendPacket();

        buf.clear();
        buf.writeByte((byte) MysqlDefs.COM_SET_OPTION);
        buf.writeInt(1);
        sendCommand(MysqlDefs.COM_SET_OPTION, null, buf, false, null, 0);
    }

    /**
     * @param packet
     * @param packetLen
     *            length of header + payload
     * @throws SQLException
     */
    private final void send(Buffer packet, int packetLen) throws SQLException {
        try {
            if (this.maxAllowedPacket > 0 && packetLen > this.maxAllowedPacket) {
                throw new PacketTooBigException(packetLen, this.maxAllowedPacket);
            }

            if ((this.serverMajorVersion >= 4)
                    && (packetLen - HEADER_LENGTH >= this.maxThreeBytes || (this.useCompression && packetLen - HEADER_LENGTH >= this.maxThreeBytes
                            - COMP_HEADER_LENGTH))) {
                sendSplitPackets(packet, packetLen);

            } else {
                this.packetSequence++;

                Buffer packetToSend = packet;
                packetToSend.setPosition(0);
                packetToSend.writeLongInt(packetLen - HEADER_LENGTH);
                packetToSend.writeByte(this.packetSequence);

                if (this.useCompression) {
                    this.compressedPacketSequence++;
                    int originalPacketLen = packetLen;

                    packetToSend = compressPacket(packetToSend, 0, packetLen);
                    packetLen = packetToSend.getPosition();

                    if (this.traceProtocol) {
                        StringBuilder traceMessageBuf = new StringBuilder();

                        traceMessageBuf.append(Messages.getString("MysqlIO.57"));
                        traceMessageBuf.append(getPacketDumpToLog(packetToSend, packetLen));
                        traceMessageBuf.append(Messages.getString("MysqlIO.58"));
                        traceMessageBuf.append(getPacketDumpToLog(packet, originalPacketLen));

                        this.connection.getLog().logTrace(traceMessageBuf.toString());
                    }
                } else {

                    if (this.traceProtocol) {
                        StringBuilder traceMessageBuf = new StringBuilder();

                        traceMessageBuf.append(Messages.getString("MysqlIO.59"));
                        traceMessageBuf.append("host: '");
                        traceMessageBuf.append(this.host);
                        traceMessageBuf.append("' threadId: '");
                        traceMessageBuf.append(this.threadId);
                        traceMessageBuf.append("'\n");
                        traceMessageBuf.append(packetToSend.dump(packetLen));

                        this.connection.getLog().logTrace(traceMessageBuf.toString());
                    }
                }

                this.mysqlOutput.write(packetToSend.getByteBuffer(), 0, packetLen);
                this.mysqlOutput.flush();
            }

            if (this.enablePacketDebug) {
                enqueuePacketForDebugging(true, false, packetLen + 5, this.packetHeaderBuf, packet);
            }

            //
            // Don't hold on to large packets
            //
            if (packet == this.sharedSendPacket) {
                reclaimLargeSharedSendPacket();
            }

            if (this.connection.getMaintainTimeStats()) {
                this.lastPacketSentTimeMs = System.currentTimeMillis();
            }
        } catch (IOException ioEx) {
            throw SQLError.createCommunicationsException(this.connection, this.lastPacketSentTimeMs, this.lastPacketReceivedTimeMs, ioEx,
                    getExceptionInterceptor());
        }
    }

    /**
     * Reads and sends a file to the server for LOAD DATA LOCAL INFILE
     * 
     * @param callingStatement
     * @param fileName
     *            the file name to send.
     * 
     * @throws SQLException
     */
    private final ResultSetImpl sendFileToServer(StatementImpl callingStatement, String fileName) throws SQLException {

        if (this.useCompression) {
            this.compressedPacketSequence++;
        }

        Buffer filePacket = (this.loadFileBufRef == null) ? null : this.loadFileBufRef.get();

        int bigPacketLength = Math.min(this.connection.getMaxAllowedPacket() - (HEADER_LENGTH * 3),
                alignPacketSize(this.connection.getMaxAllowedPacket() - 16, 4096) - (HEADER_LENGTH * 3));

        int oneMeg = 1024 * 1024;

        int smallerPacketSizeAligned = Math.min(oneMeg - (HEADER_LENGTH * 3), alignPacketSize(oneMeg - 16, 4096) - (HEADER_LENGTH * 3));

        int packetLength = Math.min(smallerPacketSizeAligned, bigPacketLength);

        if (filePacket == null) {
            try {
                filePacket = new Buffer((packetLength + HEADER_LENGTH));
                this.loadFileBufRef = new SoftReference<Buffer>(filePacket);
            } catch (OutOfMemoryError oom) {
                throw SQLError.createSQLException("Could not allocate packet of " + packetLength + " bytes required for LOAD DATA LOCAL INFILE operation."
                        + " Try increasing max heap allocation for JVM or decreasing server variable 'max_allowed_packet'",
                        SQLError.SQL_STATE_MEMORY_ALLOCATION_FAILURE, getExceptionInterceptor());

            }
        }

        filePacket.clear();
        send(filePacket, 0);

        byte[] fileBuf = new byte[packetLength];

        BufferedInputStream fileIn = null;

        try {
            if (!this.connection.getAllowLoadLocalInfile()) {
                throw SQLError.createSQLException(Messages.getString("MysqlIO.LoadDataLocalNotAllowed"), SQLError.SQL_STATE_GENERAL_ERROR,
                        getExceptionInterceptor());
            }

            InputStream hookedStream = null;

            if (callingStatement != null) {
                hookedStream = callingStatement.getLocalInfileInputStream();
            }

            if (hookedStream != null) {
                fileIn = new BufferedInputStream(hookedStream);
            } else if (!this.connection.getAllowUrlInLocalInfile()) {
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
                filePacket.clear();
                filePacket.writeBytesNoNull(fileBuf, 0, bytesRead);
                send(filePacket, filePacket.getPosition());
            }
        } catch (IOException ioEx) {
            StringBuilder messageBuf = new StringBuilder(Messages.getString("MysqlIO.60"));

            if (fileName != null && !this.connection.getParanoid()) {
                messageBuf.append("'");
                messageBuf.append(fileName);
                messageBuf.append("'");
            }

            messageBuf.append(Messages.getString("MysqlIO.63"));

            if (!this.connection.getParanoid()) {
                messageBuf.append(Messages.getString("MysqlIO.64"));
                messageBuf.append(Util.stackTraceToString(ioEx));
            }

            throw SQLError.createSQLException(messageBuf.toString(), SQLError.SQL_STATE_ILLEGAL_ARGUMENT, getExceptionInterceptor());
        } finally {
            if (fileIn != null) {
                try {
                    fileIn.close();
                } catch (Exception ex) {
                    SQLException sqlEx = SQLError.createSQLException(Messages.getString("MysqlIO.65"), SQLError.SQL_STATE_GENERAL_ERROR, ex,
                            getExceptionInterceptor());

                    throw sqlEx;
                }

                fileIn = null;
            } else {
                // file open failed, but server needs one packet
                filePacket.clear();
                send(filePacket, filePacket.getPosition());
                checkErrorPacket(); // to clear response off of queue
            }
        }

        // send empty packet to mark EOF
        filePacket.clear();
        send(filePacket, filePacket.getPosition());

        Buffer resultPacket = checkErrorPacket();

        return buildResultSetWithUpdates(callingStatement, resultPacket);
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
        this.serverStatus = 0;

        try {
            // Check return value, if we get a java.io.EOFException, the server has gone away. We'll pass it on up the exception chain and let someone higher up
            // decide what to do (barf, reconnect, etc).
            resultPacket = reuseAndReadPacket(this.reusablePacket);
        } catch (SQLException sqlEx) {
            // Don't wrap SQL Exceptions
            throw sqlEx;
        } catch (Exception fallThru) {
            throw SQLError.createCommunicationsException(this.connection, this.lastPacketSentTimeMs, this.lastPacketReceivedTimeMs, fallThru,
                    getExceptionInterceptor());
        }

        checkErrorPacket(resultPacket);

        return resultPacket;
    }

    private void checkErrorPacket(Buffer resultPacket) throws SQLException {

        int statusCode = resultPacket.readByte();

        // Error handling
        if (statusCode == (byte) 0xff) {
            String serverErrorMessage;
            int errno = 2000;

            if (this.protocolVersion > 9) {
                errno = resultPacket.readInt();

                String xOpen = null;

                serverErrorMessage = resultPacket.readString(this.connection.getErrorMessageEncoding(), getExceptionInterceptor());

                if (serverErrorMessage.charAt(0) == '#') {

                    // we have an SQLState
                    if (serverErrorMessage.length() > 6) {
                        xOpen = serverErrorMessage.substring(1, 6);
                        serverErrorMessage = serverErrorMessage.substring(6);

                        if (xOpen.equals("HY000")) {
                            xOpen = SQLError.mysqlToSqlState(errno, this.connection.getUseSqlStateCodes());
                        }
                    } else {
                        xOpen = SQLError.mysqlToSqlState(errno, this.connection.getUseSqlStateCodes());
                    }
                } else {
                    xOpen = SQLError.mysqlToSqlState(errno, this.connection.getUseSqlStateCodes());
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

            serverErrorMessage = resultPacket.readString(this.connection.getErrorMessageEncoding(), getExceptionInterceptor());
            clearInputStream();

            if (serverErrorMessage.indexOf(Messages.getString("MysqlIO.70")) != -1) {
                throw SQLError.createSQLException(SQLError.get(SQLError.SQL_STATE_COLUMN_NOT_FOUND) + ", " + serverErrorMessage,
                        SQLError.SQL_STATE_COLUMN_NOT_FOUND, -1, false, getExceptionInterceptor(), this.connection);
            }

            StringBuilder errorBuf = new StringBuilder(Messages.getString("MysqlIO.72"));
            errorBuf.append(serverErrorMessage);
            errorBuf.append("\"");

            throw SQLError.createSQLException(SQLError.get(SQLError.SQL_STATE_GENERAL_ERROR) + ", " + errorBuf.toString(), SQLError.SQL_STATE_GENERAL_ERROR,
                    -1, false, getExceptionInterceptor(), this.connection);
        }
    }

    private void appendDeadlockStatusInformation(String xOpen, StringBuilder errorBuf) throws SQLException {
        if (this.connection.getIncludeInnodbStatusInDeadlockExceptions() && xOpen != null && (xOpen.startsWith("40") || xOpen.startsWith("41"))
                && this.streamingData == null) {
            ResultSet rs = null;

            try {
                rs = sqlQueryDirect(null, "SHOW ENGINE INNODB STATUS", this.connection.getEncoding(), null, -1, ResultSet.TYPE_FORWARD_ONLY,
                        ResultSet.CONCUR_READ_ONLY, false, this.connection.getCatalog(), null);

                if (rs.next()) {
                    errorBuf.append("\n\n");
                    errorBuf.append(rs.getString("Status"));
                } else {
                    errorBuf.append("\n\n");
                    errorBuf.append(Messages.getString("MysqlIO.NoInnoDBStatusFound"));
                }
            } catch (Exception ex) {
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
     * Sends a large packet to the server as a series of smaller packets
     * 
     * @param packet
     * 
     * @throws SQLException
     * @throws CommunicationsException
     */
    private final void sendSplitPackets(Buffer packet, int packetLen) throws SQLException {
        try {
            Buffer packetToSend = (this.splitBufRef == null) ? null : this.splitBufRef.get();
            Buffer toCompress = (!this.useCompression || this.compressBufRef == null) ? null : this.compressBufRef.get();

            //
            // Store this packet in a soft reference...It can be re-used if not GC'd (so clients that use it frequently won't have to re-alloc the 16M buffer),
            // but we don't penalize infrequent users of large packets by keeping 16M allocated all of the time
            //
            if (packetToSend == null) {
                packetToSend = new Buffer((this.maxThreeBytes + HEADER_LENGTH));
                this.splitBufRef = new SoftReference<Buffer>(packetToSend);
            }
            if (this.useCompression) {
                int cbuflen = packetLen + ((packetLen / this.maxThreeBytes) + 1) * HEADER_LENGTH;
                if (toCompress == null) {
                    toCompress = new Buffer(cbuflen);
                } else if (toCompress.getBufLength() < cbuflen) {
                    toCompress.setPosition(toCompress.getBufLength());
                    toCompress.ensureCapacity(cbuflen - toCompress.getBufLength());
                }
            }

            int len = packetLen - HEADER_LENGTH; // payload length left
            int splitSize = this.maxThreeBytes;
            int originalPacketPos = HEADER_LENGTH;
            byte[] origPacketBytes = packet.getByteBuffer();

            int toCompressPosition = 0;

            // split to MySQL packets
            while (len >= 0) {
                this.packetSequence++;

                if (len < splitSize) {
                    splitSize = len;
                }

                packetToSend.setPosition(0);
                packetToSend.writeLongInt(splitSize);
                packetToSend.writeByte(this.packetSequence);
                if (len > 0) {
                    System.arraycopy(origPacketBytes, originalPacketPos, packetToSend.getByteBuffer(), HEADER_LENGTH, splitSize);
                }

                if (this.useCompression) {
                    System.arraycopy(packetToSend.getByteBuffer(), 0, toCompress.getByteBuffer(), toCompressPosition, HEADER_LENGTH + splitSize);
                    toCompressPosition += HEADER_LENGTH + splitSize;
                } else {
                    this.mysqlOutput.write(packetToSend.getByteBuffer(), 0, HEADER_LENGTH + splitSize);
                    this.mysqlOutput.flush();
                }

                originalPacketPos += splitSize;
                len -= this.maxThreeBytes;

            }

            // split to compressed packets
            if (this.useCompression) {
                len = toCompressPosition;
                toCompressPosition = 0;
                splitSize = this.maxThreeBytes - COMP_HEADER_LENGTH;
                while (len >= 0) {
                    this.compressedPacketSequence++;

                    if (len < splitSize) {
                        splitSize = len;
                    }

                    Buffer compressedPacketToSend = compressPacket(toCompress, toCompressPosition, splitSize);
                    packetLen = compressedPacketToSend.getPosition();
                    this.mysqlOutput.write(compressedPacketToSend.getByteBuffer(), 0, packetLen);
                    this.mysqlOutput.flush();

                    toCompressPosition += splitSize;
                    len -= (this.maxThreeBytes - COMP_HEADER_LENGTH);
                }
            }
        } catch (IOException ioEx) {
            throw SQLError.createCommunicationsException(this.connection, this.lastPacketSentTimeMs, this.lastPacketReceivedTimeMs, ioEx,
                    getExceptionInterceptor());
        }
    }

    private void reclaimLargeSharedSendPacket() {
        if ((this.sharedSendPacket != null) && (this.sharedSendPacket.getCapacity() > 1048576)) {
            this.sharedSendPacket = new Buffer(INITIAL_PACKET_SIZE);
        }
    }

    boolean hadWarnings() {
        return this.hadWarnings;
    }

    void scanForAndThrowDataTruncation() throws SQLException {
        if ((this.streamingData == null) && versionMeetsMinimum(4, 1, 0) && this.connection.getJdbcCompliantTruncation() && this.warningCount > 0) {
            SQLError.convertShowWarningsToSQLWarnings(this.connection, this.warningCount, true);
        }
    }

    /**
     * Secure authentication for 4.1 and newer servers.
     * 
     * @param packet
     * @param packLength
     * @param user
     * @param password
     * @param database
     * @param writeClientParams
     * 
     * @throws SQLException
     */
    private void secureAuth(Buffer packet, int packLength, String user, String password, String database, boolean writeClientParams) throws SQLException {
        // Passwords can be 16 chars long
        if (packet == null) {
            packet = new Buffer(packLength);
        }

        if (writeClientParams) {
            if (this.use41Extensions) {
                if (versionMeetsMinimum(4, 1, 1)) {
                    packet.writeLong(this.clientParam);
                    packet.writeLong(this.maxThreeBytes);

                    // charset, JDBC will connect as 'latin1', and use 'SET NAMES' to change to the desired charset after the connection is established.
                    packet.writeByte((byte) 8);

                    // Set of bytes reserved for future use.
                    packet.writeBytesNoNull(new byte[23]);
                } else {
                    packet.writeLong(this.clientParam);
                    packet.writeLong(this.maxThreeBytes);
                }
            } else {
                packet.writeInt((int) this.clientParam);
                packet.writeLongInt(this.maxThreeBytes);
            }
        }

        // User/Password data
        packet.writeString(user, CODE_PAGE_1252, this.connection);

        if (password.length() != 0) {
            /* Prepare false scramble */
            packet.writeString(FALSE_SCRAMBLE, CODE_PAGE_1252, this.connection);
        } else {
            /* For empty password */
            packet.writeString("", CODE_PAGE_1252, this.connection);
        }

        if (this.useConnectWithDb) {
            packet.writeString(database, CODE_PAGE_1252, this.connection);
        }

        send(packet, packet.getPosition());

        //
        // Don't continue stages if password is empty
        //
        if (password.length() > 0) {
            Buffer b = readPacket();

            b.setPosition(0);

            byte[] replyAsBytes = b.getByteBuffer();

            if ((replyAsBytes.length == 25) && (replyAsBytes[0] != 0)) {
                // Old passwords will have '*' at the first byte of hash */
                if (replyAsBytes[0] != '*') {
                    try {
                        /* Build full password hash as it is required to decode scramble */
                        byte[] buff = Security.passwordHashStage1(password);

                        /* Store copy as we'll need it later */
                        byte[] passwordHash = new byte[buff.length];
                        System.arraycopy(buff, 0, passwordHash, 0, buff.length);

                        /* Finally hash complete password using hash we got from server */
                        passwordHash = Security.passwordHashStage2(passwordHash, replyAsBytes);

                        byte[] packetDataAfterSalt = new byte[replyAsBytes.length - 5];

                        System.arraycopy(replyAsBytes, 4, packetDataAfterSalt, 0, replyAsBytes.length - 5);

                        byte[] mysqlScrambleBuff = new byte[SEED_LENGTH];

                        /* Decypt and store scramble 4 = hash for stage2 */
                        Security.xorString(packetDataAfterSalt, mysqlScrambleBuff, passwordHash, SEED_LENGTH);

                        /* Encode scramble with password. Recycle buffer */
                        Security.xorString(mysqlScrambleBuff, buff, buff, SEED_LENGTH);

                        Buffer packet2 = new Buffer(25);
                        packet2.writeBytesNoNull(buff);

                        this.packetSequence++;

                        send(packet2, 24);
                    } catch (NoSuchAlgorithmException nse) {
                        throw SQLError.createSQLException(Messages.getString("MysqlIO.91") + Messages.getString("MysqlIO.92"),
                                SQLError.SQL_STATE_GENERAL_ERROR, getExceptionInterceptor());
                    }
                } else {
                    try {
                        /* Create password to decode scramble */
                        byte[] passwordHash = Security.createKeyFromOldPassword(password);

                        /* Decypt and store scramble 4 = hash for stage2 */
                        byte[] netReadPos4 = new byte[replyAsBytes.length - 5];

                        System.arraycopy(replyAsBytes, 4, netReadPos4, 0, replyAsBytes.length - 5);

                        byte[] mysqlScrambleBuff = new byte[SEED_LENGTH];

                        /* Decypt and store scramble 4 = hash for stage2 */
                        Security.xorString(netReadPos4, mysqlScrambleBuff, passwordHash, SEED_LENGTH);

                        /* Finally scramble decoded scramble with password */
                        String scrambledPassword = Util.scramble(StringUtils.toString(mysqlScrambleBuff), password);

                        Buffer packet2 = new Buffer(packLength);
                        packet2.writeString(scrambledPassword, CODE_PAGE_1252, this.connection);
                        this.packetSequence++;

                        send(packet2, 24);
                    } catch (NoSuchAlgorithmException nse) {
                        throw SQLError.createSQLException(Messages.getString("MysqlIO.91") + Messages.getString("MysqlIO.92"),
                                SQLError.SQL_STATE_GENERAL_ERROR, getExceptionInterceptor());
                    }
                }
            }
        }
    }

    /**
     * Secure authentication for 4.1.1 and newer servers.
     * 
     * @param packet
     * @param packLength
     * @param user
     * @param password
     * @param database
     * @param writeClientParams
     * 
     * @throws SQLException
     */
    void secureAuth411(Buffer packet, int packLength, String user, String password, String database, boolean writeClientParams) throws SQLException {
        String enc = getEncodingForHandshake();
        //	SERVER:  public_seed=create_random_string()
        //			 send(public_seed)
        //
        //	CLIENT:  recv(public_seed)
        //			 hash_stage1=sha1("password")
        //			 hash_stage2=sha1(hash_stage1)
        //			 reply=xor(hash_stage1, sha1(public_seed,hash_stage2)
        //
        //			 // this three steps are done in scramble()
        //
        //			 send(reply)
        //
        //
        //	SERVER:  recv(reply)
        //			 hash_stage1=xor(reply, sha1(public_seed,hash_stage2))
        //			 candidate_hash2=sha1(hash_stage1)
        //			 check(candidate_hash2==hash_stage2)
        // Passwords can be 16 chars long
        if (packet == null) {
            packet = new Buffer(packLength);
        }

        if (writeClientParams) {
            if (this.use41Extensions) {
                if (versionMeetsMinimum(4, 1, 1)) {
                    packet.writeLong(this.clientParam);
                    packet.writeLong(this.maxThreeBytes);

                    appendCharsetByteForHandshake(packet, enc);

                    // Set of bytes reserved for future use.
                    packet.writeBytesNoNull(new byte[23]);
                } else {
                    packet.writeLong(this.clientParam);
                    packet.writeLong(this.maxThreeBytes);
                }
            } else {
                packet.writeInt((int) this.clientParam);
                packet.writeLongInt(this.maxThreeBytes);
            }
        }

        // User/Password data
        packet.writeString(user, enc, this.connection);

        if (password.length() != 0) {
            packet.writeByte((byte) 0x14);

            try {
                packet.writeBytesNoNull(Security.scramble411(password, this.seed, this.connection.getPasswordCharacterEncoding()));
            } catch (NoSuchAlgorithmException nse) {
                throw SQLError.createSQLException(Messages.getString("MysqlIO.91") + Messages.getString("MysqlIO.92"), SQLError.SQL_STATE_GENERAL_ERROR,
                        getExceptionInterceptor());
            } catch (UnsupportedEncodingException e) {
                throw SQLError.createSQLException(Messages.getString("MysqlIO.91") + Messages.getString("MysqlIO.92"), SQLError.SQL_STATE_GENERAL_ERROR,
                        getExceptionInterceptor());
            }
        } else {
            /* For empty password */
            packet.writeByte((byte) 0);
        }

        if (this.useConnectWithDb) {
            packet.writeString(database, enc, this.connection);
        } else {
            /* For empty database */
            packet.writeByte((byte) 0);
        }

        // connection attributes
        if ((this.serverCapabilities & CLIENT_CONNECT_ATTRS) != 0) {
            sendConnectionAttributes(packet, enc, this.connection);
        }

        send(packet, packet.getPosition());

        byte savePacketSequence = this.packetSequence++;

        Buffer reply = checkErrorPacket();

        if (reply.isLastDataPacket()) {
            /*
             * By sending this very specific reply server asks us to send scrambled password in old format. The reply contains scramble_323.
             */
            this.packetSequence = ++savePacketSequence;
            packet.clear();

            String seed323 = this.seed.substring(0, 8);
            packet.writeString(Util.newCrypt(password, seed323, this.connection.getPasswordCharacterEncoding()));
            send(packet, packet.getPosition());

            /* Read what server thinks about out new auth message report */
            checkErrorPacket();
        }
    }

    /**
     * Un-packs binary-encoded result set data for one row
     * 
     * @param fields
     * @param binaryData
     * @param resultSetConcurrency
     * 
     * @return byte[][]
     * 
     * @throws SQLException
     */
    private final ResultSetRow unpackBinaryResultSetRow(Field[] fields, Buffer binaryData, int resultSetConcurrency) throws SQLException {
        int numFields = fields.length;

        byte[][] unpackedRowData = new byte[numFields][];

        //
        // Unpack the null bitmask, first
        //

        int nullCount = (numFields + 9) / 8;
        int nullMaskPos = binaryData.getPosition();
        binaryData.setPosition(nullMaskPos + nullCount);
        int bit = 4; // first two bits are reserved for future use

        //
        // TODO: Benchmark if moving check for updatable result sets out of loop is worthwhile?
        //

        for (int i = 0; i < numFields; i++) {
            if ((binaryData.readByte(nullMaskPos) & bit) != 0) {
                unpackedRowData[i] = null;
            } else {
                if (resultSetConcurrency != ResultSet.CONCUR_UPDATABLE) {
                    extractNativeEncodedColumn(binaryData, fields, i, unpackedRowData);
                } else {
                    unpackNativeEncodedColumn(binaryData, fields, i, unpackedRowData);
                }
            }

            if (((bit <<= 1) & 255) == 0) {
                bit = 1; /* To next byte */

                nullMaskPos++;
            }
        }

        return new ByteArrayRow(unpackedRowData, getExceptionInterceptor());
    }

    private final void extractNativeEncodedColumn(Buffer binaryData, Field[] fields, int columnIndex, byte[][] unpackedRowData) throws SQLException {
        Field curField = fields[columnIndex];

        switch (curField.getMysqlType()) {
            case MysqlDefs.FIELD_TYPE_NULL:
                break; // for dummy binds

            case MysqlDefs.FIELD_TYPE_TINY:

                unpackedRowData[columnIndex] = new byte[] { binaryData.readByte() };
                break;

            case MysqlDefs.FIELD_TYPE_SHORT:
            case MysqlDefs.FIELD_TYPE_YEAR:

                unpackedRowData[columnIndex] = binaryData.getBytes(2);
                break;
            case MysqlDefs.FIELD_TYPE_LONG:
            case MysqlDefs.FIELD_TYPE_INT24:

                unpackedRowData[columnIndex] = binaryData.getBytes(4);
                break;
            case MysqlDefs.FIELD_TYPE_LONGLONG:

                unpackedRowData[columnIndex] = binaryData.getBytes(8);
                break;
            case MysqlDefs.FIELD_TYPE_FLOAT:

                unpackedRowData[columnIndex] = binaryData.getBytes(4);
                break;
            case MysqlDefs.FIELD_TYPE_DOUBLE:

                unpackedRowData[columnIndex] = binaryData.getBytes(8);
                break;
            case MysqlDefs.FIELD_TYPE_TIME:

                int length = (int) binaryData.readFieldLength();

                unpackedRowData[columnIndex] = binaryData.getBytes(length);

                break;
            case MysqlDefs.FIELD_TYPE_DATE:

                length = (int) binaryData.readFieldLength();

                unpackedRowData[columnIndex] = binaryData.getBytes(length);

                break;
            case MysqlDefs.FIELD_TYPE_DATETIME:
            case MysqlDefs.FIELD_TYPE_TIMESTAMP:
                length = (int) binaryData.readFieldLength();

                unpackedRowData[columnIndex] = binaryData.getBytes(length);
                break;
            case MysqlDefs.FIELD_TYPE_TINY_BLOB:
            case MysqlDefs.FIELD_TYPE_MEDIUM_BLOB:
            case MysqlDefs.FIELD_TYPE_LONG_BLOB:
            case MysqlDefs.FIELD_TYPE_BLOB:
            case MysqlDefs.FIELD_TYPE_VAR_STRING:
            case MysqlDefs.FIELD_TYPE_VARCHAR:
            case MysqlDefs.FIELD_TYPE_STRING:
            case MysqlDefs.FIELD_TYPE_DECIMAL:
            case MysqlDefs.FIELD_TYPE_NEW_DECIMAL:
            case MysqlDefs.FIELD_TYPE_GEOMETRY:
            case MysqlDefs.FIELD_TYPE_BIT:
            case MysqlDefs.FIELD_TYPE_JSON:
                unpackedRowData[columnIndex] = binaryData.readLenByteArray(0);

                break;
            default:
                throw SQLError.createSQLException(Messages.getString("MysqlIO.97") + curField.getMysqlType() + Messages.getString("MysqlIO.98") + columnIndex
                        + Messages.getString("MysqlIO.99") + fields.length + Messages.getString("MysqlIO.100"), SQLError.SQL_STATE_GENERAL_ERROR,
                        getExceptionInterceptor());
        }
    }

    private final void unpackNativeEncodedColumn(Buffer binaryData, Field[] fields, int columnIndex, byte[][] unpackedRowData) throws SQLException {
        Field curField = fields[columnIndex];

        switch (curField.getMysqlType()) {
            case MysqlDefs.FIELD_TYPE_NULL:
                break; // for dummy binds

            case MysqlDefs.FIELD_TYPE_TINY:

                byte tinyVal = binaryData.readByte();

                if (!curField.isUnsigned()) {
                    unpackedRowData[columnIndex] = StringUtils.getBytes(String.valueOf(tinyVal));
                } else {
                    short unsignedTinyVal = (short) (tinyVal & 0xff);

                    unpackedRowData[columnIndex] = StringUtils.getBytes(String.valueOf(unsignedTinyVal));
                }

                break;

            case MysqlDefs.FIELD_TYPE_SHORT:
            case MysqlDefs.FIELD_TYPE_YEAR:

                short shortVal = (short) binaryData.readInt();

                if (!curField.isUnsigned()) {
                    unpackedRowData[columnIndex] = StringUtils.getBytes(String.valueOf(shortVal));
                } else {
                    int unsignedShortVal = shortVal & 0xffff;

                    unpackedRowData[columnIndex] = StringUtils.getBytes(String.valueOf(unsignedShortVal));
                }

                break;

            case MysqlDefs.FIELD_TYPE_LONG:
            case MysqlDefs.FIELD_TYPE_INT24:

                int intVal = (int) binaryData.readLong();

                if (!curField.isUnsigned()) {
                    unpackedRowData[columnIndex] = StringUtils.getBytes(String.valueOf(intVal));
                } else {
                    long longVal = intVal & 0xffffffffL;

                    unpackedRowData[columnIndex] = StringUtils.getBytes(String.valueOf(longVal));
                }

                break;

            case MysqlDefs.FIELD_TYPE_LONGLONG:

                long longVal = binaryData.readLongLong();

                if (!curField.isUnsigned()) {
                    unpackedRowData[columnIndex] = StringUtils.getBytes(String.valueOf(longVal));
                } else {
                    BigInteger asBigInteger = ResultSetImpl.convertLongToUlong(longVal);

                    unpackedRowData[columnIndex] = StringUtils.getBytes(asBigInteger.toString());
                }

                break;

            case MysqlDefs.FIELD_TYPE_FLOAT:

                float floatVal = Float.intBitsToFloat(binaryData.readIntAsLong());

                unpackedRowData[columnIndex] = StringUtils.getBytes(String.valueOf(floatVal));

                break;

            case MysqlDefs.FIELD_TYPE_DOUBLE:

                double doubleVal = Double.longBitsToDouble(binaryData.readLongLong());

                unpackedRowData[columnIndex] = StringUtils.getBytes(String.valueOf(doubleVal));

                break;

            case MysqlDefs.FIELD_TYPE_TIME:

                int length = (int) binaryData.readFieldLength();

                int hour = 0;
                int minute = 0;
                int seconds = 0;

                if (length != 0) {
                    binaryData.readByte(); // skip tm->neg
                    binaryData.readLong(); // skip daysPart
                    hour = binaryData.readByte();
                    minute = binaryData.readByte();
                    seconds = binaryData.readByte();

                    if (length > 8) {
                        binaryData.readLong(); // ignore 'secondsPart'
                    }
                }

                byte[] timeAsBytes = new byte[8];

                timeAsBytes[0] = (byte) Character.forDigit(hour / 10, 10);
                timeAsBytes[1] = (byte) Character.forDigit(hour % 10, 10);

                timeAsBytes[2] = (byte) ':';

                timeAsBytes[3] = (byte) Character.forDigit(minute / 10, 10);
                timeAsBytes[4] = (byte) Character.forDigit(minute % 10, 10);

                timeAsBytes[5] = (byte) ':';

                timeAsBytes[6] = (byte) Character.forDigit(seconds / 10, 10);
                timeAsBytes[7] = (byte) Character.forDigit(seconds % 10, 10);

                unpackedRowData[columnIndex] = timeAsBytes;

                break;

            case MysqlDefs.FIELD_TYPE_DATE:
                length = (int) binaryData.readFieldLength();

                int year = 0;
                int month = 0;
                int day = 0;

                hour = 0;
                minute = 0;
                seconds = 0;

                if (length != 0) {
                    year = binaryData.readInt();
                    month = binaryData.readByte();
                    day = binaryData.readByte();
                }

                if ((year == 0) && (month == 0) && (day == 0)) {
                    if (ConnectionPropertiesImpl.ZERO_DATETIME_BEHAVIOR_CONVERT_TO_NULL.equals(this.connection.getZeroDateTimeBehavior())) {
                        unpackedRowData[columnIndex] = null;

                        break;
                    } else if (ConnectionPropertiesImpl.ZERO_DATETIME_BEHAVIOR_EXCEPTION.equals(this.connection.getZeroDateTimeBehavior())) {
                        throw SQLError.createSQLException("Value '0000-00-00' can not be represented as java.sql.Date", SQLError.SQL_STATE_ILLEGAL_ARGUMENT,
                                getExceptionInterceptor());
                    }

                    year = 1;
                    month = 1;
                    day = 1;
                }

                byte[] dateAsBytes = new byte[10];

                dateAsBytes[0] = (byte) Character.forDigit(year / 1000, 10);

                int after1000 = year % 1000;

                dateAsBytes[1] = (byte) Character.forDigit(after1000 / 100, 10);

                int after100 = after1000 % 100;

                dateAsBytes[2] = (byte) Character.forDigit(after100 / 10, 10);
                dateAsBytes[3] = (byte) Character.forDigit(after100 % 10, 10);

                dateAsBytes[4] = (byte) '-';

                dateAsBytes[5] = (byte) Character.forDigit(month / 10, 10);
                dateAsBytes[6] = (byte) Character.forDigit(month % 10, 10);

                dateAsBytes[7] = (byte) '-';

                dateAsBytes[8] = (byte) Character.forDigit(day / 10, 10);
                dateAsBytes[9] = (byte) Character.forDigit(day % 10, 10);

                unpackedRowData[columnIndex] = dateAsBytes;

                break;

            case MysqlDefs.FIELD_TYPE_DATETIME:
            case MysqlDefs.FIELD_TYPE_TIMESTAMP:
                length = (int) binaryData.readFieldLength();

                year = 0;
                month = 0;
                day = 0;

                hour = 0;
                minute = 0;
                seconds = 0;

                int nanos = 0;

                if (length != 0) {
                    year = binaryData.readInt();
                    month = binaryData.readByte();
                    day = binaryData.readByte();

                    if (length > 4) {
                        hour = binaryData.readByte();
                        minute = binaryData.readByte();
                        seconds = binaryData.readByte();
                    }

                    //if (length > 7) {
                    //    nanos = (int)binaryData.readLong();
                    //}
                }

                if ((year == 0) && (month == 0) && (day == 0)) {
                    if (ConnectionPropertiesImpl.ZERO_DATETIME_BEHAVIOR_CONVERT_TO_NULL.equals(this.connection.getZeroDateTimeBehavior())) {
                        unpackedRowData[columnIndex] = null;

                        break;
                    } else if (ConnectionPropertiesImpl.ZERO_DATETIME_BEHAVIOR_EXCEPTION.equals(this.connection.getZeroDateTimeBehavior())) {
                        throw SQLError.createSQLException("Value '0000-00-00' can not be represented as java.sql.Timestamp",
                                SQLError.SQL_STATE_ILLEGAL_ARGUMENT, getExceptionInterceptor());
                    }

                    year = 1;
                    month = 1;
                    day = 1;
                }

                int stringLength = 19;

                byte[] nanosAsBytes = StringUtils.getBytes(Integer.toString(nanos));

                stringLength += (1 + nanosAsBytes.length); // '.' + # of digits

                byte[] datetimeAsBytes = new byte[stringLength];

                datetimeAsBytes[0] = (byte) Character.forDigit(year / 1000, 10);

                after1000 = year % 1000;

                datetimeAsBytes[1] = (byte) Character.forDigit(after1000 / 100, 10);

                after100 = after1000 % 100;

                datetimeAsBytes[2] = (byte) Character.forDigit(after100 / 10, 10);
                datetimeAsBytes[3] = (byte) Character.forDigit(after100 % 10, 10);

                datetimeAsBytes[4] = (byte) '-';

                datetimeAsBytes[5] = (byte) Character.forDigit(month / 10, 10);
                datetimeAsBytes[6] = (byte) Character.forDigit(month % 10, 10);

                datetimeAsBytes[7] = (byte) '-';

                datetimeAsBytes[8] = (byte) Character.forDigit(day / 10, 10);
                datetimeAsBytes[9] = (byte) Character.forDigit(day % 10, 10);

                datetimeAsBytes[10] = (byte) ' ';

                datetimeAsBytes[11] = (byte) Character.forDigit(hour / 10, 10);
                datetimeAsBytes[12] = (byte) Character.forDigit(hour % 10, 10);

                datetimeAsBytes[13] = (byte) ':';

                datetimeAsBytes[14] = (byte) Character.forDigit(minute / 10, 10);
                datetimeAsBytes[15] = (byte) Character.forDigit(minute % 10, 10);

                datetimeAsBytes[16] = (byte) ':';

                datetimeAsBytes[17] = (byte) Character.forDigit(seconds / 10, 10);
                datetimeAsBytes[18] = (byte) Character.forDigit(seconds % 10, 10);

                datetimeAsBytes[19] = (byte) '.';

                final int nanosOffset = 20;

                System.arraycopy(nanosAsBytes, 0, datetimeAsBytes, nanosOffset, nanosAsBytes.length);

                unpackedRowData[columnIndex] = datetimeAsBytes;

                break;

            case MysqlDefs.FIELD_TYPE_TINY_BLOB:
            case MysqlDefs.FIELD_TYPE_MEDIUM_BLOB:
            case MysqlDefs.FIELD_TYPE_LONG_BLOB:
            case MysqlDefs.FIELD_TYPE_BLOB:
            case MysqlDefs.FIELD_TYPE_VAR_STRING:
            case MysqlDefs.FIELD_TYPE_STRING:
            case MysqlDefs.FIELD_TYPE_VARCHAR:
            case MysqlDefs.FIELD_TYPE_DECIMAL:
            case MysqlDefs.FIELD_TYPE_NEW_DECIMAL:
            case MysqlDefs.FIELD_TYPE_BIT:
                unpackedRowData[columnIndex] = binaryData.readLenByteArray(0);

                break;

            default:
                throw SQLError.createSQLException(Messages.getString("MysqlIO.97") + curField.getMysqlType() + Messages.getString("MysqlIO.98") + columnIndex
                        + Messages.getString("MysqlIO.99") + fields.length + Messages.getString("MysqlIO.100"), SQLError.SQL_STATE_GENERAL_ERROR,
                        getExceptionInterceptor());
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
    private void negotiateSSLConnection(String user, String password, String database, int packLength) throws SQLException {
        if (!ExportControlled.enabled()) {
            throw new ConnectionFeatureNotAvailableException(this.connection, this.lastPacketSentTimeMs, null);
        }

        if ((this.serverCapabilities & CLIENT_SECURE_CONNECTION) != 0) {
            this.clientParam |= CLIENT_SECURE_CONNECTION;
        }

        this.clientParam |= CLIENT_SSL;

        Buffer packet = new Buffer(packLength);

        if (this.use41Extensions) {
            packet.writeLong(this.clientParam);
            packet.writeLong(this.maxThreeBytes);
            appendCharsetByteForHandshake(packet, getEncodingForHandshake());
            packet.writeBytesNoNull(new byte[23]);	// Set of bytes reserved for future use.
        } else {
            packet.writeInt((int) this.clientParam);
        }

        send(packet, packet.getPosition());

        ExportControlled.transformSocketToSSLSocket(this);
    }

    public boolean isSSLEstablished() {
        return ExportControlled.enabled() && ExportControlled.isSSLEstablished(this);
    }

    protected int getServerStatus() {
        return this.serverStatus;
    }

    protected List<ResultSetRow> fetchRowsViaCursor(List<ResultSetRow> fetchedRows, long statementId, Field[] columnTypes, int fetchSize,
            boolean useBufferRowExplicit) throws SQLException {

        if (fetchedRows == null) {
            fetchedRows = new ArrayList<ResultSetRow>(fetchSize);
        } else {
            fetchedRows.clear();
        }

        this.sharedSendPacket.clear();

        this.sharedSendPacket.writeByte((byte) MysqlDefs.COM_FETCH);
        this.sharedSendPacket.writeLong(statementId);
        this.sharedSendPacket.writeLong(fetchSize);

        sendCommand(MysqlDefs.COM_FETCH, null, this.sharedSendPacket, true, null, 0);

        ResultSetRow row = null;

        while ((row = nextRow(columnTypes, columnTypes.length, true, ResultSet.CONCUR_READ_ONLY, false, useBufferRowExplicit, false, null)) != null) {
            fetchedRows.add(row);
        }

        return fetchedRows;
    }

    protected long getThreadId() {
        return this.threadId;
    }

    protected boolean useNanosForElapsedTime() {
        return this.useNanosForElapsedTime;
    }

    protected long getSlowQueryThreshold() {
        return this.slowQueryThreshold;
    }

    protected String getQueryTimingUnits() {
        return this.queryTimingUnits;
    }

    protected int getCommandCount() {
        return this.commandCount;
    }

    private void checkTransactionState(int oldStatus) throws SQLException {
        boolean previouslyInTrans = ((oldStatus & SERVER_STATUS_IN_TRANS) != 0);
        boolean currentlyInTrans = ((this.serverStatus & SERVER_STATUS_IN_TRANS) != 0);

        if (previouslyInTrans && !currentlyInTrans) {
            this.connection.transactionCompleted();
        } else if (!previouslyInTrans && currentlyInTrans) {
            this.connection.transactionBegun();
        }
    }

    protected void setStatementInterceptors(List<StatementInterceptorV2> statementInterceptors) {
        this.statementInterceptors = statementInterceptors.isEmpty() ? null : statementInterceptors;
    }

    protected ExceptionInterceptor getExceptionInterceptor() {
        return this.exceptionInterceptor;
    }

    protected void setSocketTimeout(int milliseconds) throws SQLException {
        try {
            this.mysqlConnection.setSoTimeout(milliseconds);
        } catch (SocketException e) {
            SQLException sqlEx = SQLError.createSQLException("Invalid socket timeout value or state", SQLError.SQL_STATE_ILLEGAL_ARGUMENT,
                    getExceptionInterceptor());
            sqlEx.initCause(e);

            throw sqlEx;
        }
    }

    protected void releaseResources() {
        if (this.deflater != null) {
            this.deflater.end();
            this.deflater = null;
        }
    }

    /**
     * Get the Java encoding to be used for the handshake
     * response. Defaults to UTF-8.
     */
    String getEncodingForHandshake() {
        String enc = this.connection.getEncoding();
        if (enc == null) {
            enc = "UTF-8";
        }
        return enc;
    }

    /**
     * Append the MySQL collation index to the handshake packet. A
     * single byte will be added to the packet corresponding to the
     * collation index found for the requested Java encoding name.
     * 
     * If the index is &gt; 255 which may be valid at some point in
     * the future, an exception will be thrown. At the time of this
     * implementation the index cannot be &gt; 255 and only the
     * COM_CHANGE_USER rpc, not the handshake response, can handle a
     * value &gt; 255.
     * 
     * @param packet
     *            to append to
     * @param end
     *            The Java encoding name used to lookup the collation index
     */
    private void appendCharsetByteForHandshake(Buffer packet, String enc) throws SQLException {
        int charsetIndex = 0;
        if (enc != null) {
            charsetIndex = CharsetMapping.getCollationIndexForJavaEncoding(enc, this.connection);
        }
        if (charsetIndex == 0) {
            charsetIndex = CharsetMapping.MYSQL_COLLATION_INDEX_utf8;
        }
        if (charsetIndex > 255) {
            throw SQLError.createSQLException("Invalid character set index for encoding: " + enc, SQLError.SQL_STATE_ILLEGAL_ARGUMENT,
                    getExceptionInterceptor());
        }
        packet.writeByte((byte) charsetIndex);
    }
}
