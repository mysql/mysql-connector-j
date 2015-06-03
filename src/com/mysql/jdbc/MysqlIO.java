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

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.lang.ref.SoftReference;
import java.net.MalformedURLException;
import java.net.SocketException;
import java.net.URL;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;

import com.mysql.cj.api.ProfilerEvent;
import com.mysql.cj.api.ProfilerEventHandler;
import com.mysql.cj.api.SessionState;
import com.mysql.cj.api.conf.PropertySet;
import com.mysql.cj.api.io.PacketBuffer;
import com.mysql.cj.api.io.SocketFactory;
import com.mysql.cj.core.Constants;
import com.mysql.cj.core.Messages;
import com.mysql.cj.core.ServerVersion;
import com.mysql.cj.core.conf.PropertyDefinitions;
import com.mysql.cj.core.exception.CJConnectionFeatureNotAvailableException;
import com.mysql.cj.core.exception.CJException;
import com.mysql.cj.core.exception.ExceptionFactory;
import com.mysql.cj.core.exception.FeatureNotAvailableException;
import com.mysql.cj.core.exception.UnableToConnectException;
import com.mysql.cj.core.io.Buffer;
import com.mysql.cj.core.io.CompressedInputStream;
import com.mysql.cj.core.io.CompressedPacketSender;
import com.mysql.cj.core.io.CoreIO;
import com.mysql.cj.core.io.ExportControlled;
import com.mysql.cj.core.io.MysqlBinaryValueDecoder;
import com.mysql.cj.core.io.MysqlTextValueDecoder;
import com.mysql.cj.core.io.NetworkResources;
import com.mysql.cj.core.io.ProtocolConstants;
import com.mysql.cj.core.io.ReadAheadInputStream;
import com.mysql.cj.core.io.SimplePacketSender;
import com.mysql.cj.core.profiler.ProfilerEventHandlerFactory;
import com.mysql.cj.core.profiler.ProfilerEventImpl;
import com.mysql.cj.core.util.LogUtils;
import com.mysql.cj.core.util.ProtocolUtils;
import com.mysql.cj.core.util.StringUtils;
import com.mysql.cj.core.util.TestUtils;
import com.mysql.cj.core.util.Util;
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

/**
 * This class is used by Connection for communicating with the MySQL server.
 */
public class MysqlIO extends CoreIO {
    protected static final int NULL_LENGTH = ~0;
    protected static final int COMP_HEADER_LENGTH = 3;
    protected static final int MIN_COMPRESS_LEN = 50;
    private static int maxBufferSize = 65535;

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

    //
    // Use this when reading in rows to avoid thousands of new() calls, because the byte arrays just get copied out of the packet anyway
    //
    private Buffer reusablePacket = null;
    private Buffer sendPacket = null;
    private Buffer sharedSendPacket = null;

    /** Data to the server */
    protected MysqlJdbcConnection connection;
    private RowData streamingData = null;
    /** Track this to manually shut down. */
    private CompressedPacketSender compressedPacketSender;

    //
    // Packet used for 'LOAD DATA LOCAL INFILE'
    //
    // We use a SoftReference, so that we don't penalize intermittent use of this feature
    //
    private SoftReference<Buffer> loadFileBufRef;

    private String socketFactoryClassName = null;
    private byte[] packetHeaderBuf = new byte[4];
    private boolean hadWarnings = false;

    /** Does the server support long column info? */
    private boolean logSlowQueries = false;

    /**
     * Does the character set of this connection match the character set of the
     * platform
     */
    private boolean platformDbCharsetMatches = true; // changed once we've connected.
    private boolean profileSQL = false;
    private boolean queryBadIndexUsed = false;
    private boolean queryNoIndexUsed = false;
    private boolean serverQueryWasSlow = false;

    private boolean useCompression = false;
    private byte packetSequence = 0;
    private byte readPacketSequence = -1;
    private boolean checkPacketSequence = false;
    private int maxAllowedPacket = 1024 * 1024;
    private int warningCount = 0;
    private boolean needToGrabQueryFromPacket;
    private boolean autoGenerateTestcaseScript;
    private boolean useNanosForElapsedTime;
    private long slowQueryThreshold;
    private String queryTimingUnits;
    private boolean useDirectRowUnpack = true;
    private int useBufferRowSizeThreshold;
    private int commandCount = 0;
    private List<StatementInterceptorV2> statementInterceptors;

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
    public MysqlIO(String host, int port, Properties props, String socketFactoryClassName, MysqlJdbcConnection conn, int socketTimeout,
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
            this.mysqlSocket = this.socketFactory.connect(this.host, this.port, props, DriverManager.getLoginTimeout() * 1000);

            if (socketTimeout != 0) {
                try {
                    this.mysqlSocket.setSoTimeout(socketTimeout);
                } catch (Exception ex) {
                    /* Ignore if the platform does not support it */
                }
            }

            this.mysqlSocket = this.socketFactory.beforeHandshake();

            if (this.connection.getUseReadAheadInput()) {
                this.mysqlInput = new ReadAheadInputStream(this.mysqlSocket.getInputStream(), 16384, this.connection.getTraceProtocol(),
                        this.connection.getLog());
            } else if (this.connection.useUnbufferedInput()) {
                this.mysqlInput = this.mysqlSocket.getInputStream();
            } else {
                this.mysqlInput = new BufferedInputStream(this.mysqlSocket.getInputStream(), 16384);
            }

            this.mysqlOutput = new BufferedOutputStream(this.mysqlSocket.getOutputStream(), 16384);

            this.packetSender = new SimplePacketSender(this.mysqlOutput);

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
        } catch (IOException ioEx) {
            throw SQLError.createCommunicationsException(this.connection, 0, 0, ioEx, getExceptionInterceptor());
        }
    }

    protected boolean isDataAvailable() throws SQLException {
        try {
            return this.mysqlInput.available() > 0;
        } catch (IOException ioEx) {
            throw SQLError.createCommunicationsException(this.connection, this.packetSentTimeHolder.getLastPacketSentTime(), this.lastPacketReceivedTimeMs,
                    ioEx, getExceptionInterceptor());
        }
    }

    /**
     * @return Returns the last packet sent time in ms.
     */
    @Override
    public long getLastPacketSentTimeMs() {
        return this.packetSentTimeHolder.getLastPacketSentTime();
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

        if (this.connection.getUseCursorFetch() && isBinaryEncoded && callingStatement != null && callingStatement.getFetchSize() != 0
                && callingStatement.getResultSetType() == ResultSet.TYPE_FORWARD_ONLY) {
            ServerPreparedStatement prepStmt = (com.mysql.jdbc.ServerPreparedStatement) callingStatement;

            boolean usingCursor = true;

            //
            // Server versions 5.0.5 or newer will only open a cursor and set this flag if they can, otherwise they punt and go back to mysql_store_results()
            // behavior
            //
            usingCursor = this.getSession().getSessionState().cursorExists();

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
        return new NetworkResources(this.mysqlSocket, this.mysqlInput, this.mysqlOutput);
    }

    /**
     * Forcibly closes the underlying socket to MySQL.
     */
    protected final void forceClose() {
        try {
            getNetworkResources().forceClose();
        } finally {
            this.mysqlSocket = null;
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

    /**
     * Unpacks the Field information from the given packet.
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
        // we only store the position of the string and
        // materialize only if needed...
        int catalogNameStart = packet.getPosition() + 1;
        int catalogNameLength = packet.fastSkipLenString();
        catalogNameStart = adjustStartForFieldLength(catalogNameStart, catalogNameLength);

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

        colLength = packet.readLong();

        int colType = packet.readByte() & 0xff;

        short colFlag = 0;

        if (getSession().getSessionState().hasLongColumnInfo()) {
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
                originalTableNameStart, originalTableNameLength, nameStart, nameLength, originalColumnNameStart, originalColumnNameLength, colLength, colType,
                colFlag, colDecimals, defaultValueStart, defaultValueLength, charSetNumber);

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
        if (this.connection.getElideSetAutoCommits()) {
            boolean autoCommitModeOnServer = this.getSession().getSessionState().isAutocommit();

            if (!autoCommitFlag) {
                // Just to be safe, check if a transaction is in progress on the server....
                // if so, then we must be in autoCommit == false
                // therefore return the opposite of transaction status
                boolean inTransactionOnServer = this.getSession().getSessionState().inTransactionOnServer();

                return !inTransactionOnServer;
            }

            return autoCommitModeOnServer != autoCommitFlag;
        }

        return true;
    }

    /**
     * Re-authenticates as the given user and password
     * 
     * @param userName
     * @param password
     * @param database
     * 
     */
    protected void changeUser(String userName, String password, String database) {
        this.packetSequence = -1;

        this.getSession().changeUser(userName, password, database);
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

    protected void clearInputStream() throws SQLException {
        try {
            int len;

            // Due to a bug in some older Linux kernels (fixed after the patch "tcp: fix FIONREAD/SIOCINQ"), our SocketInputStream.available() may return 1 even
            // if there is no data in the Stream, so, we need to check if InputStream.skip() actually skipped anything.
            while ((len = this.mysqlInput.available()) > 0 && this.mysqlInput.skip(len) > 0) {
                continue;
            }
        } catch (IOException ioEx) {
            throw SQLError.createCommunicationsException(this.connection, this.packetSentTimeHolder.getLastPacketSentTime(), this.lastPacketReceivedTimeMs,
                    ioEx, getExceptionInterceptor());
        }
    }

    protected void resetReadPacketSequence() {
        this.readPacketSequence = 0;
    }

    protected void dumpPacketRingBuffer() {
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

    static int getMaxBuf() {
        return maxBufferSize;
    }

    /**
     * Get the version of the MySQL server we are talking to.
     */
    final ServerVersion getServerVersion() {
        return this.getSession().getSessionState().getServerVersion();
    }

    // TODO: find a better place for method?
    public void rejectConnection(String message) {
        try {
            this.connection.close();
        } catch (SQLException e) {
            throw ExceptionFactory.createException(e.getMessage(), e, getExceptionInterceptor());
        }
        forceClose();
        throw ExceptionFactory.createException(UnableToConnectException.class, message, getExceptionInterceptor());
    }

    @Override
    public void rejectProtocol(Buffer buf) {
        try {
            this.mysqlSocket.close();
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

                return new BufferRow(rowPacket, fields, false, getExceptionInterceptor(), new MysqlTextValueDecoder());

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

            return new BufferRow(rowPacket, fields, true, getExceptionInterceptor(), new MysqlBinaryValueDecoder());
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
            if (packetLength == ProtocolConstants.MAX_PACKET_SIZE) {
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
                        Buffer errorPacket = new Buffer(packetLength + ProtocolConstants.HEADER_LENGTH);
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
                        this.warningCount = (this.mysqlInput.read() & 0xff) | ((this.mysqlInput.read() & 0xff) << 8);
                        remaining -= 2;

                        if (this.warningCount > 0) {
                            this.hadWarnings = true; // this is a 'latch', it's reset by sendCommand()
                        }

                        this.getSession().getSessionState().setServerStatus((this.mysqlInput.read() & 0xff) | ((this.mysqlInput.read() & 0xff) << 8), true);
                        checkTransactionState();

                        remaining -= 2;

                        if (remaining > 0) {
                            skipFully(this.mysqlInput, remaining);
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
                        throw SQLError.createCommunicationsException(this.connection, this.packetSentTimeHolder.getLastPacketSentTime(),
                                this.lastPacketReceivedTimeMs, new IOException(Messages.getString("MysqlIO.43")), getExceptionInterceptor());
                    }

                    remaining -= bytesRead;
                }
            }

            if (remaining > 0) {
                skipFully(this.mysqlInput, remaining);
            }

            if (isBinaryEncoded) {
                return new ByteArrayRow(rowData, getExceptionInterceptor(), new MysqlBinaryValueDecoder());
            }
            return new ByteArrayRow(rowData, getExceptionInterceptor());

        } catch (IOException ioEx) {
            throw SQLError.createCommunicationsException(this.connection, this.packetSentTimeHolder.getLastPacketSentTime(), this.lastPacketReceivedTimeMs,
                    ioEx, getExceptionInterceptor());
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
                if (!this.mysqlSocket.isClosed()) {
                    try {
                        this.mysqlSocket.shutdownInput();
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
        this.sharedSendPacket.setPosition(0);

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

    boolean tackOnMoreStreamingResults(ResultSetImpl addingTo, boolean isBinaryEncoded) throws SQLException {
        if (this.getSession().getSessionState().hasMoreResults()) {

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
                        owningStatement.getResultSetConcurrency(), true, owningStatement.getConnection().getCatalog(), fieldPacket, isBinaryEncoded, -1L, null);

                currentResultSet.setNextResultSet(newResultSet);

                currentResultSet = newResultSet;

                moreRowSetsExist = this.getSession().getSessionState().hasMoreResults();

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

        boolean checkForMoreResults = this.getSession().getSessionState().useMultiResults();

        boolean serverHasMoreResults = this.getSession().getSessionState().hasMoreResults();

        //
        // TODO: We need to support streaming of multiple result sets
        //
        if (serverHasMoreResults && streamResults) {
            //clearInputStream();
            //
            //throw SQLError.createSQLException(Messages.getString("MysqlIO.23"), 
            //SQLError.SQL_STATE_DRIVER_NOT_CAPABLE);
            if (topLevelResultSet.getUpdateCount() != -1) {
                tackOnMoreStreamingResults(topLevelResultSet, isBinaryEncoded);
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

            moreRowSetsExist = this.getSession().getSessionState().hasMoreResults();
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

    public final Buffer sendCommand(int command, String extraData, Buffer queryPacket, boolean skipCheck, String extraDataCharEncoding, int timeoutMillis)
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
                oldTimeout = this.mysqlSocket.getSoTimeout();
                this.mysqlSocket.setSoTimeout(timeoutMillis);
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
                    this.mysqlSocket.setSoTimeout(oldTimeout);
                } catch (SocketException e) {
                    throw SQLError.createCommunicationsException(this.connection, this.packetSentTimeHolder.getLastPacketSentTime(),
                            this.lastPacketReceivedTimeMs, e, getExceptionInterceptor());
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
     * @throws SQLException
     */
    final ResultSetInternalMethods sqlQueryDirect(StatementImpl callingStatement, String query, String characterEncoding, Buffer queryPacket, int maxRows,
            int resultSetType, int resultSetConcurrency, boolean streamResults, String catalog, Field[] cachedMetadata) throws SQLException {
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
    @Override
    public String getHost() {
        return this.host;
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
    boolean versionMeetsMinimum(int major, int minor, int subminor) {
        return this.getServerVersion().meetsMinimum(new ServerVersion(major, minor, subminor));
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
            String charEncoding = this.connection.getCharacterEncoding();
            String fileName = null;

            if (this.platformDbCharsetMatches) {
                try {
                    fileName = ((charEncoding != null) ? resultPacket.readString(charEncoding, getExceptionInterceptor()) : resultPacket.readString());
                } catch (CJException e) {
                    throw SQLExceptionsMapping.translateException(e, getExceptionInterceptor());
                }
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
            updateCount = resultPacket.readLength();
            updateID = resultPacket.readLength();

            // oldStatus set in sendCommand()
            this.getSession().getSessionState().setServerStatus(resultPacket.readInt());

            checkTransactionState();

            this.warningCount = resultPacket.readInt();

            if (this.warningCount > 0) {
                this.hadWarnings = true; // this is a 'latch', it's reset by sendCommand()
            }

            resultPacket.readByte(); // advance pointer

            setServerSlowQueryFlags();

            if (this.connection.isReadInfoMsgEnabled()) {
                info = resultPacket.readString(this.connection.getErrorMessageEncoding(), getExceptionInterceptor());
            }
        } catch (SQLException | CJException ex) {
            SQLException sqlEx = SQLError.createSQLException(SQLError.get(SQLError.SQL_STATE_GENERAL_ERROR), SQLError.SQL_STATE_GENERAL_ERROR, -1, ex,
                    getExceptionInterceptor());
            throw sqlEx;
        }

        ResultSetInternalMethods updateRs = com.mysql.jdbc.ResultSetImpl.getInstance(updateCount, updateID, this.connection, callingStatement);

        if (info != null) {
            ((com.mysql.jdbc.ResultSetImpl) updateRs).setServerInfo(info);
        }

        return (com.mysql.jdbc.ResultSetImpl) updateRs;
    }

    private void setServerSlowQueryFlags() {
        SessionState state = this.getSession().getSessionState();
        this.queryBadIndexUsed = state.noGoodIndexUsed();
        this.queryNoIndexUsed = state.noIndexUsed();
        this.serverQueryWasSlow = state.queryWasSlow();
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

    private final void readServerStatusForResultSets(Buffer rowPacket) throws SQLException {
        rowPacket.readByte(); // skips the 'last packet' flag

        this.warningCount = rowPacket.readInt();

        if (this.warningCount > 0) {
            this.hadWarnings = true; // this is a 'latch', it's reset by sendCommand()
        }

        this.getSession().getSessionState().setServerStatus(rowPacket.readInt(), true);
        checkTransactionState();

        setServerSlowQueryFlags();
    }

    private SocketFactory createSocketFactory() throws SQLException {
        try {
            if (this.socketFactoryClassName == null) {
                throw SQLError.createSQLException(Messages.getString("MysqlIO.75"), SQLError.SQL_STATE_UNABLE_TO_CONNECT_TO_DATASOURCE,
                        getExceptionInterceptor());
            }

            return (SocketFactory) (Class.forName(this.socketFactoryClassName).newInstance());
        } catch (InstantiationException | IllegalAccessException | ClassNotFoundException | CJException ex) {
            SQLException sqlEx = SQLError.createSQLException(Messages.getString("MysqlIO.76") + this.socketFactoryClassName + Messages.getString("MysqlIO.77"),
                    SQLError.SQL_STATE_UNABLE_TO_CONNECT_TO_DATASOURCE, ex, getExceptionInterceptor());
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
     * 
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
            final int lengthRead = readFully(this.mysqlInput, this.packetHeaderBuf, 0, 4);
            if (lengthRead < 4) {
                forceClose();
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

            int bytesRead = readFully(this.mysqlInput, byteBuf, 0, packetLength);

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

    void enableMultiQueries() throws SQLException {
        Buffer buf = getSharedSendPacket();

        buf.writeByte((byte) MysqlDefs.COM_SET_OPTION);
        buf.writeInt(0);
        sendCommand(MysqlDefs.COM_SET_OPTION, null, buf, false, null, 0);
    }

    void disableMultiQueries() throws SQLException {
        Buffer buf = getSharedSendPacket();

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
            throw ExceptionFactory.createCommunicationException(this.connection.getPropertySet(), this.connection.getSession(),
                    this.packetSentTimeHolder.getLastPacketSentTime(), this.lastPacketReceivedTimeMs, ioEx, getExceptionInterceptor());
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

        Buffer filePacket = (this.loadFileBufRef == null) ? null : this.loadFileBufRef.get();

        int bigPacketLength = Math.min(this.connection.getMaxAllowedPacket() - (ProtocolConstants.HEADER_LENGTH * 3),
                alignPacketSize(this.connection.getMaxAllowedPacket() - 16, 4096) - (ProtocolConstants.HEADER_LENGTH * 3));

        int oneMeg = 1024 * 1024;

        int smallerPacketSizeAligned = Math.min(oneMeg - (ProtocolConstants.HEADER_LENGTH * 3), alignPacketSize(oneMeg - 16, 4096)
                - (ProtocolConstants.HEADER_LENGTH * 3));

        int packetLength = Math.min(smallerPacketSizeAligned, bigPacketLength);

        if (filePacket == null) {
            try {
                filePacket = new Buffer(packetLength);
                filePacket.setPosition(0);
                this.loadFileBufRef = new SoftReference<Buffer>(filePacket);
            } catch (OutOfMemoryError oom) {
                throw SQLError.createSQLException(Messages.getString("MysqlIO.111", new Object[] { packetLength }), SQLError.SQL_STATE_MEMORY_ALLOCATION_ERROR,
                        getExceptionInterceptor());

            }
        }

        filePacket.setPosition(0);
        // account for the packet file-read-request from read()
        this.packetSequence++;

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
                filePacket.setPosition(0);
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
                filePacket.setPosition(0);
                send(filePacket, filePacket.getPosition());
                checkErrorPacket(); // to clear response off of queue
            }
        }

        // send empty packet to mark EOF
        filePacket.setPosition(0);
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

    private void checkErrorPacket(Buffer resultPacket) throws SQLException {

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

    private void reclaimLargeSharedSendPacket() {
        if ((this.sharedSendPacket != null) && (this.sharedSendPacket.getCapacity() > 1048576)) {
            this.sharedSendPacket = new Buffer(INITIAL_PACKET_SIZE);
        }
    }

    boolean hadWarnings() {
        return this.hadWarnings;
    }

    void scanForAndThrowDataTruncation() throws SQLException {
        if ((this.streamingData == null) && this.connection.getJdbcCompliantTruncation() && this.warningCount > 0) {
            SQLError.convertShowWarningsToSQLWarnings(this.connection, this.warningCount, true);
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

        for (int i = 0; i < numFields; i++) {
            if ((binaryData.readByte(nullMaskPos) & bit) != 0) {
                unpackedRowData[i] = null;
            } else {
                extractNativeEncodedColumn(binaryData, fields, i, unpackedRowData);
            }

            if (((bit <<= 1) & 255) == 0) {
                bit = 1; /* To next byte */

                nullMaskPos++;
            }
        }

        return new ByteArrayRow(unpackedRowData, getExceptionInterceptor(), new MysqlBinaryValueDecoder());
    }

    /**
     * Copy the raw result bytes from the
     * 
     * @param binaryData
     *            packet to the
     * @param unpackedRowData
     *            byte array.
     */
    private final void extractNativeEncodedColumn(Buffer binaryData, Field[] fields, int columnIndex, byte[][] unpackedRowData) throws SQLException {
        int type = fields[columnIndex].getMysqlType();

        int len = ProtocolUtils.getBinaryEncodedLength(type);

        if (type == MysqlDefs.FIELD_TYPE_NULL) {
            // Do nothing
        } else if (len == 0) {
            unpackedRowData[columnIndex] = binaryData.readLenByteArray(0);
        } else if (len > 0) {
            unpackedRowData[columnIndex] = binaryData.getBytes(len);
        } else {
            throw SQLError.createSQLException(
                    Messages.getString("MysqlIO.97") + type + Messages.getString("MysqlIO.98") + columnIndex + Messages.getString("MysqlIO.99") + fields.length
                            + Messages.getString("MysqlIO.100"), SQLError.SQL_STATE_GENERAL_ERROR, getExceptionInterceptor());
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
    public void negotiateSSLConnection(String user, String password, String database, int packLength) {
        if (!ExportControlled.enabled()) {
            throw new CJConnectionFeatureNotAvailableException(this.connection.getPropertySet(), this.connection.getSession(),
                    this.packetSentTimeHolder.getLastPacketSentTime(), null);
        }

        long clientParam = getSession().getSessionState().getClientParam();
        clientParam |= SessionState.CLIENT_SSL;
        getSession().getSessionState().setClientParam(clientParam);

        Buffer packet = new Buffer(packLength);
        packet.setPosition(0);

        packet.writeLong(clientParam);
        packet.writeLong(ProtocolConstants.MAX_PACKET_SIZE);
        getSession().getAuthenticationFactory().appendCharsetByteForHandshake(packet, getSession().getAuthenticationFactory().getEncodingForHandshake());
        packet.writeBytesNoNull(new byte[23]);  // Set of bytes reserved for future use.

        send(packet, packet.getPosition());

        try {
            ExportControlled.transformSocketToSSLSocket(this);
        } catch (FeatureNotAvailableException nae) {
            throw new CJConnectionFeatureNotAvailableException(this.connection.getPropertySet(), this.connection.getSession(),
                    this.packetSentTimeHolder.getLastPacketSentTime(), nae);
        } catch (IOException ioEx) {
            throw ExceptionFactory.createCommunicationException(this.getConnection().getPropertySet(), this.getConnection().getSession(),
                    this.getLastPacketSentTimeMs(), this.getLastPacketReceivedTimeMs(), ioEx, getExceptionInterceptor());
        }
        // output stream is replaced, build new packet sender
        this.packetSender = new SimplePacketSender(this.mysqlOutput);
    }

    @Override
    public boolean isSSLEstablished() {
        return ExportControlled.enabled() && ExportControlled.isSSLEstablished(this);
    }

    protected List<ResultSetRow> fetchRowsViaCursor(List<ResultSetRow> fetchedRows, long statementId, Field[] columnTypes, int fetchSize,
            boolean useBufferRowExplicit) throws SQLException {

        if (fetchedRows == null) {
            fetchedRows = new ArrayList<ResultSetRow>(fetchSize);
        } else {
            fetchedRows.clear();
        }

        this.sharedSendPacket.setPosition(0);

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

    @Override
    public void setThreadId(long threadId) {
        this.threadId = threadId;
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

    private void checkTransactionState() throws SQLException {
        int transState = this.getSession().getSessionState().getTransactionState();
        if (transState == SessionState.TRANSACTION_COMPLETED) {
            this.connection.transactionCompleted();
        } else if (transState == SessionState.TRANSACTION_STARTED) {
            this.connection.transactionBegun();
        }
    }

    protected void setStatementInterceptors(List<StatementInterceptorV2> statementInterceptors) {
        this.statementInterceptors = statementInterceptors.isEmpty() ? null : statementInterceptors;
    }

    protected void setSocketTimeout(int milliseconds) throws SQLException {
        try {
            this.mysqlSocket.setSoTimeout(milliseconds);
        } catch (SocketException e) {
            SQLException sqlEx = SQLError.createSQLException(Messages.getString("MysqlIO.112"), SQLError.SQL_STATE_ILLEGAL_ARGUMENT, e,
                    getExceptionInterceptor());
            throw sqlEx;
        }
    }

    protected void releaseResources() {
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

        PropertySet pset = this.connection.getPropertySet();

        //
        // Can't enable compression until after handshake
        //
        if (((getSession().getSessionState().getServerCapabilities() & SessionState.CLIENT_COMPRESS) != 0)
                && pset.getBooleanReadableProperty(PropertyDefinitions.PNAME_useCompression).getValue() && !(this.mysqlInput instanceof CompressedInputStream)) {
            this.useCompression = true;
            this.mysqlInput = new CompressedInputStream(this.connection, this.mysqlInput,
                    pset.getBooleanReadableProperty(PropertyDefinitions.PNAME_traceProtocol));
            this.compressedPacketSender = new CompressedPacketSender(this.mysqlOutput);
            this.packetSender = this.compressedPacketSender;
        }

        decoratePacketSender();

        try {
            this.mysqlSocket = this.socketFactory.afterHandshake();
        } catch (IOException ioEx) {
            throw ExceptionFactory.createCommunicationException(this.connection.getPropertySet(), this.connection.getSession(),
                    this.packetSentTimeHolder.getLastPacketSentTime(), this.lastPacketReceivedTimeMs, ioEx, getExceptionInterceptor());
        }
    }

    @Override
    public void changeDatabase(String database) {
        if (database == null || database.length() == 0) {
            return;
        }

        try {
            sendCommand(MysqlDefs.INIT_DB, database, null, false, null, 0);
        } catch (SQLException | CJException ex) {
            if (this.connection.getPropertySet().getBooleanReadableProperty(PropertyDefinitions.PNAME_createDatabaseIfNotExist).getValue()) {
                try {
                    sendCommand(MysqlDefs.QUERY, "CREATE DATABASE IF NOT EXISTS " + database, null, false, null, 0);
                    sendCommand(MysqlDefs.INIT_DB, database, null, false, null, 0);
                } catch (SQLException e) {
                    throw ExceptionFactory.createException(e.getMessage(), e, getExceptionInterceptor());
                }
            } else {
                throw ExceptionFactory.createCommunicationException(this.connection.getPropertySet(), this.connection.getSession(),
                        this.packetSentTimeHolder.getLastPacketSentTime(), this.lastPacketReceivedTimeMs, ex, getExceptionInterceptor());
            }
        }
    }

}
