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
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.ref.SoftReference;
import java.net.MalformedURLException;
import java.net.URL;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import com.mysql.cj.core.Constants;
import com.mysql.cj.core.Messages;
import com.mysql.cj.core.conf.PropertyDefinitions;
import com.mysql.cj.core.exception.CJException;
import com.mysql.cj.core.io.Buffer;
import com.mysql.cj.core.io.MysqlBinaryValueDecoder;
import com.mysql.cj.core.io.MysqlTextValueDecoder;
import com.mysql.cj.core.io.ProtocolConstants;
import com.mysql.cj.core.util.ProtocolUtils;
import com.mysql.cj.core.util.Util;
import com.mysql.cj.mysqla.io.MysqlaProtocol;
import com.mysql.jdbc.exceptions.SQLError;
import com.mysql.jdbc.exceptions.SQLExceptionsMapping;

/**
 * This class is used by Connection for communicating with the MySQL server.
 */
public class MysqlIO extends MysqlaProtocol {
    protected static final int NULL_LENGTH = ~0;
    protected static final int MIN_COMPRESS_LEN = 50;
    private static int maxBufferSize = 65535;

    protected static final int MAX_QUERY_SIZE_TO_LOG = 1024; // truncate logging of queries at 1K

    /**
     * We need to have a 'marker' for all-zero datetimes so that ResultSet can decide what to do based on connection setting
     */
    protected final static String ZERO_DATE_VALUE_MARKER = "0000-00-00";
    protected final static String ZERO_DATETIME_VALUE_MARKER = "0000-00-00 00:00:00";

    //
    // Packet used for 'LOAD DATA LOCAL INFILE'
    //
    // We use a SoftReference, so that we don't penalize intermittent use of this feature
    //
    private SoftReference<Buffer> loadFileBufRef;

    /**
     * Used in ProtocolFactory
     */
    public MysqlIO() {
        this.resultsHandler = this;
    }

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
    //public void init(String host, int port, Properties props, String socketFactoryClassName, MysqlJdbcConnection conn, int socketTimeout,
    //        int useBufferRowSizeThreshold) throws IOException, SQLException {

    //}

    protected boolean isDataAvailable() throws SQLException {
        try {
            return this.physicalConnection.getMysqlInput().available() > 0;
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
            usingCursor = this.sessionState.cursorExists();

            if (usingCursor) {
                RowData rows = new RowDataCursor(this.sessionState, this, prepStmt, fields);

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

        if (this.sessionState.hasLongColumnInfo()) {
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
            boolean autoCommitModeOnServer = this.sessionState.isAutocommit();

            if (!autoCommitFlag) {
                // Just to be safe, check if a transaction is in progress on the server....
                // if so, then we must be in autoCommit == false
                // therefore return the opposite of transaction status
                boolean inTransactionOnServer = this.sessionState.inTransactionOnServer();

                return !inTransactionOnServer;
            }

            return autoCommitModeOnServer != autoCommitFlag;
        }

        return true;
    }

    protected void resetReadPacketSequence() {
        this.readPacketSequence = 0;
    }

    protected void dumpPacketRingBuffer() {
        if ((this.packetDebugRingBuffer != null)
                && this.connection.getPropertySet().getBooleanReadableProperty(PropertyDefinitions.PNAME_enablePacketDebug).getValue()) {
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

    static int getMaxBuf() {
        return maxBufferSize;
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
            int lengthRead = readFully(this.physicalConnection.getMysqlInput(), this.packetHeaderBuf, 0, 4);

            if (lengthRead < 4) {
                this.physicalConnection.forceClose();
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

                int sw = this.physicalConnection.getMysqlInput().read() & 0xff;
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
                        readFully(this.physicalConnection.getMysqlInput(), errorPacket.getByteBuffer(), 5, packetLength - 1);
                        errorPacket.setPosition(4);
                        checkErrorPacket(errorPacket);
                    }

                    if (sw == 254 && packetLength < 9) {
                        this.warningCount = (this.physicalConnection.getMysqlInput().read() & 0xff)
                                | ((this.physicalConnection.getMysqlInput().read() & 0xff) << 8);
                        remaining -= 2;

                        if (this.warningCount > 0) {
                            this.hadWarnings = true; // this is a 'latch', it's reset by sendCommand()
                        }

                        this.sessionState.setServerStatus((this.physicalConnection.getMysqlInput().read() & 0xff)
                                | ((this.physicalConnection.getMysqlInput().read() & 0xff) << 8), true);
                        checkTransactionState();

                        remaining -= 2;

                        if (remaining > 0) {
                            skipFully(this.physicalConnection.getMysqlInput(), remaining);
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
                        len = (this.physicalConnection.getMysqlInput().read() & 0xff) | ((this.physicalConnection.getMysqlInput().read() & 0xff) << 8);
                        remaining -= 2;
                        break;

                    case 253:
                        len = (this.physicalConnection.getMysqlInput().read() & 0xff) | ((this.physicalConnection.getMysqlInput().read() & 0xff) << 8)
                                | ((this.physicalConnection.getMysqlInput().read() & 0xff) << 16);

                        remaining -= 3;
                        break;

                    case 254:
                        len = (int) ((this.physicalConnection.getMysqlInput().read() & 0xff)
                                | ((long) (this.physicalConnection.getMysqlInput().read() & 0xff) << 8)
                                | ((long) (this.physicalConnection.getMysqlInput().read() & 0xff) << 16)
                                | ((long) (this.physicalConnection.getMysqlInput().read() & 0xff) << 24)
                                | ((long) (this.physicalConnection.getMysqlInput().read() & 0xff) << 32)
                                | ((long) (this.physicalConnection.getMysqlInput().read() & 0xff) << 40)
                                | ((long) (this.physicalConnection.getMysqlInput().read() & 0xff) << 48) | ((long) (this.physicalConnection.getMysqlInput()
                                .read() & 0xff) << 56));
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

                    int bytesRead = readFully(this.physicalConnection.getMysqlInput(), rowData[i], 0, len);

                    if (bytesRead != len) {
                        throw SQLError.createCommunicationsException(this.connection, this.packetSentTimeHolder.getLastPacketSentTime(),
                                this.lastPacketReceivedTimeMs, new IOException(Messages.getString("MysqlIO.43")), getExceptionInterceptor());
                    }

                    remaining -= bytesRead;
                }
            }

            if (remaining > 0) {
                skipFully(this.physicalConnection.getMysqlInput(), remaining);
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
        if (this.sessionState.hasMoreResults()) {

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

                moreRowSetsExist = this.sessionState.hasMoreResults();

                if (!currentResultSet.reallyResult() && !moreRowSetsExist) {
                    // special case, we can stop "streaming"
                    return false;
                }
            }

            return true;
        }

        return false;
    }

    public ResultSetImpl readAllResults(StatementImpl callingStatement, int maxRows, int resultSetType, int resultSetConcurrency, boolean streamResults,
            String catalog, Buffer resultPacket, boolean isBinaryEncoded, long preSentColumnCount, Field[] metadataFromCache) throws SQLException {
        resultPacket.setPosition(resultPacket.getPosition() - 1);

        ResultSetImpl topLevelResultSet = readResultsForQueryOrUpdate(callingStatement, maxRows, resultSetType, resultSetConcurrency, streamResults, catalog,
                resultPacket, isBinaryEncoded, preSentColumnCount, metadataFromCache);

        ResultSetImpl currentResultSet = topLevelResultSet;

        boolean checkForMoreResults = this.sessionState.useMultiResults();

        boolean serverHasMoreResults = this.sessionState.hasMoreResults();

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

            moreRowSetsExist = this.sessionState.hasMoreResults();
        }

        if (!streamResults) {
            clearInputStream();
        }

        reclaimLargeReusablePacket();

        return topLevelResultSet;
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
                rs = com.mysql.jdbc.ResultSetImpl.getInstance(catalog, fields, rows, this.connection, callingStatement);

                break;

            case java.sql.ResultSet.CONCUR_UPDATABLE:
                rs = new UpdatableResultSet(catalog, fields, rows, this.connection, callingStatement, this.sessionState.hasLongColumnInfo());

                break;

            default:
                return com.mysql.jdbc.ResultSetImpl.getInstance(catalog, fields, rows, this.connection, callingStatement);
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
            this.sessionState.setServerStatus(resultPacket.readInt());

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

    private final void readServerStatusForResultSets(Buffer rowPacket) throws SQLException {
        rowPacket.readByte(); // skips the 'last packet' flag

        this.warningCount = rowPacket.readInt();

        if (this.warningCount > 0) {
            this.hadWarnings = true; // this is a 'latch', it's reset by sendCommand()
        }

        this.sessionState.setServerStatus(rowPacket.readInt(), true);
        checkTransactionState();

        setServerSlowQueryFlags();
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

}
