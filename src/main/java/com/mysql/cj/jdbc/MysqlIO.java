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

package com.mysql.cj.jdbc;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.lang.ref.SoftReference;
import java.net.MalformedURLException;
import java.net.URL;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import com.mysql.cj.api.conf.PropertySet;
import com.mysql.cj.api.conf.ReadableProperty;
import com.mysql.cj.api.io.ResultsHandler;
import com.mysql.cj.api.jdbc.JdbcConnection;
import com.mysql.cj.api.jdbc.result.ResultSetInternalMethods;
import com.mysql.cj.api.mysqla.io.NativeProtocol.IntegerDataType;
import com.mysql.cj.api.mysqla.io.NativeProtocol.StringLengthDataType;
import com.mysql.cj.api.mysqla.io.NativeProtocol.StringSelfDataType;
import com.mysql.cj.api.mysqla.io.PacketHeader;
import com.mysql.cj.api.mysqla.io.PacketPayload;
import com.mysql.cj.api.mysqla.result.ResultsetRows;
import com.mysql.cj.api.mysqla.result.ResultsetRowsOwner;
import com.mysql.cj.api.result.Row;
import com.mysql.cj.core.Constants;
import com.mysql.cj.core.Messages;
import com.mysql.cj.core.MysqlType;
import com.mysql.cj.core.conf.PropertyDefinitions;
import com.mysql.cj.core.exceptions.CJException;
import com.mysql.cj.core.exceptions.ExceptionFactory;
import com.mysql.cj.core.io.FullReadInputStream;
import com.mysql.cj.core.io.MysqlBinaryValueDecoder;
import com.mysql.cj.core.io.MysqlTextValueDecoder;
import com.mysql.cj.core.result.Field;
import com.mysql.cj.core.util.LazyString;
import com.mysql.cj.core.util.Util;
import com.mysql.cj.jdbc.exceptions.SQLError;
import com.mysql.cj.jdbc.exceptions.SQLExceptionsMapping;
import com.mysql.cj.jdbc.result.ResultSetImpl;
import com.mysql.cj.jdbc.result.UpdatableResultSet;
import com.mysql.cj.mysqla.MysqlaConstants;
import com.mysql.cj.mysqla.MysqlaSession;
import com.mysql.cj.mysqla.MysqlaUtils;
import com.mysql.cj.mysqla.io.Buffer;
import com.mysql.cj.mysqla.io.MysqlaProtocol;
import com.mysql.cj.mysqla.result.BinaryBufferRow;
import com.mysql.cj.mysqla.result.ByteArrayRow;
import com.mysql.cj.mysqla.result.ResultsetRowsCursor;
import com.mysql.cj.mysqla.result.ResultsetRowsDynamic;
import com.mysql.cj.mysqla.result.ResultsetRowsStatic;
import com.mysql.cj.mysqla.result.TextBufferRow;

/**
 * This class is used by Connection for communicating with the MySQL server.
 */
public class MysqlIO implements ResultsHandler {
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
    private SoftReference<PacketPayload> loadFileBufRef;

    private MysqlaSession session;
    private MysqlaProtocol protocol;
    private PropertySet propertySet;
    private JdbcConnection connection;

    /** Data to the server */
    protected ResultsetRows streamingData = null;

    protected ReadableProperty<Integer> useBufferRowSizeThreshold;
    protected ReadableProperty<Boolean> useDirectRowUnpack;

    public MysqlIO(MysqlaProtocol protocol, PropertySet propertySet, JdbcConnection connection) {
        this.session = connection.getSession();
        this.protocol = protocol;
        this.propertySet = propertySet;
        this.connection = connection;

        this.useBufferRowSizeThreshold = this.propertySet.getMemorySizeReadableProperty(PropertyDefinitions.PNAME_largeRowSizeThreshold);
        this.useDirectRowUnpack = this.propertySet.getBooleanReadableProperty(PropertyDefinitions.PNAME_useDirectRowUnpack);
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
     * @param metadataFromCache
     *            metadata to avoid reading/parsing metadata
     * 
     * @return a result set
     * 
     * @throws SQLException
     *             if a database access error occurs
     */
    protected ResultSetImpl getResultSet(StatementImpl callingStatement, long columnCount, int maxRows, int resultSetType, int resultSetConcurrency,
            boolean streamResults, String catalog, boolean isBinaryEncoded, Field[] metadataFromCache) throws SQLException {
        PacketPayload packet; // The packet from the server
        Field[] fields = null;

        // Read in the column information

        if (metadataFromCache == null /* we want the metadata from the server */) {
            fields = new Field[(int) columnCount];

            for (int i = 0; i < columnCount; i++) {
                PacketPayload fieldPacket = this.protocol.readPacket(null);

                fields[i] = unpackField(fieldPacket, this.connection.getCharacterSetMetadata());
            }
        } else {
            for (int i = 0; i < columnCount; i++) {
                this.protocol.skipPacket();
            }
        }

        // There is no EOF packet after fields when CLIENT_DEPRECATE_EOF is set
        if (!isEOFDeprecated() ||
                // if we asked to use cursor then there should be an OK packet here
                (this.protocol.versionMeetsMinimum(5, 0, 2) && callingStatement != null && isBinaryEncoded && callingStatement.isCursorRequired())) {

            packet = this.protocol.readPacket(this.protocol.getReusablePacket());
            readServerStatusForResultSets(packet);
        }

        //
        // Handle cursor-based fetch first
        //

        if (this.propertySet.getBooleanReadableProperty(PropertyDefinitions.PNAME_useCursorFetch).getValue() && isBinaryEncoded && callingStatement != null
                && callingStatement.getFetchSize() != 0 && callingStatement.getResultSetType() == ResultSet.TYPE_FORWARD_ONLY) {

            boolean usingCursor = true;

            //
            // Server versions 5.0.5 or newer will only open a cursor and set this flag if they can, otherwise they punt and go back to mysql_store_results()
            // behavior
            //
            usingCursor = this.protocol.getServerSession().cursorExists();

            if (usingCursor) {
                ResultsetRows rows = new ResultsetRowsCursor(this.protocol, fields);

                ResultSetImpl rs = buildResultSetWithRows(callingStatement, catalog, fields, rows, resultSetType, resultSetConcurrency);

                if (usingCursor) {
                    rs.setFetchSize(callingStatement.getFetchSize());
                }

                return rs;
            }
        }

        ResultsetRows rowData = null;

        if (!streamResults) {
            rowData = readSingleRowSet(columnCount, maxRows, resultSetConcurrency, isBinaryEncoded, (metadataFromCache == null) ? fields : metadataFromCache);
        } else {
            rowData = new ResultsetRowsDynamic(this.protocol, (int) columnCount, (metadataFromCache == null) ? fields : metadataFromCache, isBinaryEncoded);
            this.streamingData = rowData;
        }

        ResultSetImpl rs = buildResultSetWithRows(callingStatement, catalog, (metadataFromCache == null) ? fields : metadataFromCache, rowData, resultSetType,
                resultSetConcurrency);

        return rs;
    }

    /**
     * Unpacks the Field information from the given packet.
     * 
     * @param packet
     *            the packet containing the field information
     * @param characterSetMetadata
     *            encoding of the metadata in the packet
     * 
     * @return the unpacked field
     */
    protected Field unpackField(PacketPayload packet, String characterSetMetadata) {
        int offset, length;

        length = (int) packet.readInteger(IntegerDataType.INT_LENENC);
        packet.setPosition(packet.getPosition() + length); // skip catalog name

        length = (int) packet.readInteger(IntegerDataType.INT_LENENC);
        offset = packet.getPosition();
        LazyString databaseName = new LazyString(packet.getByteBuffer(), offset, length, characterSetMetadata);
        packet.setPosition(packet.getPosition() + length);

        length = (int) packet.readInteger(IntegerDataType.INT_LENENC);
        offset = packet.getPosition();
        LazyString tableName = new LazyString(packet.getByteBuffer(), offset, length, characterSetMetadata);
        packet.setPosition(packet.getPosition() + length);

        length = (int) packet.readInteger(IntegerDataType.INT_LENENC);
        offset = packet.getPosition();
        LazyString originalTableName = new LazyString(packet.getByteBuffer(), offset, length, characterSetMetadata);
        packet.setPosition(packet.getPosition() + length);

        length = (int) packet.readInteger(IntegerDataType.INT_LENENC);
        offset = packet.getPosition();
        LazyString columnName = new LazyString(packet.getByteBuffer(), offset, length, characterSetMetadata);
        packet.setPosition(packet.getPosition() + length);

        length = (int) packet.readInteger(IntegerDataType.INT_LENENC);
        offset = packet.getPosition();
        LazyString originalColumnName = new LazyString(packet.getByteBuffer(), offset, length, characterSetMetadata);
        packet.setPosition(packet.getPosition() + length);

        packet.readInteger(IntegerDataType.INT1);

        short collationIndex = (short) packet.readInteger(IntegerDataType.INT2);
        long colLength = packet.readInteger(IntegerDataType.INT4);
        int colType = (int) packet.readInteger(IntegerDataType.INT1);
        short colFlag = (short) packet.readInteger(this.protocol.getServerSession().hasLongColumnInfo() ? IntegerDataType.INT2 : IntegerDataType.INT1);
        int colDecimals = (int) packet.readInteger(IntegerDataType.INT1);

        String encoding = this.protocol.getServerSession().getEncodingForIndex(collationIndex);

        MysqlType mysqlType = MysqlaProtocol.findMysqlType(this.propertySet, colType, colFlag, colLength, tableName, originalTableName, collationIndex,
                encoding);

        return new Field(databaseName, tableName, originalTableName, columnName, originalColumnName, colLength, colType, colFlag, colDecimals, collationIndex,
                encoding, mysqlType);
    }

    protected boolean isSetNeededForAutoCommitMode(boolean autoCommitFlag) {
        if (this.propertySet.getBooleanReadableProperty(PropertyDefinitions.PNAME_elideSetAutoCommits).getValue()) {
            boolean autoCommitModeOnServer = this.protocol.getServerSession().isAutocommit();

            if (!autoCommitFlag) {
                // Just to be safe, check if a transaction is in progress on the server....
                // if so, then we must be in autoCommit == false
                // therefore return the opposite of transaction status
                boolean inTransactionOnServer = this.protocol.getServerSession().inTransactionOnServer();

                return !inTransactionOnServer;
            }

            return autoCommitModeOnServer != autoCommitFlag;
        }

        return true;
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
    public final Row nextRow(Field[] fields, int columnCount, boolean isBinaryEncoded, int resultSetConcurrency, boolean canReuseRowPacketForBufferRow)
            throws SQLException {

        // use a buffer row for reusable packets (streaming results) or blobs and long strings
        boolean useBufferRow = canReuseRowPacketForBufferRow || forceBufferRow(fields);

        // use nextRowFast() if necessary/possible, otherwise read the entire packet
        PacketPayload rowPacket = null;
        try {
            PacketHeader hdr = this.protocol.getPacketReader().readHeader();
            int packetLength = hdr.getPacketLength();

            // use a buffer row if we're over the threshold
            useBufferRow = useBufferRow || packetLength >= this.useBufferRowSizeThreshold.getValue();

            if (this.useDirectRowUnpack.getValue() && !isBinaryEncoded && !useBufferRow && packetLength < MysqlaConstants.MAX_PACKET_SIZE) {
                // we can do a direct row unpack (which creates a ByteArrayRow) if:
                // * we don't want a buffer row explicitly
                // * we have a TEXT-encoded result
                // * we don't have a multi-packet
                return nextRowFast(hdr, columnCount);
            }
            // else read the entire packet(s)
            rowPacket = this.protocol.getPacketReader().readPayload(Optional.ofNullable(this.protocol.getReusablePacket()), packetLength);
            this.protocol.checkErrorPacket(rowPacket);
            // Didn't read an error, so re-position to beginning of packet in order to read result set data
            rowPacket.setPosition(rowPacket.getPosition() - 1);

        } catch (IOException ioEx) {
            throw SQLError.createCommunicationsException(this.connection, this.protocol.getPacketSentTimeHolder().getLastPacketSentTime(),
                    this.protocol.getPacketReceivedTimeHolder().getLastPacketReceivedTime(), ioEx, this.protocol.getExceptionInterceptor());
        }

        // exit early with null if there's an EOF packet
        //if (rowPacket.isEOFPacket()) {
        if (!isEOFDeprecated() && rowPacket.isEOFPacket() || isEOFDeprecated() && rowPacket.isResultSetOKPacket()) {
            readServerStatusForResultSets(rowPacket);
            return null;
        }

        if (!isBinaryEncoded) {
            if (resultSetConcurrency == ResultSet.CONCUR_UPDATABLE || !useBufferRow) {
                byte[][] rowData = new byte[columnCount][];

                for (int i = 0; i < columnCount; i++) {
                    rowData[i] = rowPacket.readBytes(StringSelfDataType.STRING_LENENC);
                }

                return new ByteArrayRow(rowData, this.protocol.getExceptionInterceptor());
            }

            if (!canReuseRowPacketForBufferRow) {
                this.protocol.setReusablePacket(new Buffer(rowPacket.getPayloadLength()));
            }

            return new TextBufferRow(rowPacket, fields, this.protocol.getExceptionInterceptor(), new MysqlTextValueDecoder());
        }
        // Handle binary-encoded data for server-side PreparedStatements

        // bump past ProtocolBinary::ResultsetRow packet header
        rowPacket.setPosition(rowPacket.getPosition() + 1);

        if (resultSetConcurrency == ResultSet.CONCUR_UPDATABLE || !useBufferRow) {
            return unpackBinaryResultSetRow(fields, rowPacket, resultSetConcurrency);
        }

        if (!canReuseRowPacketForBufferRow) {
            this.protocol.setReusablePacket(new Buffer(rowPacket.getPayloadLength()));
        }

        return new BinaryBufferRow(rowPacket, fields, this.protocol.getExceptionInterceptor(), new MysqlBinaryValueDecoder());

    }

    /**
     * Use the 'fast' approach to reading a row. We read everything piece-meal into the byte buffers that are used to back the ByteArrayRow. This avoids reading
     * the entire packet at once.
     */
    private Row nextRowFast(PacketHeader header, int columnCount) throws SQLException, IOException {
        int packetLength = header.getPacketLength();
        int remaining = packetLength;

        boolean firstTime = true;

        byte[][] rowData = null;

        FullReadInputStream mysqlInput = this.protocol.getSocketConnection().getMysqlInput();
        for (int i = 0; i < columnCount; i++) {

            int sw = mysqlInput.read() & 0xff;
            remaining--;

            if (firstTime) {
                if (sw == PacketPayload.TYPE_ID_ERROR) {
                    // error packet - we assemble it whole for "fidelity" in case we ever need an entire packet in checkErrorPacket() but we could've gotten
                    // away with just writing the error code and message in it (for now).
                    PacketPayload errorPacket = new Buffer(packetLength);
                    errorPacket.writeInteger(IntegerDataType.INT1, sw);
                    mysqlInput.readFully(errorPacket.getByteBuffer(), 1, packetLength - 1);
                    this.protocol.checkErrorPacket(errorPacket);
                }

                if (sw == PacketPayload.TYPE_ID_EOF && packetLength < 16777215) {
                    // Both EOF and OK packets have the same 0xfe signature in result sets.

                    // OK packet length limit restricted to MAX_PACKET_LENGTH value (256L*256L*256L-1) as any length greater
                    // than this value will have first byte of OK packet to be 254 thus does not provide a means to identify
                    // if this is OK or EOF packet.
                    // Thus we need to check the packet length to distinguish between OK packet and ResultsetRow packet starting with 0xfe
                    if (isEOFDeprecated()) {
                        // read OK packet
                        remaining -= mysqlInput.skipLengthEncodedInteger(); // affected_rows
                        remaining -= mysqlInput.skipLengthEncodedInteger(); // last_insert_id

                        this.protocol.getServerSession().setStatusFlags((this.protocol.getSocketConnection().getMysqlInput().read() & 0xff)
                                | ((this.protocol.getSocketConnection().getMysqlInput().read() & 0xff) << 8), true);
                        this.protocol.checkTransactionState();
                        remaining -= 2;

                        this.protocol.setWarningCount((mysqlInput.read() & 0xff) | ((mysqlInput.read() & 0xff) << 8));
                        remaining -= 2;

                        if (this.protocol.getWarningCount() > 0) {
                            this.protocol.setHadWarnings(true); // this is a 'latch', it's reset by sendCommand()
                        }

                    } else {
                        // read EOF packet
                        this.protocol.setWarningCount((mysqlInput.read() & 0xff) | ((mysqlInput.read() & 0xff) << 8));
                        remaining -= 2;

                        if (this.protocol.getWarningCount() > 0) {
                            this.protocol.setHadWarnings(true); // this is a 'latch', it's reset by sendCommand()
                        }

                        this.protocol.getServerSession().setStatusFlags((this.protocol.getSocketConnection().getMysqlInput().read() & 0xff)
                                | ((this.protocol.getSocketConnection().getMysqlInput().read() & 0xff) << 8), true);
                        this.protocol.checkTransactionState();

                        remaining -= 2;
                    }

                    this.protocol.setServerSlowQueryFlags();

                    if (remaining > 0) {
                        mysqlInput.skipFully(remaining);
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
                    len = (mysqlInput.read() & 0xff) | ((mysqlInput.read() & 0xff) << 8);
                    remaining -= 2;
                    break;

                case 253:
                    len = (mysqlInput.read() & 0xff) | ((mysqlInput.read() & 0xff) << 8) | ((mysqlInput.read() & 0xff) << 16);

                    remaining -= 3;
                    break;

                case 254:
                    len = (int) ((mysqlInput.read() & 0xff) | ((long) (mysqlInput.read() & 0xff) << 8) | ((long) (mysqlInput.read() & 0xff) << 16)
                            | ((long) (mysqlInput.read() & 0xff) << 24) | ((long) (mysqlInput.read() & 0xff) << 32) | ((long) (mysqlInput.read() & 0xff) << 40)
                            | ((long) (mysqlInput.read() & 0xff) << 48) | ((long) (mysqlInput.read() & 0xff) << 56));
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

                int bytesRead = mysqlInput.readFully(rowData[i], 0, len);

                if (bytesRead != len) {
                    throw SQLError.createCommunicationsException(this.connection, this.protocol.getPacketSentTimeHolder().getLastPacketSentTime(),
                            this.protocol.getPacketReceivedTimeHolder().getLastPacketReceivedTime(), new IOException(Messages.getString("MysqlIO.43")),
                            this.protocol.getExceptionInterceptor());
                }

                remaining -= bytesRead;
            }
        }

        if (remaining > 0) {
            throw new IOException("Unable to read entire packet. Found '" + remaining + "' bytes remaining.");
        }

        return new ByteArrayRow(rowData, this.protocol.getExceptionInterceptor());
    }

    public boolean tackOnMoreStreamingResults(ResultsetRowsOwner addingTo, boolean isBinaryEncoded) throws SQLException {
        if (this.protocol.getServerSession().hasMoreResults()) {

            boolean moreRowSetsExist = true;
            ResultSetImpl currentResultSet = (ResultSetImpl) addingTo;
            boolean firstTime = true;

            while (moreRowSetsExist) {
                if (!firstTime && currentResultSet.hasRows()) {
                    break;
                }

                firstTime = false;

                PacketPayload fieldPacket = this.protocol.checkErrorPacket();
                fieldPacket.setPosition(0);

                java.sql.Statement owningStatement = ((ResultSetImpl) addingTo).getStatement();

                int maxRows = addingTo.getOwningStatementMaxRows();

                // fixme for catalog, isBinary

                ResultSetImpl newResultSet = readResultsForQueryOrUpdate((StatementImpl) owningStatement, maxRows, owningStatement.getResultSetType(),
                        owningStatement.getResultSetConcurrency(), true, owningStatement.getConnection().getCatalog(), fieldPacket, isBinaryEncoded, -1L, null);

                currentResultSet.setNextResultSet(newResultSet);

                currentResultSet = newResultSet;

                moreRowSetsExist = this.protocol.getServerSession().hasMoreResults();

                if (!currentResultSet.hasRows() && !moreRowSetsExist) {
                    // special case, we can stop "streaming"
                    return false;
                }
            }

            return true;
        }

        return false;
    }

    public ResultSetImpl readAllResults(StatementImpl callingStatement, int maxRows, int resultSetType, int resultSetConcurrency, boolean streamResults,
            String catalog, PacketPayload resultPacket, boolean isBinaryEncoded, long preSentColumnCount, Field[] metadataFromCache) throws SQLException {
        resultPacket.setPosition(resultPacket.getPosition() - 1);

        ResultSetImpl topLevelResultSet = readResultsForQueryOrUpdate(callingStatement, maxRows, resultSetType, resultSetConcurrency, streamResults, catalog,
                resultPacket, isBinaryEncoded, preSentColumnCount, metadataFromCache);

        ResultSetImpl currentResultSet = topLevelResultSet;

        boolean checkForMoreResults = this.protocol.getServerSession().useMultiResults();

        boolean serverHasMoreResults = this.protocol.getServerSession().hasMoreResults();

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

            this.protocol.reclaimLargeReusablePacket();

            return topLevelResultSet;
        }

        boolean moreRowSetsExist = checkForMoreResults & serverHasMoreResults;

        while (moreRowSetsExist) {
            PacketPayload fieldPacket = this.protocol.checkErrorPacket();
            fieldPacket.setPosition(0);

            ResultSetImpl newResultSet = readResultsForQueryOrUpdate(callingStatement, maxRows, resultSetType, resultSetConcurrency, streamResults, catalog,
                    fieldPacket, isBinaryEncoded, preSentColumnCount, metadataFromCache);

            currentResultSet.setNextResultSet(newResultSet);

            currentResultSet = newResultSet;

            moreRowSetsExist = this.protocol.getServerSession().hasMoreResults();
        }

        if (!streamResults) {
            this.protocol.clearInputStream();
        }

        this.protocol.reclaimLargeReusablePacket();

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
     * @param metadataFromCache
     *            metadata to avoid reading/parsing metadata
     * 
     * @return a result set that either represents the rows, or an update count
     * 
     * @throws SQLException
     *             if an error occurs while reading the rows
     */
    protected final ResultSetImpl readResultsForQueryOrUpdate(StatementImpl callingStatement, int maxRows, int resultSetType, int resultSetConcurrency,
            boolean streamResults, String catalog, PacketPayload resultPacket, boolean isBinaryEncoded, long preSentColumnCount, Field[] metadataFromCache)
                    throws SQLException {
        long columnCount = resultPacket.readInteger(IntegerDataType.INT_LENENC);

        if (columnCount == 0) {
            return buildResultSetWithUpdates(callingStatement, resultPacket);
        } else if (columnCount == PacketPayload.NULL_LENGTH) {
            String charEncoding = this.propertySet.getStringReadableProperty(PropertyDefinitions.PNAME_characterEncoding).getValue();
            String fileName = null;

            if (this.protocol.doesPlatformDbCharsetMatches()) {
                try {
                    fileName = resultPacket.readString(StringSelfDataType.STRING_TERM, charEncoding);
                } catch (CJException e) {
                    throw SQLExceptionsMapping.translateException(e, this.protocol.getExceptionInterceptor());
                }
            } else {
                fileName = resultPacket.readString(StringSelfDataType.STRING_TERM, null);
            }

            return sendFileToServer(callingStatement, fileName);
        } else {
            com.mysql.cj.jdbc.result.ResultSetImpl results = getResultSet(callingStatement, columnCount, maxRows, resultSetType, resultSetConcurrency,
                    streamResults, catalog, isBinaryEncoded, metadataFromCache);

            return results;
        }
    }

    private int alignPacketSize(int a, int l) {
        return ((((a) + (l)) - 1) & ~((l) - 1));
    }

    private ResultSetImpl buildResultSetWithRows(StatementImpl callingStatement, String catalog, Field[] fields, ResultsetRows rows, int resultSetType,
            int resultSetConcurrency) throws SQLException {
        ResultSetImpl rs = null;

        switch (resultSetConcurrency) {
            case java.sql.ResultSet.CONCUR_READ_ONLY:
                rs = com.mysql.cj.jdbc.result.ResultSetImpl.getInstance(catalog, fields, rows, this.connection, callingStatement);

                break;

            case java.sql.ResultSet.CONCUR_UPDATABLE:
                rs = new UpdatableResultSet(catalog, fields, rows, this.connection, callingStatement, this.protocol.getServerSession().hasLongColumnInfo());

                break;

            default:
                return com.mysql.cj.jdbc.result.ResultSetImpl.getInstance(catalog, fields, rows, this.connection, callingStatement);
        }

        rs.setResultSetType(resultSetType);
        rs.setResultSetConcurrency(resultSetConcurrency);

        return rs;
    }

    private com.mysql.cj.jdbc.result.ResultSetImpl buildResultSetWithUpdates(StatementImpl callingStatement, PacketPayload resultPacket) throws SQLException {
        long updateCount = -1;
        long updateID = -1;
        String info = null;

        try {
            updateCount = resultPacket.readInteger(IntegerDataType.INT_LENENC);
            updateID = resultPacket.readInteger(IntegerDataType.INT_LENENC);

            // oldStatus set in sendCommand()
            this.protocol.getServerSession().setStatusFlags((int) resultPacket.readInteger(IntegerDataType.INT2));

            this.protocol.checkTransactionState();

            this.protocol.setWarningCount((int) resultPacket.readInteger(IntegerDataType.INT2));

            if (this.protocol.getWarningCount() > 0) {
                this.protocol.setHadWarnings(true); // this is a 'latch', it's reset by sendCommand()
            }

            this.protocol.setServerSlowQueryFlags();

            if (this.connection.isReadInfoMsgEnabled()) {
                info = resultPacket.readString(StringSelfDataType.STRING_TERM, this.protocol.getServerSession().getErrorMessageEncoding());
            }
        } catch (CJException ex) {
            SQLException sqlEx = SQLError.createSQLException(SQLError.get(SQLError.SQL_STATE_GENERAL_ERROR), SQLError.SQL_STATE_GENERAL_ERROR, -1, ex,
                    this.protocol.getExceptionInterceptor());
            throw sqlEx;
        }

        ResultSetInternalMethods updateRs = com.mysql.cj.jdbc.result.ResultSetImpl.getInstance(updateCount, updateID, this.connection, callingStatement);

        if (info != null) {
            ((com.mysql.cj.jdbc.result.ResultSetImpl) updateRs).setServerInfo(info);
        }

        return (com.mysql.cj.jdbc.result.ResultSetImpl) updateRs;
    }

    private final void readServerStatusForResultSets(PacketPayload rowPacket) throws SQLException {
        rowPacket.readInteger(IntegerDataType.INT1); // skips the 'last packet' flag

        if (isEOFDeprecated()) {
            // read OK packet
            rowPacket.readInteger(IntegerDataType.INT_LENENC); // affected_rows
            rowPacket.readInteger(IntegerDataType.INT_LENENC); // last_insert_id

            this.protocol.getServerSession().setStatusFlags((int) rowPacket.readInteger(IntegerDataType.INT2), true);
            this.protocol.checkTransactionState();

            this.protocol.setWarningCount((int) rowPacket.readInteger(IntegerDataType.INT2));
            if (this.protocol.getWarningCount() > 0) {
                this.protocol.setHadWarnings(true); // this is a 'latch', it's reset by sendCommand()
            }

            if (this.connection.isReadInfoMsgEnabled()) {
                rowPacket.readString(StringSelfDataType.STRING_TERM, this.protocol.getServerSession().getErrorMessageEncoding()); // info
            }

        } else {
            // read EOF packet
            this.protocol.setWarningCount((int) rowPacket.readInteger(IntegerDataType.INT2));
            if (this.protocol.getWarningCount() > 0) {
                this.protocol.setHadWarnings(true); // this is a 'latch', it's reset by sendCommand()
            }

            this.protocol.getServerSession().setStatusFlags((int) rowPacket.readInteger(IntegerDataType.INT2), true);
            this.protocol.checkTransactionState();

        }
        this.protocol.setServerSlowQueryFlags();

    }

    private ResultsetRows readSingleRowSet(long columnCount, int maxRows, int resultSetConcurrency, boolean isBinaryEncoded, Field[] fields)
            throws SQLException {
        ResultsetRows rowData;
        ArrayList<Row> rows = new ArrayList<Row>();

        // Now read the data
        Row row = nextRow(fields, (int) columnCount, isBinaryEncoded, resultSetConcurrency, false);

        int rowCount = 0;

        if (row != null) {
            rows.add(row);
            rowCount = 1;
        }

        while (row != null) {
            row = nextRow(fields, (int) columnCount, isBinaryEncoded, resultSetConcurrency, false);

            if (row != null) {
                if ((maxRows == -1) || (rowCount < maxRows)) {
                    rows.add(row);
                    rowCount++;
                }
            }
        }

        rowData = new ResultsetRowsStatic(rows);

        return rowData;
    }

    /**
     * Do we want to force a buffer row? (better for rows with large fields).
     */
    public static boolean forceBufferRow(Field[] fields) {
        if (fields == null) {
            return false;
        }

        for (int i = 0; i < fields.length; i++) {
            switch (fields[i].getMysqlType()) {
                case BLOB:
                case MEDIUMBLOB:
                case LONGBLOB:
                case TEXT:
                case MEDIUMTEXT:
                case LONGTEXT:
                    return true;
                default:
                    break;
            }
        }

        return false;
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

        PacketPayload filePacket = (this.loadFileBufRef == null) ? null : this.loadFileBufRef.get();

        int maxAllowedPacket = this.propertySet.getIntegerReadableProperty(PropertyDefinitions.PNAME_maxAllowedPacket).getValue();
        int bigPacketLength = Math.min(maxAllowedPacket - (MysqlaConstants.HEADER_LENGTH * 3),
                alignPacketSize(maxAllowedPacket - 16, 4096) - (MysqlaConstants.HEADER_LENGTH * 3));

        int oneMeg = 1024 * 1024;

        int smallerPacketSizeAligned = Math.min(oneMeg - (MysqlaConstants.HEADER_LENGTH * 3),
                alignPacketSize(oneMeg - 16, 4096) - (MysqlaConstants.HEADER_LENGTH * 3));

        int packetLength = Math.min(smallerPacketSizeAligned, bigPacketLength);

        if (filePacket == null) {
            try {
                filePacket = new Buffer(packetLength);
                this.loadFileBufRef = new SoftReference<PacketPayload>(filePacket);
            } catch (OutOfMemoryError oom) {
                throw SQLError.createSQLException(Messages.getString("MysqlIO.111", new Object[] { packetLength }), SQLError.SQL_STATE_MEMORY_ALLOCATION_ERROR,
                        this.protocol.getExceptionInterceptor());

            }
        }

        filePacket.setPosition(0);

        byte[] fileBuf = new byte[packetLength];

        BufferedInputStream fileIn = null;

        try {
            if (!this.propertySet.getBooleanReadableProperty(PropertyDefinitions.PNAME_allowLoadLocalInfile).getValue()) {
                throw SQLError.createSQLException(Messages.getString("MysqlIO.LoadDataLocalNotAllowed"), SQLError.SQL_STATE_GENERAL_ERROR,
                        this.protocol.getExceptionInterceptor());
            }

            InputStream hookedStream = null;

            if (callingStatement != null) {
                hookedStream = callingStatement.getLocalInfileInputStream();
            }

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
                this.protocol.send(filePacket, filePacket.getPosition());
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

            throw SQLError.createSQLException(messageBuf.toString(), SQLError.SQL_STATE_ILLEGAL_ARGUMENT, this.protocol.getExceptionInterceptor());
        } finally {
            if (fileIn != null) {
                try {
                    fileIn.close();
                } catch (Exception ex) {
                    SQLException sqlEx = SQLError.createSQLException(Messages.getString("MysqlIO.65"), SQLError.SQL_STATE_GENERAL_ERROR, ex,
                            this.protocol.getExceptionInterceptor());

                    throw sqlEx;
                }

                fileIn = null;
            } else {
                // file open failed, but server needs one packet
                filePacket.setPosition(0);
                this.protocol.send(filePacket, filePacket.getPosition());
                this.protocol.checkErrorPacket(); // to clear response off of queue
            }
        }

        // send empty packet to mark EOF
        filePacket.setPosition(0);
        this.protocol.send(filePacket, filePacket.getPosition());

        PacketPayload resultPacket = this.protocol.checkErrorPacket();

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
    private final Row unpackBinaryResultSetRow(Field[] fields, PacketPayload binaryData, int resultSetConcurrency) throws SQLException {
        int numFields = fields.length;

        byte[][] unpackedRowData = new byte[numFields][];

        //
        // Unpack the null bitmask, first
        //

        int nullCount = (numFields + 9) / 8;
        int nullMaskPos = binaryData.getPosition();
        binaryData.setPosition(nullMaskPos + nullCount);
        int bit = 4; // first two bits are reserved for future use

        byte[] buf = binaryData.getByteBuffer();
        for (int i = 0; i < numFields; i++) {
            if ((buf[nullMaskPos] & bit) != 0) {
                unpackedRowData[i] = null;
            } else {
                extractNativeEncodedColumn(binaryData, fields, i, unpackedRowData);
            }

            if (((bit <<= 1) & 255) == 0) {
                bit = 1; /* To next byte */

                nullMaskPos++;
            }
        }

        return new ByteArrayRow(unpackedRowData, this.protocol.getExceptionInterceptor(), new MysqlBinaryValueDecoder());
    }

    /**
     * Copy the raw result bytes from the
     * 
     * @param binaryData
     *            packet to the
     * @param unpackedRowData
     *            byte array.
     */
    private final void extractNativeEncodedColumn(PacketPayload binaryData, Field[] fields, int columnIndex, byte[][] unpackedRowData) throws SQLException {
        int type = fields[columnIndex].getMysqlTypeId();

        int len = MysqlaUtils.getBinaryEncodedLength(type);

        if (type == MysqlaConstants.FIELD_TYPE_NULL) {
            // Do nothing
        } else if (len == 0) {
            unpackedRowData[columnIndex] = binaryData.readBytes(StringSelfDataType.STRING_LENENC);
        } else if (len > 0) {
            unpackedRowData[columnIndex] = binaryData.readBytes(StringLengthDataType.STRING_FIXED, len);
        } else {
            throw SQLError
                    .createSQLException(
                            Messages.getString("MysqlIO.97") + type + Messages.getString("MysqlIO.98") + columnIndex + Messages.getString("MysqlIO.99")
                                    + fields.length + Messages.getString("MysqlIO.100"),
                            SQLError.SQL_STATE_GENERAL_ERROR, this.protocol.getExceptionInterceptor());
        }
    }

    public void checkForOutstandingStreamingData() {
        try {
            if (this.streamingData != null) {
                boolean shouldClobber = this.connection.getPropertySet().getBooleanReadableProperty(PropertyDefinitions.PNAME_clobberStreamingResults)
                        .getValue();

                if (!shouldClobber) {
                    throw SQLError.createSQLException(Messages.getString("MysqlIO.39") + this.streamingData + Messages.getString("MysqlIO.40")
                            + Messages.getString("MysqlIO.41") + Messages.getString("MysqlIO.42"), this.protocol.getExceptionInterceptor());
                }

                // Close the result set
                this.streamingData.getOwner().closeOwner(false);

                // clear any pending data....
                this.protocol.clearInputStream();
            }
        } catch (SQLException ex) {
            throw ExceptionFactory.createException(ex.getMessage(), ex);
        }
    }

    public void closeStreamer(ResultsetRows streamer) {
        if (this.streamingData == null) {
            throw ExceptionFactory.createException(Messages.getString("MysqlIO.17") + streamer + Messages.getString("MysqlIO.18"),
                    this.protocol.getExceptionInterceptor());
        }

        if (streamer != this.streamingData) {
            throw ExceptionFactory.createException(Messages.getString("MysqlIO.19") + streamer + Messages.getString("MysqlIO.20")
                    + Messages.getString("MysqlIO.21") + Messages.getString("MysqlIO.22"), this.protocol.getExceptionInterceptor());
        }

        this.streamingData = null;
    }

    public void scanForAndThrowDataTruncation() throws SQLException {
        if ((this.streamingData == null) && this.propertySet.getBooleanReadableProperty(PropertyDefinitions.PNAME_jdbcCompliantTruncation).getValue()
                && this.protocol.getWarningCount() > 0) {
            SQLError.convertShowWarningsToSQLWarnings(this.connection, this.protocol.getWarningCount(), true);
        }
    }

    public void appendDeadlockStatusInformation(String xOpen, StringBuilder errorBuf) {
        if (this.propertySet.getBooleanReadableProperty(PropertyDefinitions.PNAME_includeInnodbStatusInDeadlockExceptions).getValue() && xOpen != null
                && (xOpen.startsWith("40") || xOpen.startsWith("41")) && this.streamingData == null) {
            ResultSet rs = null;

            try {
                rs = (ResultSet) this.protocol.sqlQueryDirect(null, "SHOW ENGINE INNODB STATUS",
                        this.propertySet.getStringReadableProperty(PropertyDefinitions.PNAME_characterEncoding).getValue(), null, -1,
                        ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY, false, this.connection.getCatalog(), null,
                        this.session::getProfilerEventHandlerInstanceFunction);

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
                    try {
                        rs.close();
                    } catch (SQLException ex) {
                        throw ExceptionFactory.createException(ex.getMessage(), ex);
                    }
                }
            }
        }

        if (this.propertySet.getBooleanReadableProperty(PropertyDefinitions.PNAME_includeThreadDumpInDeadlockExceptions).getValue()) {
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

    public boolean isEOFDeprecated() {
        return this.protocol.getServerSession().isEOFDeprecated();
    }
}
