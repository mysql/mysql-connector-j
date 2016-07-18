/*
  Copyright (c) 2016, Oracle and/or its affiliates. All rights reserved.

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

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;

import com.mysql.cj.api.mysqla.io.NativeProtocol.IntegerDataType;
import com.mysql.cj.api.mysqla.io.NativeProtocol.StringSelfDataType;
import com.mysql.cj.api.mysqla.io.PacketPayload;
import com.mysql.cj.api.mysqla.io.StructureFactory;
import com.mysql.cj.api.mysqla.io.StructureReader;
import com.mysql.cj.api.mysqla.result.ColumnDefinition;
import com.mysql.cj.api.mysqla.result.ProtocolStructure;
import com.mysql.cj.api.mysqla.result.Resultset;
import com.mysql.cj.api.mysqla.result.ResultsetRows;
import com.mysql.cj.core.conf.PropertyDefinitions;
import com.mysql.cj.core.exceptions.CJException;
import com.mysql.cj.core.result.Field;
import com.mysql.cj.jdbc.exceptions.SQLError;
import com.mysql.cj.jdbc.exceptions.SQLExceptionsMapping;
import com.mysql.cj.mysqla.result.OkPacket;
import com.mysql.cj.mysqla.result.ResultSetRow;
import com.mysql.cj.mysqla.result.ResultsetRowsCursor;
import com.mysql.cj.mysqla.result.ResultsetRowsStatic;
import com.mysql.cj.mysqla.result.ResultsetRowsStreaming;

public class BinaryResultsetReader extends TextResultsetReader implements StructureReader<Resultset> {

    public BinaryResultsetReader(MysqlaProtocol prot) {
        super(prot);
    }

    /**
     * Reads one result set off of the wire, if the result is actually an
     * update count, creates an update-count only result set.
     * 
     * @param maxRows
     *            the maximum number of rows to read (-1 means all rows)
     * @param resultSetType
     *            scrollability (TYPE_FORWARD_ONLY, TYPE_SCROLL_????)
     * @param resultSetConcurrency
     *            the type of result set (CONCUR_UPDATABLE or READ_ONLY)
     * @param streamResults
     *            should the driver leave the results on the wire,
     *            and read them only when needed?
     * @param resultPacket
     *            the first packet of information in the result set
     * @param metadataFromCache
     *            metadata to avoid reading/parsing metadata
     * @param resultSetFactory
     * 
     * @return a result set that either represents the rows, or an update count
     * 
     * @throws SQLException
     *             if an error occurs while reading the rows
     */
    @Override
    public final <T extends ProtocolStructure> T readResultsForQueryOrUpdate(int maxRows, boolean streamResults, PacketPayload resultPacket,
            ColumnDefinition metadataFromCache, StructureFactory<T> resultSetFactory) throws SQLException {

        T rs = null;
        try {
            long columnCount = resultPacket.readInteger(IntegerDataType.INT_LENENC);

            if (columnCount > 0) {
                /*
                 * Build a result set with rows.
                 */

                /*
                 * Read ResultsetRows from server
                 */
                PacketPayload packet; // The packet from the server

                // Read in the column information
                ColumnDefinition cdef = this.protocol.read(ColumnDefinition.class, new ColumnDefinitionFactory(columnCount, metadataFromCache));
                Field[] fields = cdef.getFields();

                // There is no EOL packet after fields when CLIENT_DEPRECATE_EOF is set
                if (!this.protocol.getServerSession().isEOFDeprecated() ||
                        // if we asked to use cursor then there should be an OK packet here
                        (this.protocol.versionMeetsMinimum(5, 0, 2)
                                && this.propertySet.getBooleanReadableProperty(PropertyDefinitions.PNAME_useCursorFetch).getValue()
                                && resultSetFactory.getFetchSize() != 0 && resultSetFactory.getResultSetType() == ResultSet.TYPE_FORWARD_ONLY)) {

                    packet = this.protocol.readPacket(this.protocol.getReusablePacket());
                    this.protocol.readServerStatusForResultSets(packet, true);
                }

                ResultsetRows rows = null;
                boolean usingCursor = false;

                //
                // Handle cursor-based fetch first
                //

                if (this.propertySet.getBooleanReadableProperty(PropertyDefinitions.PNAME_useCursorFetch).getValue() && resultSetFactory.getFetchSize() != 0
                        && resultSetFactory.getResultSetType() == ResultSet.TYPE_FORWARD_ONLY) {
                    usingCursor = this.protocol.getServerSession().cursorExists();
                    if (usingCursor) {
                        rows = new ResultsetRowsCursor(this.protocol, fields);

                    }
                } else if (!streamResults) {
                    // read single row set

                    BinaryRowFactory brf = new BinaryRowFactory(this.protocol, cdef, resultSetFactory.getResultSetConcurrency(), false);
                    ArrayList<ResultSetRow> rowList = new ArrayList<ResultSetRow>();
                    ResultSetRow row = this.protocol.read(ResultSetRow.class, brf);

                    int rowCount = 0;

                    if (row != null) {
                        rowList.add(row);
                        rowCount = 1;
                    }

                    while (row != null) {
                        row = this.protocol.read(ResultSetRow.class, brf);

                        if (row != null) {
                            if ((maxRows == -1) || (rowCount < maxRows)) {
                                rowList.add(row);
                                rowCount++;
                            }
                        }
                    }

                    rows = new ResultsetRowsStatic(rowList, fields);

                } else {
                    rows = new ResultsetRowsStreaming<T>(this.protocol, fields, true, resultSetFactory);
                    this.protocol.setStreamingData(rows);
                }

                /*
                 * Build ResultSet from ResultsetRows
                 */
                rs = resultSetFactory.getInstance(resultSetFactory.getResultSetConcurrency(), resultSetFactory.getResultSetType(), rows);

            } else {
                // check for file request
                if (columnCount == PacketPayload.NULL_LENGTH) {
                    String charEncoding = this.propertySet.getStringReadableProperty(PropertyDefinitions.PNAME_characterEncoding).getValue();
                    String fileName = resultPacket.readString(StringSelfDataType.STRING_TERM,
                            this.protocol.doesPlatformDbCharsetMatches() ? charEncoding : null);
                    resultPacket = this.protocol.sendFileToServer(fileName);
                }

                /*
                 * Build ResultSet with no ResultsetRows
                 */

                OkPacket ok;

                try {
                    // read and parse OK packet
                    ok = this.protocol.readServerStatusForResultSets(resultPacket, false); // oldStatus set in sendCommand()
                } catch (CJException ex) {
                    SQLException sqlEx = SQLError.createSQLException(SQLError.get(SQLError.SQL_STATE_GENERAL_ERROR), SQLError.SQL_STATE_GENERAL_ERROR, -1, ex,
                            this.protocol.getExceptionInterceptor());
                    throw sqlEx;
                }

                rs = resultSetFactory.getInstance(ok);
            }
            return rs;

        } catch (IOException ioEx) {
            throw SQLError.createCommunicationsException(this.protocol.getConnection(), this.protocol.getPacketSentTimeHolder().getLastPacketSentTime(),
                    this.protocol.getPacketReceivedTimeHolder().getLastPacketReceivedTime(), ioEx, this.protocol.getExceptionInterceptor());
        } catch (CJException e) {
            throw SQLExceptionsMapping.translateException(e, this.protocol.getExceptionInterceptor());
        }
    }
}
