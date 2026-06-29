/*
 * Copyright (c) 2016, 2026, Oracle and/or its affiliates.
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

import java.io.IOException;
import java.util.ArrayList;

import com.mysql.cj.conf.PropertyKey;
import com.mysql.cj.protocol.ColumnDefinition;
import com.mysql.cj.protocol.ProtocolEntityFactory;
import com.mysql.cj.protocol.ProtocolEntityReader;
import com.mysql.cj.protocol.Resultset;
import com.mysql.cj.protocol.Resultset.Type;
import com.mysql.cj.protocol.ResultsetRow;
import com.mysql.cj.protocol.ResultsetRows;
import com.mysql.cj.protocol.a.NativeConstants.IntegerDataType;
import com.mysql.cj.protocol.a.NativeConstants.StringSelfDataType;
import com.mysql.cj.protocol.a.result.OkPacket;
import com.mysql.cj.protocol.a.result.ResultsetRowsCursor;
import com.mysql.cj.protocol.a.result.ResultsetRowsStatic;
import com.mysql.cj.protocol.a.result.ResultsetRowsStreaming;

public class BinaryResultsetReader implements ProtocolEntityReader<Resultset, NativePacketPayload> {

    protected NativeProtocol protocol;

    public BinaryResultsetReader(NativeProtocol prot) {
        this.protocol = prot;
    }

    @Override
    public Resultset read(int maxRows, boolean streamResults, NativePacketPayload resultPacket, ColumnDefinition metadata,
            ProtocolEntityFactory<Resultset, NativePacketPayload> resultSetFactory) throws IOException {
        Resultset rs = null;
        //try {
        long columnCount = resultPacket.readInteger(IntegerDataType.INT_LENENC);

        if (columnCount > 0) {
            // Build a result set with rows.

            // When CLIENT_CACHE_METADATA is negotiated the server inserts a 1-byte flag after the column count:
            //   1 = column definitions follow (regular protocol)
            //   0 = column definitions are skipped because metadata is unchanged
            boolean metadataFollows = true;
            if (this.protocol.getServerSession().hasCacheMetadataEnabled()) {
                metadataFollows = resultPacket.readInteger(IntegerDataType.INT1) != 0;
            }

            // Read in the column information
            ColumnDefinition cdef = metadataFollows //
                    ? this.protocol.read(ColumnDefinition.class, new MergingColumnDefinitionFactory(columnCount, metadata)) //
                    : metadata;

            boolean isCursorPossible = this.protocol.getPropertySet().getBooleanProperty(PropertyKey.useCursorFetch).getValue()
                    && resultSetFactory.getResultSetType() == Type.FORWARD_ONLY && resultSetFactory.getFetchSize() > 0;

            // If CLIENT_DEPRECATE_EOF is set, there is no way to tell which one, OK or ResultsetRow, is the next packet, so it should be read but not consumed.
            // At this point 3 types of packets are expected:
            // 1. If CLIENT_DEPRECATE_EOF is not set then an EOF packet is always expected to be the next one.
            // 2. If CLIENT_DEPRECATE_EOF is set and a cursor was created then the next packet is an OK with 0xFE signature.
            // 3. If CLIENT_DEPRECATE_EOF is set and a cursor was not created then the next packet is either a ProtocolBinary::ResultsetRow or an OK packet
            // signaling the end of the (empty) results.
            if (isCursorPossible) {
                // Read the next packet but leave it in the reader cache. In case it's not the OK or EOF one it will be read again by ResultSet factories.
                NativePacketPayload rowPacket = this.protocol.probeMessage(this.protocol.getReusablePacket());
                this.protocol.checkErrorMessage(rowPacket);
                if (rowPacket.isResultSetOKPacket() || rowPacket.isEOFPacket()) {
                    // Read the status flags from the OK/EOF packet. The SERVER_STATUS_CURSOR_EXISTS flag should indicate the cursor state in this case.
                    this.protocol.readServerStatusForResultSets(rowPacket, true);
                    if (this.protocol.getServerSession().cursorExists()) {
                        // Consume the OK/EOF packet from the reader cache only if a cursor was created.
                        rowPacket = this.protocol.readMessage(this.protocol.getReusablePacket());
                    }
                } else {
                    // If it's not an OK/EOF then the cursor is not created and this recent packet is a row.
                    // Retain the packet in the reader cache.
                    isCursorPossible = false;
                }
            } else if (!this.protocol.getServerSession().isEOFDeprecated()) {
                // There can't be a cursor and CLIENT_DEPRECATE_EOF was not set, therefore consume next (EOF) packet.
                this.protocol.readMessage(this.protocol.getReusablePacket());
            }

            ResultsetRows rows = null;

            if (isCursorPossible && this.protocol.getServerSession().cursorExists()) {
                rows = new ResultsetRowsCursor(this.protocol, cdef);

            } else if (!streamResults) {
                BinaryRowFactory brf = new BinaryRowFactory(this.protocol, cdef, resultSetFactory.getResultSetConcurrency(), false);

                ArrayList<ResultsetRow> rowList = new ArrayList<>();
                ResultsetRow row = this.protocol.read(ResultsetRow.class, brf);
                while (row != null) {
                    if (maxRows == -1 || rowList.size() < maxRows) {
                        rowList.add(row);
                    }
                    row = this.protocol.read(ResultsetRow.class, brf);
                }

                rows = new ResultsetRowsStatic(rowList, cdef);

            } else {
                rows = new ResultsetRowsStreaming<>(this.protocol, cdef, true, resultSetFactory);
                this.protocol.setStreamingData(rows);
            }

            /*
             * Build ResultSet from ResultsetRows
             */
            rs = resultSetFactory.createFromProtocolEntity(rows);

        } else {
            // check for file request
            if (columnCount == NativePacketPayload.NULL_LENGTH) {
                String charEncoding = this.protocol.getPropertySet().getStringProperty(PropertyKey.characterEncoding).getValue();
                String fileName = resultPacket.readString(StringSelfDataType.STRING_TERM,
                        this.protocol.getServerSession().getCharsetSettings().doesPlatformDbCharsetMatches() ? null : charEncoding);
                resultPacket = this.protocol.sendFileToServer(fileName);
            }

            /*
             * Build ResultSet with no ResultsetRows
             */

            // read and parse OK packet
            OkPacket ok = this.protocol.readServerStatusForResultSets(resultPacket, false); // oldStatus set in sendCommand()

            rs = resultSetFactory.createFromProtocolEntity(ok);
        }
        return rs;
    }

}
