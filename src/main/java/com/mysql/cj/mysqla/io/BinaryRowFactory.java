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

import com.mysql.cj.api.mysqla.io.NativeProtocol.StringLengthDataType;
import com.mysql.cj.api.mysqla.io.NativeProtocol.StringSelfDataType;
import com.mysql.cj.api.mysqla.io.PacketPayload;
import com.mysql.cj.api.mysqla.io.ProtocolEntityFactory;
import com.mysql.cj.api.mysqla.result.ColumnDefinition;
import com.mysql.cj.api.mysqla.result.Resultset;
import com.mysql.cj.api.mysqla.result.Resultset.Concurrency;
import com.mysql.cj.api.mysqla.result.ResultsetRow;
import com.mysql.cj.core.Messages;
import com.mysql.cj.core.conf.PropertyDefinitions;
import com.mysql.cj.core.exceptions.ExceptionFactory;
import com.mysql.cj.core.io.MysqlBinaryValueDecoder;
import com.mysql.cj.core.result.Field;
import com.mysql.cj.mysqla.MysqlaConstants;
import com.mysql.cj.mysqla.MysqlaUtils;
import com.mysql.cj.mysqla.result.BinaryBufferRow;
import com.mysql.cj.mysqla.result.ByteArrayRow;

/**
 * Handle binary-encoded data for server-side PreparedStatements
 *
 */
public class BinaryRowFactory extends AbstractRowFactory implements ProtocolEntityFactory<ResultsetRow> {

    public BinaryRowFactory(MysqlaProtocol protocol, ColumnDefinition columnDefinition, Resultset.Concurrency resultSetConcurrency,
            boolean canReuseRowPacketForBufferRow) {
        this.columnDefinition = columnDefinition;
        this.resultSetConcurrency = resultSetConcurrency;
        this.canReuseRowPacketForBufferRow = canReuseRowPacketForBufferRow;
        this.useBufferRowSizeThreshold = protocol.getPropertySet().getMemorySizeReadableProperty(PropertyDefinitions.PNAME_largeRowSizeThreshold);
        this.exceptionInterceptor = protocol.getExceptionInterceptor();
        this.valueDecoder = new MysqlBinaryValueDecoder();
    }

    @Override
    public ResultsetRow createFromPacketPayload(PacketPayload rowPacket) {

        // use a buffer row for reusable packets (streaming results), blobs and long strings
        // or if we're over the threshold
        boolean useBufferRow = this.canReuseRowPacketForBufferRow || this.columnDefinition.hasLargeFields()
                || rowPacket.getPayloadLength() >= this.useBufferRowSizeThreshold.getValue();

        // bump past ProtocolBinary::ResultsetRow packet header
        rowPacket.setPosition(rowPacket.getPosition() + 1);

        if (this.resultSetConcurrency == Concurrency.UPDATABLE || !useBufferRow) {
            return unpackBinaryResultSetRow(this.columnDefinition.getFields(), rowPacket);
        }

        return new BinaryBufferRow(rowPacket, this.columnDefinition, this.exceptionInterceptor, this.valueDecoder);
    }

    @Override
    public boolean canReuseRowPacketForBufferRow() {
        return this.canReuseRowPacketForBufferRow;
    }

    /**
     * Un-packs binary-encoded result set data for one row
     * 
     * @param fields
     * @param binaryData
     * 
     * @return byte[][]
     */
    private final ResultsetRow unpackBinaryResultSetRow(Field[] fields, PacketPayload binaryData) {
        int numFields = fields.length;

        byte[][] unpackedRowBytes = new byte[numFields][];

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
                unpackedRowBytes[i] = null;
            } else {
                extractNativeEncodedColumn(binaryData, fields, i, unpackedRowBytes);
            }

            if (((bit <<= 1) & 255) == 0) {
                bit = 1; /* To next byte */

                nullMaskPos++;
            }
        }

        return new ByteArrayRow(unpackedRowBytes, this.exceptionInterceptor, new MysqlBinaryValueDecoder());
    }

    /**
     * Copy the raw result bytes from the
     * 
     * @param binaryData
     *            packet to the
     * @param unpackedRowData
     *            byte array.
     */
    private final void extractNativeEncodedColumn(PacketPayload binaryData, Field[] fields, int columnIndex, byte[][] unpackedRowData) {
        int type = fields[columnIndex].getMysqlTypeId();

        int len = MysqlaUtils.getBinaryEncodedLength(type);

        if (type == MysqlaConstants.FIELD_TYPE_NULL) {
            // Do nothing
        } else if (len == 0) {
            unpackedRowData[columnIndex] = binaryData.readBytes(StringSelfDataType.STRING_LENENC);
        } else if (len > 0) {
            unpackedRowData[columnIndex] = binaryData.readBytes(StringLengthDataType.STRING_FIXED, len);
        } else {
            throw ExceptionFactory.createException(Messages.getString("MysqlIO.97") + type + Messages.getString("MysqlIO.98") + columnIndex
                    + Messages.getString("MysqlIO.99") + fields.length + Messages.getString("MysqlIO.100"));
        }
    }
}
