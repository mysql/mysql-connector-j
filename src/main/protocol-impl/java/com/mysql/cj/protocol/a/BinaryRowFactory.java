/*
 * Copyright (c) 2016, 2018, Oracle and/or its affiliates. All rights reserved.
 *
 * This program is free software; you can redistribute it and/or modify it under
 * the terms of the GNU General Public License, version 2.0, as published by the
 * Free Software Foundation.
 *
 * This program is also distributed with certain software (including but not
 * limited to OpenSSL) that is licensed under separate terms, as designated in a
 * particular file or component or in included license documentation. The
 * authors of MySQL hereby grant you an additional permission to link the
 * program and your derivative works with the separately licensed software that
 * they have included with MySQL.
 *
 * Without limiting anything contained in the foregoing, this file, which is
 * part of MySQL Connector/J, is also subject to the Universal FOSS Exception,
 * version 1.0, a copy of which can be found at
 * http://oss.oracle.com/licenses/universal-foss-exception.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 * FOR A PARTICULAR PURPOSE. See the GNU General Public License, version 2.0,
 * for more details.
 *
 * You should have received a copy of the GNU General Public License along with
 * this program; if not, write to the Free Software Foundation, Inc.,
 * 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA
 */

package com.mysql.cj.protocol.a;

import com.mysql.cj.Messages;
import com.mysql.cj.MysqlType;
import com.mysql.cj.conf.PropertyDefinitions;
import com.mysql.cj.exceptions.ExceptionFactory;
import com.mysql.cj.protocol.ColumnDefinition;
import com.mysql.cj.protocol.ProtocolEntityFactory;
import com.mysql.cj.protocol.Resultset;
import com.mysql.cj.protocol.Resultset.Concurrency;
import com.mysql.cj.protocol.ResultsetRow;
import com.mysql.cj.protocol.a.NativeConstants.StringLengthDataType;
import com.mysql.cj.protocol.a.NativeConstants.StringSelfDataType;
import com.mysql.cj.protocol.a.result.BinaryBufferRow;
import com.mysql.cj.protocol.a.result.ByteArrayRow;
import com.mysql.cj.result.Field;

/**
 * Handle binary-encoded data for server-side PreparedStatements
 *
 */
public class BinaryRowFactory extends AbstractRowFactory implements ProtocolEntityFactory<ResultsetRow, NativePacketPayload> {

    public BinaryRowFactory(NativeProtocol protocol, ColumnDefinition columnDefinition, Resultset.Concurrency resultSetConcurrency,
            boolean canReuseRowPacketForBufferRow) {
        this.columnDefinition = columnDefinition;
        this.resultSetConcurrency = resultSetConcurrency;
        this.canReuseRowPacketForBufferRow = canReuseRowPacketForBufferRow;
        this.useBufferRowSizeThreshold = protocol.getPropertySet().getMemorySizeProperty(PropertyDefinitions.PNAME_largeRowSizeThreshold);
        this.exceptionInterceptor = protocol.getExceptionInterceptor();
        this.valueDecoder = new MysqlBinaryValueDecoder();
    }

    @Override
    public ResultsetRow createFromMessage(NativePacketPayload rowPacket) {

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
     *            {@link Field}s array
     * @param binaryData
     *            data
     * 
     * @return byte[][]
     */
    private final ResultsetRow unpackBinaryResultSetRow(Field[] fields, NativePacketPayload binaryData) {
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
     * @param fields
     *            {@link Field}s array
     * @param columnIndex
     *            column index
     * @param unpackedRowData
     *            byte array.
     */
    private final void extractNativeEncodedColumn(NativePacketPayload binaryData, Field[] fields, int columnIndex, byte[][] unpackedRowData) {
        int type = fields[columnIndex].getMysqlTypeId();

        int len = NativeUtils.getBinaryEncodedLength(type);

        if (type == MysqlType.FIELD_TYPE_NULL) {
            // Do nothing
        } else if (len == 0) {
            unpackedRowData[columnIndex] = binaryData.readBytes(StringSelfDataType.STRING_LENENC);
        } else if (len > 0) {
            unpackedRowData[columnIndex] = binaryData.readBytes(StringLengthDataType.STRING_FIXED, len);
        } else {
            throw ExceptionFactory.createException(Messages.getString("MysqlIO.97", new Object[] { type, columnIndex, fields.length }));
        }
    }
}
