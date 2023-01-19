/*
 * Copyright (c) 2016, 2022, Oracle and/or its affiliates.
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

import com.mysql.cj.MysqlType;
import com.mysql.cj.protocol.ColumnDefinition;
import com.mysql.cj.protocol.ProtocolEntityFactory;
import com.mysql.cj.protocol.ProtocolEntityReader;
import com.mysql.cj.protocol.a.NativeConstants.IntegerDataType;
import com.mysql.cj.result.Field;
import com.mysql.cj.util.LazyString;

public class ColumnDefinitionReader implements ProtocolEntityReader<ColumnDefinition, NativePacketPayload> {

    private NativeProtocol protocol;

    public ColumnDefinitionReader(NativeProtocol prot) {
        this.protocol = prot;
    }

    @Override
    public ColumnDefinition read(ProtocolEntityFactory<ColumnDefinition, NativePacketPayload> sf) {

        ColumnDefinitionFactory cdf = (ColumnDefinitionFactory) sf;

        long columnCount = cdf.getColumnCount();
        ColumnDefinition cdef = cdf.getColumnDefinitionFromCache();

        if (cdef != null && !cdf.mergeColumnDefinitions()) {
            for (int i = 0; i < columnCount; i++) {
                this.protocol.skipPacket();
            }
            return cdef;
        }

        /* read the metadata from the server */
        Field[] fields = new Field[(int) columnCount];

        for (int i = 0; i < columnCount; i++) {
            NativePacketPayload fieldPacket = this.protocol.readMessage(null);
            fields[i] = unpackField(fieldPacket, this.protocol.getServerSession().getCharsetSettings().getMetadataEncoding());
        }

        return cdf.createFromFields(fields);
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
    protected Field unpackField(NativePacketPayload packet, String characterSetMetadata) {
        int offset, length;

        length = (int) packet.readInteger(IntegerDataType.INT_LENENC);
        packet.setPosition(packet.getPosition() + length); // skip database name

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

        String encoding = this.protocol.getServerSession().getCharsetSettings().getJavaEncodingForCollationIndex(collationIndex);

        MysqlType mysqlType = NativeProtocol.findMysqlType(this.protocol.getPropertySet(), colType, colFlag, colLength, tableName, originalTableName,
                collationIndex, encoding);

        // Protocol returns precision and scale differently for some types. We need to align then to I_S.
        switch (mysqlType) {
            case TINYINT:
            case TINYINT_UNSIGNED:
            case SMALLINT:
            case SMALLINT_UNSIGNED:
            case MEDIUMINT:
            case MEDIUMINT_UNSIGNED:
            case INT:
            case INT_UNSIGNED:
            case BIGINT:
            case BIGINT_UNSIGNED:
            case BOOLEAN:
                colLength = mysqlType.getPrecision().intValue();
                break;

            case DECIMAL:
                colLength--;
                if (colDecimals > 0) {
                    colLength--;
                }
                break;
            case DECIMAL_UNSIGNED:
                if (colDecimals > 0) {
                    colLength--;
                }
                break;
            case FLOAT:
            case FLOAT_UNSIGNED:
            case DOUBLE:
            case DOUBLE_UNSIGNED:
                // According to SQL standard, NUMERIC_SCALE should be NULL for approximate numeric data types.
                // DECIMAL_NOT_SPECIFIED=31 is the MySQL internal constant value used to indicate that NUMERIC_SCALE is not applicable.
                // It's probably a mistake that it's exposed by protocol as a decimals and it should be replaced with 0. 
                if (colDecimals == 31) {
                    colDecimals = 0;
                }
                break;

            default:
                break;
        }

        return new Field(databaseName, tableName, originalTableName, columnName, originalColumnName, colLength, colType, colFlag, colDecimals, collationIndex,
                encoding, mysqlType);
    }

}
