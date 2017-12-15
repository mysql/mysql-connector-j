/*
 * Copyright (c) 2016, Oracle and/or its affiliates. All rights reserved.
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

package com.mysql.cj.mysqla.io;

import com.mysql.cj.api.mysqla.io.NativeProtocol.IntegerDataType;
import com.mysql.cj.api.mysqla.io.PacketPayload;
import com.mysql.cj.api.mysqla.io.ProtocolEntityFactory;
import com.mysql.cj.api.mysqla.io.ProtocolEntityReader;
import com.mysql.cj.api.mysqla.result.ColumnDefinition;
import com.mysql.cj.core.MysqlType;
import com.mysql.cj.core.result.Field;
import com.mysql.cj.core.util.LazyString;

public class ColumnDefinitionReader implements ProtocolEntityReader<ColumnDefinition> {

    private MysqlaProtocol protocol;

    public ColumnDefinitionReader(MysqlaProtocol prot) {
        this.protocol = prot;
    }

    @Override
    public ColumnDefinition read(ProtocolEntityFactory<ColumnDefinition> sf) {

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
        Field[] fields = null;
        boolean checkEOF = !this.protocol.getServerSession().isEOFDeprecated();

        // Read in the column information

        fields = new Field[(int) columnCount];

        for (int i = 0; i < columnCount; i++) {
            PacketPayload fieldPacket = this.protocol.readPacket(null);
            // next check is needed for SSPS
            if (checkEOF && fieldPacket.isEOFPacket()) {
                break;
            }
            fields[i] = unpackField(fieldPacket, this.protocol.getServerSession().getCharacterSetMetadata());
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

        MysqlType mysqlType = MysqlaProtocol.findMysqlType(this.protocol.getPropertySet(), colType, colFlag, colLength, tableName, originalTableName,
                collationIndex, encoding);

        return new Field(databaseName, tableName, originalTableName, columnName, originalColumnName, colLength, colType, colFlag, colDecimals, collationIndex,
                encoding, mysqlType);
    }

}
