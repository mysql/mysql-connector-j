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

import com.mysql.cj.api.mysqla.io.NativeProtocol.IntegerDataType;
import com.mysql.cj.api.mysqla.io.PacketPayload;
import com.mysql.cj.api.mysqla.io.ProtocolEntityFactory;
import com.mysql.cj.api.mysqla.io.ProtocolEntityReader;
import com.mysql.cj.api.mysqla.result.ColumnDefinition;
import com.mysql.cj.core.MysqlType;
import com.mysql.cj.core.result.Field;
import com.mysql.cj.core.util.LazyString;
import com.mysql.cj.mysqla.result.MysqlaColumnDefinition;

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

        if (cdef != null) {
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

        // TODO do it via ColumnDefinitionFactory
        return new MysqlaColumnDefinition(fields);
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
