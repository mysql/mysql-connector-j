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
import java.util.Optional;

import com.mysql.cj.api.mysqla.io.NativeProtocol.StringLengthDataType;
import com.mysql.cj.api.mysqla.io.PacketHeader;
import com.mysql.cj.api.mysqla.io.PacketPayload;
import com.mysql.cj.api.mysqla.io.PacketReader;
import com.mysql.cj.core.Messages;
import com.mysql.cj.mysqla.MysqlaConstants;

/**
 * A {@link PacketReader} which reads a full packet
 * built from sequence of it's on-wire parts.
 * See http://dev.mysql.com/doc/internals/en/sending-more-than-16mbyte.html
 */
public class MultiPacketReader implements PacketReader {

    private PacketReader packetReader;

    public MultiPacketReader(PacketReader packetReader) {
        this.packetReader = packetReader;
    }

    @Override
    public PacketHeader readHeader() throws IOException {
        return this.packetReader.readHeader();
    }

    @Override
    public PacketPayload readPayload(Optional<PacketPayload> reuse, int packetLength) throws IOException {

        PacketPayload buf = this.packetReader.readPayload(reuse, packetLength);

        if (packetLength == MysqlaConstants.MAX_PACKET_SIZE) { // it's a multi-packet

            buf.setPosition(MysqlaConstants.MAX_PACKET_SIZE);

            PacketPayload multiPacket = null;
            int multiPacketLength = -1;
            byte multiPacketSeq = getPacketSequence();

            do {
                PacketHeader hdr = readHeader();
                multiPacketLength = hdr.getPacketLength();

                if (multiPacket == null) {
                    multiPacket = new Buffer(multiPacketLength);
                }

                multiPacketSeq++;
                if (multiPacketSeq != hdr.getPacketSequence()) {
                    throw new IOException(Messages.getString("PacketReader.10"));
                }

                this.packetReader.readPayload(Optional.of(multiPacket), multiPacketLength);

                buf.writeBytes(StringLengthDataType.STRING_FIXED, multiPacket.getByteBuffer(), 0, multiPacketLength);

            } while (multiPacketLength == MysqlaConstants.MAX_PACKET_SIZE);

            buf.setPosition(0);
        }

        return buf;
    }

    @Override
    public byte getPacketSequence() {
        return this.packetReader.getPacketSequence();
    }

    @Override
    public void resetPacketSequence() {
        this.packetReader.resetPacketSequence();
    }

    @Override
    public PacketReader undecorateAll() {
        return this.packetReader.undecorateAll();
    }

    @Override
    public PacketReader undecorate() {
        return this.packetReader;
    }

}
