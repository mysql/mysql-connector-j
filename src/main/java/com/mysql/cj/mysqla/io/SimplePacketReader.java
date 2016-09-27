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

import com.mysql.cj.api.conf.ReadableProperty;
import com.mysql.cj.api.io.SocketConnection;
import com.mysql.cj.api.mysqla.io.PacketHeader;
import com.mysql.cj.api.mysqla.io.PacketPayload;
import com.mysql.cj.api.mysqla.io.PacketReader;
import com.mysql.cj.core.Messages;
import com.mysql.cj.core.exceptions.CJPacketTooBigException;
import com.mysql.cj.mysqla.MysqlaConstants;

/**
 * Simple implementation of {@link PacketReader} which handles the receiving of logical MySQL packets from the provided socket input stream.
 * Multi-packets are handled outside of this reader.
 */
public class SimplePacketReader implements PacketReader {

    protected SocketConnection socketConnection;
    protected ReadableProperty<Integer> maxAllowedPacket;

    private byte readPacketSequence = -1;

    public SimplePacketReader(SocketConnection socketConnection, ReadableProperty<Integer> maxAllowedPacket) {
        this.socketConnection = socketConnection;
        this.maxAllowedPacket = maxAllowedPacket;
    }

    @Override
    public PacketHeader readHeader() throws IOException {

        PacketHeader hdr = new DefaultPacketHeader();

        try {
            this.socketConnection.getMysqlInput().readFully(hdr.getBuffer(), 0, MysqlaConstants.HEADER_LENGTH);

            int packetLength = hdr.getPacketLength();

            if (packetLength > this.maxAllowedPacket.getValue()) {
                throw new CJPacketTooBigException(packetLength, this.maxAllowedPacket.getValue());
            }

        } catch (IOException | CJPacketTooBigException e) {
            try {
                this.socketConnection.forceClose();
            } catch (Exception ex) {
                // ignore
            }
            throw e;
        }

        this.readPacketSequence = hdr.getPacketSequence();

        return hdr;
    }

    @Override
    public PacketPayload readPayload(Optional<PacketPayload> reuse, int packetLength) throws IOException {
        try {
            PacketPayload buf;
            if (reuse.isPresent()) {
                buf = reuse.get();
                // Set the Buffer to it's original state
                buf.setPosition(0);
                // Do we need to re-alloc the byte buffer?
                if (buf.getByteBuffer().length < packetLength) {
                    // Note: We actually check the length of the buffer, rather than getBufLength(), because getBufLength()
                    // is not necessarily the actual length of the byte array used as the buffer
                    buf.setByteBuffer(new byte[packetLength]);
                }

                // Set the new length
                buf.setPayloadLength(packetLength);
            } else {
                buf = new Buffer(new byte[packetLength]);
            }

            // Read the data from the server
            int numBytesRead = this.socketConnection.getMysqlInput().readFully(buf.getByteBuffer(), 0, packetLength);
            if (numBytesRead != packetLength) {
                throw new IOException(Messages.getString("PacketReader.1", new Object[] { packetLength, numBytesRead }));
            }
            return buf;

        } catch (IOException e) {
            try {
                this.socketConnection.forceClose();
            } catch (Exception ex) {
                // ignore
            }
            throw e;
        }
    }

    @Override
    public byte getPacketSequence() {
        return this.readPacketSequence;
    }

    @Override
    public void resetPacketSequence() {
        this.readPacketSequence = 0;
    }

    @Override
    public PacketReader undecorateAll() {
        return this;
    }

    @Override
    public PacketReader undecorate() {
        return this;
    }

}
