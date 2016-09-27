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
import java.util.LinkedList;
import java.util.Optional;

import com.mysql.cj.api.conf.ReadableProperty;
import com.mysql.cj.api.mysqla.io.PacketHeader;
import com.mysql.cj.api.mysqla.io.PacketPayload;
import com.mysql.cj.api.mysqla.io.PacketReader;
import com.mysql.cj.core.Messages;
import com.mysql.cj.core.util.StringUtils;
import com.mysql.cj.mysqla.MysqlaConstants;

/**
 * A decorating {@link PacketReader} which put debugging info to a ring-buffer.
 */
public class DebugBufferingPacketReader implements PacketReader {

    /** Max number of bytes to dump when tracing the protocol */
    private static final int MAX_PACKET_DUMP_LENGTH = 1024;
    private static final int DEBUG_MSG_LEN = 96;

    private PacketReader packetReader;
    private LinkedList<StringBuilder> packetDebugBuffer;
    private ReadableProperty<Integer> packetDebugBufferSize;
    private String lastHeaderPayload = "";

    private boolean packetSequenceReset = false;

    public DebugBufferingPacketReader(PacketReader packetReader, LinkedList<StringBuilder> packetDebugBuffer, ReadableProperty<Integer> packetDebugBufferSize) {
        this.packetReader = packetReader;
        this.packetDebugBuffer = packetDebugBuffer;
        this.packetDebugBufferSize = packetDebugBufferSize;
    }

    @Override
    public PacketHeader readHeader() throws IOException {

        byte prevPacketSeq = this.packetReader.getPacketSequence();

        PacketHeader hdr = this.packetReader.readHeader();

        // Normally we shouldn't get into situation of getting packets out of order from server,
        // so we do this check only in debug mode.
        byte currPacketSeq = hdr.getPacketSequence();
        if (!this.packetSequenceReset) {

            if ((currPacketSeq == -128) && (prevPacketSeq != 127)) {
                throw new IOException(Messages.getString("PacketReader.9", new Object[] { "-128", currPacketSeq }));
            }

            if ((prevPacketSeq == -1) && (currPacketSeq != 0)) {
                throw new IOException(Messages.getString("PacketReader.9", new Object[] { "-1", currPacketSeq }));
            }

            if ((currPacketSeq != -128) && (prevPacketSeq != -1) && (currPacketSeq != (prevPacketSeq + 1))) {
                throw new IOException(Messages.getString("PacketReader.9", new Object[] { (prevPacketSeq + 1), currPacketSeq }));
            }

        } else {
            this.packetSequenceReset = false;
        }

        this.lastHeaderPayload = StringUtils.dumpAsHex(hdr.getBuffer(), MysqlaConstants.HEADER_LENGTH);

        return hdr;
    }

    @Override
    public PacketPayload readPayload(Optional<PacketPayload> reuse, int packetLength) throws IOException {
        PacketPayload buf = this.packetReader.readPayload(reuse, packetLength);

        int bytesToDump = Math.min(MAX_PACKET_DUMP_LENGTH, packetLength);
        String packetPayload = StringUtils.dumpAsHex(buf.getByteBuffer(), bytesToDump);

        StringBuilder packetDump = new StringBuilder(DEBUG_MSG_LEN + MysqlaConstants.HEADER_LENGTH + packetPayload.length());
        packetDump.append("Server ");
        packetDump.append(reuse.isPresent() ? "(re-used) " : "(new) ");
        packetDump.append(buf.toString());
        packetDump.append(" --------------------> Client\n");
        packetDump.append("\nPacket payload:\n\n");
        packetDump.append(this.lastHeaderPayload);
        packetDump.append(packetPayload);

        if (bytesToDump == MAX_PACKET_DUMP_LENGTH) {
            packetDump.append("\nNote: Packet of " + packetLength + " bytes truncated to " + MAX_PACKET_DUMP_LENGTH + " bytes.\n");
        }

        if ((this.packetDebugBuffer.size() + 1) > this.packetDebugBufferSize.getValue()) {
            this.packetDebugBuffer.removeFirst();
        }

        this.packetDebugBuffer.addLast(packetDump);

        return buf;
    }

    @Override
    public byte getPacketSequence() {
        return this.packetReader.getPacketSequence();
    }

    @Override
    public void resetPacketSequence() {
        this.packetReader.resetPacketSequence();
        this.packetSequenceReset = true;
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
