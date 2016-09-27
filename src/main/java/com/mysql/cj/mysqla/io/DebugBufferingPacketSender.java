/*
  Copyright (c) 2015, 2016, Oracle and/or its affiliates. All rights reserved.

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

import com.mysql.cj.api.conf.ReadableProperty;
import com.mysql.cj.api.mysqla.io.PacketSender;
import com.mysql.cj.core.util.StringUtils;
import com.mysql.cj.mysqla.MysqlaConstants;

public class DebugBufferingPacketSender implements PacketSender {
    private PacketSender packetSender;
    private LinkedList<StringBuilder> packetDebugBuffer;
    private ReadableProperty<Integer> packetDebugBufferSize;
    private int maxPacketDumpLength = 1024;

    private static final int DEBUG_MSG_LEN = 64;

    public DebugBufferingPacketSender(PacketSender packetSender, LinkedList<StringBuilder> packetDebugBuffer, ReadableProperty<Integer> packetDebugBufferSize) {
        this.packetSender = packetSender;
        this.packetDebugBuffer = packetDebugBuffer;
        this.packetDebugBufferSize = packetDebugBufferSize;
    }

    public void setMaxPacketDumpLength(int maxPacketDumpLength) {
        this.maxPacketDumpLength = maxPacketDumpLength;
    }

    /**
     * Add a packet to the debug buffer.
     */
    private void pushPacketToDebugBuffer(byte[] packet, int packetLen) {
        int bytesToDump = Math.min(this.maxPacketDumpLength, packetLen);

        String packetPayload = StringUtils.dumpAsHex(packet, bytesToDump);

        StringBuilder packetDump = new StringBuilder(DEBUG_MSG_LEN + MysqlaConstants.HEADER_LENGTH + packetPayload.length());

        packetDump.append("Client ");
        packetDump.append(packet.toString());
        packetDump.append("--------------------> Server\n");
        packetDump.append("\nPacket payload:\n\n");
        packetDump.append(packetPayload);

        if (packetLen > this.maxPacketDumpLength) {
            packetDump.append("\nNote: Packet of " + packetLen + " bytes truncated to " + this.maxPacketDumpLength + " bytes.\n");
        }

        if ((this.packetDebugBuffer.size() + 1) > this.packetDebugBufferSize.getValue()) {
            this.packetDebugBuffer.removeFirst();
        }

        this.packetDebugBuffer.addLast(packetDump);
    }

    public void send(byte[] packet, int packetLen, byte packetSequence) throws IOException {
        pushPacketToDebugBuffer(packet, packetLen);
        this.packetSender.send(packet, packetLen, packetSequence);
    }

    @Override
    public PacketSender undecorateAll() {
        return this.packetSender.undecorateAll();
    }

    @Override
    public PacketSender undecorate() {
        return this.packetSender;
    }
}
