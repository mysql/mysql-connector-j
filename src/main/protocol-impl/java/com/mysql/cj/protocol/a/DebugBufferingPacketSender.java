/*
 * Copyright (c) 2015, 2024, Oracle and/or its affiliates.
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
import java.util.LinkedList;

import com.mysql.cj.conf.RuntimeProperty;
import com.mysql.cj.protocol.MessageSender;
import com.mysql.cj.util.StringUtils;

public class DebugBufferingPacketSender implements MessageSender<NativePacketPayload> {

    private MessageSender<NativePacketPayload> packetSender;
    private LinkedList<StringBuilder> packetDebugBuffer;
    private RuntimeProperty<Integer> packetDebugBufferSize;
    private int maxPacketDumpLength = 1024;

    private static final int DEBUG_MSG_LEN = 64;

    public DebugBufferingPacketSender(MessageSender<NativePacketPayload> packetSender, LinkedList<StringBuilder> packetDebugBuffer,
            RuntimeProperty<Integer> packetDebugBufferSize) {
        this.packetSender = packetSender;
        this.packetDebugBuffer = packetDebugBuffer;
        this.packetDebugBufferSize = packetDebugBufferSize;
    }

    public void setMaxPacketDumpLength(int maxPacketDumpLength) {
        this.maxPacketDumpLength = maxPacketDumpLength;
    }

    /**
     * Add a packet to the debug buffer.
     *
     * @param packet
     *            packet as bytes
     * @param packetLen
     *            packet length
     */
    private void pushPacketToDebugBuffer(byte[] packet, int packetLen) {
        int bytesToDump = Math.min(this.maxPacketDumpLength, packetLen);

        String packetPayload = StringUtils.dumpAsHex(packet, bytesToDump);

        StringBuilder packetDump = new StringBuilder(DEBUG_MSG_LEN + NativeConstants.HEADER_LENGTH + packetPayload.length());

        packetDump.append("Client ");
        packetDump.append(packet.toString());
        packetDump.append("--------------------> Server\n");
        packetDump.append("\nPacket payload:\n\n");
        packetDump.append(packetPayload);

        if (packetLen > this.maxPacketDumpLength) {
            packetDump.append("\nNote: Packet of " + packetLen + " bytes truncated to " + this.maxPacketDumpLength + " bytes.\n");
        }

        if (this.packetDebugBuffer.size() + 1 > this.packetDebugBufferSize.getValue()) {
            this.packetDebugBuffer.removeFirst();
        }

        this.packetDebugBuffer.addLast(packetDump);
    }

    @Override
    public void send(byte[] packet, int packetLen, byte packetSequence) throws IOException {
        pushPacketToDebugBuffer(packet, packetLen);
        this.packetSender.send(packet, packetLen, packetSequence);
    }

    @Override
    public MessageSender<NativePacketPayload> undecorateAll() {
        return this.packetSender.undecorateAll();
    }

    @Override
    public MessageSender<NativePacketPayload> undecorate() {
        return this.packetSender;
    }

}
