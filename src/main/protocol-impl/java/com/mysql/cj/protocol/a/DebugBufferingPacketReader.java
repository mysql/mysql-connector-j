/*
 * Copyright (c) 2016, 2021, Oracle and/or its affiliates.
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

import java.io.IOException;
import java.util.LinkedList;
import java.util.Optional;

import com.mysql.cj.Messages;
import com.mysql.cj.conf.RuntimeProperty;
import com.mysql.cj.protocol.MessageReader;
import com.mysql.cj.util.StringUtils;

/**
 * A decorating {@link MessageReader} which put debugging info to a ring-buffer.
 */
public class DebugBufferingPacketReader implements MessageReader<NativePacketHeader, NativePacketPayload> {

    /** Max number of bytes to dump when tracing the protocol */
    private static final int MAX_PACKET_DUMP_LENGTH = 1024;
    private static final int DEBUG_MSG_LEN = 96;

    private MessageReader<NativePacketHeader, NativePacketPayload> packetReader;
    private LinkedList<StringBuilder> packetDebugBuffer;
    private RuntimeProperty<Integer> packetDebugBufferSize;
    private String lastHeaderPayload = "";

    private boolean packetSequenceReset = false;

    public DebugBufferingPacketReader(MessageReader<NativePacketHeader, NativePacketPayload> packetReader, LinkedList<StringBuilder> packetDebugBuffer,
            RuntimeProperty<Integer> packetDebugBufferSize) {
        this.packetReader = packetReader;
        this.packetDebugBuffer = packetDebugBuffer;
        this.packetDebugBufferSize = packetDebugBufferSize;
    }

    @Override
    public NativePacketHeader readHeader() throws IOException {
        byte prevPacketSeq = this.packetReader.getMessageSequence();
        return readHeaderLocal(prevPacketSeq, this.packetReader.readHeader());
    }

    @Override
    public NativePacketHeader probeHeader() throws IOException {
        byte prevPacketSeq = this.packetReader.getMessageSequence();
        return readHeaderLocal(prevPacketSeq, this.packetReader.probeHeader());
    }

    private NativePacketHeader readHeaderLocal(byte prevPacketSeq, NativePacketHeader hdr) throws IOException {
        // Normally we shouldn't get into situation of getting packets out of order from server,
        // so we do this check only in debug mode.
        byte currPacketSeq = hdr.getMessageSequence();
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

        this.lastHeaderPayload = StringUtils.dumpAsHex(hdr.getBuffer().array(), NativeConstants.HEADER_LENGTH);

        return hdr;
    }

    @Override
    public NativePacketPayload readMessage(Optional<NativePacketPayload> reuse, NativePacketHeader header) throws IOException {
        int packetLength = header.getMessageSize();
        NativePacketPayload buf = this.packetReader.readMessage(reuse, header);

        int bytesToDump = Math.min(MAX_PACKET_DUMP_LENGTH, packetLength);
        String PacketPayloadImpl = StringUtils.dumpAsHex(buf.getByteBuffer(), bytesToDump);

        StringBuilder packetDump = new StringBuilder(DEBUG_MSG_LEN + NativeConstants.HEADER_LENGTH + PacketPayloadImpl.length());
        packetDump.append("Server ");
        packetDump.append(reuse.isPresent() ? "(re-used) " : "(new) ");
        packetDump.append(buf.toString());
        packetDump.append(" --------------------> Client\n");
        packetDump.append("\nPacket payload:\n\n");
        packetDump.append(this.lastHeaderPayload);
        packetDump.append(PacketPayloadImpl);

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
    public NativePacketPayload probeMessage(Optional<NativePacketPayload> reuse, NativePacketHeader header) throws IOException {
        int packetLength = header.getMessageSize();
        NativePacketPayload buf = this.packetReader.probeMessage(reuse, header);

        int bytesToDump = Math.min(MAX_PACKET_DUMP_LENGTH, packetLength);
        String PacketPayloadImpl = StringUtils.dumpAsHex(buf.getByteBuffer(), bytesToDump);

        StringBuilder packetDump = new StringBuilder(DEBUG_MSG_LEN + NativeConstants.HEADER_LENGTH + PacketPayloadImpl.length());
        packetDump.append("Server ");
        packetDump.append(reuse.isPresent() ? "(re-used) " : "(new) ");
        packetDump.append(buf.toString());
        packetDump.append(" --------------------> Client\n");
        packetDump.append("\nPacket payload:\n\n");
        packetDump.append(this.lastHeaderPayload);
        packetDump.append(PacketPayloadImpl);

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
    public byte getMessageSequence() {
        return this.packetReader.getMessageSequence();
    }

    @Override
    public void resetMessageSequence() {
        this.packetReader.resetMessageSequence();
        this.packetSequenceReset = true;
    }

    @Override
    public MessageReader<NativePacketHeader, NativePacketPayload> undecorateAll() {
        return this.packetReader.undecorateAll();
    }

    @Override
    public MessageReader<NativePacketHeader, NativePacketPayload> undecorate() {
        return this.packetReader;
    }

}
