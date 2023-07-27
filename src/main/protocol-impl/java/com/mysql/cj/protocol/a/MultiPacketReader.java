/*
 * Copyright (c) 2016, 2023, Oracle and/or its affiliates.
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
import java.util.Optional;

import com.mysql.cj.Messages;
import com.mysql.cj.protocol.MessageReader;
import com.mysql.cj.protocol.a.NativeConstants.StringLengthDataType;

/**
 * A {@link MessageReader} which reads a full packet
 * built from sequence of it's on-wire parts.
 * See http://dev.mysql.com/doc/internals/en/sending-more-than-16mbyte.html
 */
public class MultiPacketReader implements MessageReader<NativePacketHeader, NativePacketPayload> {

    private MessageReader<NativePacketHeader, NativePacketPayload> packetReader;

    public MultiPacketReader(MessageReader<NativePacketHeader, NativePacketPayload> packetReader) {
        this.packetReader = packetReader;
    }

    @Override
    public NativePacketHeader readHeader() throws IOException {
        return this.packetReader.readHeader();
    }

    @Override
    public NativePacketHeader probeHeader() throws IOException {
        return this.packetReader.probeHeader();
    }

    @Override
    public NativePacketPayload readMessage(Optional<NativePacketPayload> reuse, NativePacketHeader header) throws IOException {
        int packetLength = header.getMessageSize();
        NativePacketPayload buf = this.packetReader.readMessage(reuse, header);

        if (packetLength == NativeConstants.MAX_PACKET_SIZE) { // it's a multi-packet

            buf.setPosition(NativeConstants.MAX_PACKET_SIZE);

            NativePacketPayload multiPacket = null;
            int multiPacketLength = -1;
            byte multiPacketSeq = getMessageSequence();

            do {
                NativePacketHeader hdr = readHeader();
                multiPacketLength = hdr.getMessageSize();

                if (multiPacket == null) {
                    multiPacket = new NativePacketPayload(multiPacketLength);
                }

                multiPacketSeq++;
                if (multiPacketSeq != hdr.getMessageSequence()) {
                    throw new IOException(Messages.getString("PacketReader.10"));
                }

                this.packetReader.readMessage(Optional.of(multiPacket), hdr);

                buf.writeBytes(StringLengthDataType.STRING_FIXED, multiPacket.getByteBuffer(), 0, multiPacketLength);

            } while (multiPacketLength == NativeConstants.MAX_PACKET_SIZE);

            buf.setPosition(0);
        }

        return buf;
    }

    @Override
    public NativePacketPayload probeMessage(Optional<NativePacketPayload> reuse, NativePacketHeader header) throws IOException {
        int packetLength = header.getMessageSize();
        NativePacketPayload buf = this.packetReader.probeMessage(reuse, header);

        if (packetLength == NativeConstants.MAX_PACKET_SIZE) { // it's a multi-packet

            buf.setPosition(NativeConstants.MAX_PACKET_SIZE);

            NativePacketPayload multiPacket = null;
            int multiPacketLength = -1;
            byte multiPacketSeq = getMessageSequence();

            do {
                NativePacketHeader hdr = readHeader();
                multiPacketLength = hdr.getMessageSize();

                if (multiPacket == null) {
                    multiPacket = new NativePacketPayload(multiPacketLength);
                }

                multiPacketSeq++;
                if (multiPacketSeq != hdr.getMessageSequence()) {
                    throw new IOException(Messages.getString("PacketReader.10"));
                }

                this.packetReader.probeMessage(Optional.of(multiPacket), hdr);

                buf.writeBytes(StringLengthDataType.STRING_FIXED, multiPacket.getByteBuffer(), 0, multiPacketLength);

            } while (multiPacketLength == NativeConstants.MAX_PACKET_SIZE);

            buf.setPosition(0);
        }

        return buf;
    }

    @Override
    public byte getMessageSequence() {
        return this.packetReader.getMessageSequence();
    }

    @Override
    public void resetMessageSequence() {
        this.packetReader.resetMessageSequence();
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
