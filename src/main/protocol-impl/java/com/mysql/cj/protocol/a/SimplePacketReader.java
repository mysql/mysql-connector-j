/*
 * Copyright (c) 2016, 2024, Oracle and/or its affiliates.
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
import java.util.Optional;

import com.mysql.cj.Messages;
import com.mysql.cj.conf.RuntimeProperty;
import com.mysql.cj.exceptions.CJPacketTooBigException;
import com.mysql.cj.protocol.MessageReader;
import com.mysql.cj.protocol.SocketConnection;

/**
 * Simple implementation of {@link MessageReader} which handles the receiving of logical MySQL packets from the provided socket input stream.
 * Multi-packets are handled outside of this reader.
 */
public class SimplePacketReader implements MessageReader<NativePacketHeader, NativePacketPayload> {

    protected SocketConnection socketConnection;
    protected RuntimeProperty<Integer> maxAllowedPacket;

    private byte readPacketSequence = -1;

    NativePacketHeader lastHeader = null;
    NativePacketPayload lastMessage = null;

    public SimplePacketReader(SocketConnection socketConnection, RuntimeProperty<Integer> maxAllowedPacket) {
        this.socketConnection = socketConnection;
        this.maxAllowedPacket = maxAllowedPacket;
    }

    @Override
    public NativePacketHeader readHeader() throws IOException {
        if (this.lastHeader == null) {
            return readHeaderLocal();
        }
        NativePacketHeader hdr = this.lastHeader;
        this.lastHeader = null;
        this.readPacketSequence = hdr.getMessageSequence();
        return hdr;
    }

    @Override
    public NativePacketHeader probeHeader() throws IOException {
        this.lastHeader = readHeaderLocal();
        return this.lastHeader;
    }

    private NativePacketHeader readHeaderLocal() throws IOException {
        NativePacketHeader hdr = new NativePacketHeader();

        try {
            this.socketConnection.getMysqlInput().readFully(hdr.getBuffer().array(), 0, NativeConstants.HEADER_LENGTH);
            int packetLength = hdr.getMessageSize();
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

        this.readPacketSequence = hdr.getMessageSequence();
        return hdr;
    }

    @Override
    public NativePacketPayload readMessage(Optional<NativePacketPayload> reuse, NativePacketHeader header) throws IOException {
        if (this.lastMessage == null) {
            return readMessageLocal(reuse, header);
        }
        NativePacketPayload buf = this.lastMessage;
        this.lastMessage = null;
        return buf;
    }

    @Override
    public NativePacketPayload probeMessage(Optional<NativePacketPayload> reuse, NativePacketHeader header) throws IOException {
        this.lastMessage = readMessageLocal(reuse, header);
        return this.lastMessage;
    }

    private NativePacketPayload readMessageLocal(Optional<NativePacketPayload> reuse, NativePacketHeader header) throws IOException {
        try {
            int packetLength = header.getMessageSize();
            NativePacketPayload message;
            if (reuse.isPresent()) {
                message = reuse.get();
                // Set the Buffer to it's original state
                message.setPosition(0);
                // Do we need to re-alloc the byte buffer?
                if (message.getByteBuffer().length < packetLength) {
                    // Note: We actually check the length of the buffer, rather than getBufLength(), because getBufLength()
                    // is not necessarily the actual length of the byte array used as the buffer
                    message.setByteBuffer(new byte[packetLength]);
                }

                // Set the new length
                message.setPayloadLength(packetLength);
            } else {
                message = new NativePacketPayload(new byte[packetLength]);
            }

            // Read the data from the server
            int numBytesRead = this.socketConnection.getMysqlInput().readFully(message.getByteBuffer(), 0, packetLength);
            if (numBytesRead != packetLength) {
                throw new IOException(Messages.getString("PacketReader.1", new Object[] { packetLength, numBytesRead }));
            }
            return message;

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
    public byte getMessageSequence() {
        return this.readPacketSequence;
    }

    @Override
    public void resetMessageSequence() {
        this.readPacketSequence = 0;
    }

}
