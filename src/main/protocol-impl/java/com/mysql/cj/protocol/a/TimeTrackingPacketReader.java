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

import com.mysql.cj.protocol.MessageReader;
import com.mysql.cj.protocol.PacketReceivedTimeHolder;

/**
 * A {@link MessageReader} which tracks the last time a packet was received.
 */
public class TimeTrackingPacketReader implements MessageReader<NativePacketHeader, NativePacketPayload>, PacketReceivedTimeHolder {

    private MessageReader<NativePacketHeader, NativePacketPayload> packetReader;
    private long lastPacketReceivedTimeMs = 0;

    public TimeTrackingPacketReader(MessageReader<NativePacketHeader, NativePacketPayload> messageReader) {
        this.packetReader = messageReader;
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
        NativePacketPayload buf = this.packetReader.readMessage(reuse, header);
        this.lastPacketReceivedTimeMs = System.currentTimeMillis();
        return buf;
    }

    @Override
    public NativePacketPayload probeMessage(Optional<NativePacketPayload> reuse, NativePacketHeader header) throws IOException {
        NativePacketPayload buf = this.packetReader.probeMessage(reuse, header);
        this.lastPacketReceivedTimeMs = System.currentTimeMillis();
        return buf;
    }

    @Override
    public long getLastPacketReceivedTime() {
        return this.lastPacketReceivedTimeMs;
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
