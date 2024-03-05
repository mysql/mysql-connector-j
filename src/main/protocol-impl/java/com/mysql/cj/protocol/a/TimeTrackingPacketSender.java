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

import com.mysql.cj.protocol.MessageSender;
import com.mysql.cj.protocol.PacketSentTimeHolder;

/**
 * A {@link MessageSender} which tracks the last time a packet was sent.
 */
public class TimeTrackingPacketSender implements MessageSender<NativePacketPayload>, PacketSentTimeHolder {

    private MessageSender<NativePacketPayload> packetSender;
    private long lastPacketSentTime = 0;
    private long previousPacketSentTime = 0;

    public TimeTrackingPacketSender(MessageSender<NativePacketPayload> packetSender) {
        this.packetSender = packetSender;
    }

    @Override
    public void send(byte[] packet, int packetLen, byte packetSequence) throws IOException {
        this.packetSender.send(packet, packetLen, packetSequence);

        this.previousPacketSentTime = this.lastPacketSentTime;
        this.lastPacketSentTime = System.currentTimeMillis();
    }

    @Override
    public long getLastPacketSentTime() {
        return this.lastPacketSentTime;
    }

    @Override
    public long getPreviousPacketSentTime() {
        return this.previousPacketSentTime;
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
