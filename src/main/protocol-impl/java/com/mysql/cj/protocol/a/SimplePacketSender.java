/*
 * Copyright (c) 2015, 2023, Oracle and/or its affiliates.
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

import java.io.BufferedOutputStream;
import java.io.IOException;

import com.mysql.cj.protocol.MessageSender;

/**
 * Simple implementation of {@link MessageSender} which handles the transmission of logical MySQL packets to the provided output stream. Large packets will be
 * split into multiple chunks.
 */
public class SimplePacketSender implements MessageSender<NativePacketPayload> {

    private BufferedOutputStream outputStream;

    public SimplePacketSender(BufferedOutputStream outputStream) {
        this.outputStream = outputStream;
    }

    @Override
    public void send(byte[] packet, int packetLen, byte packetSequence) throws IOException {
        PacketSplitter packetSplitter = new PacketSplitter(packetLen);
        while (packetSplitter.nextPacket()) {
            this.outputStream.write(NativeUtils.encodeMysqlThreeByteInteger(packetSplitter.getPacketLen()));
            this.outputStream.write(packetSequence++);
            this.outputStream.write(packet, packetSplitter.getOffset(), packetSplitter.getPacketLen());
        }
        this.outputStream.flush();
    }

    @Override
    public MessageSender<NativePacketPayload> undecorateAll() {
        return this;
    }

    @Override
    public MessageSender<NativePacketPayload> undecorate() {
        return this;
    }

}
