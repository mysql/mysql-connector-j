/*
 * Copyright (c) 2015, 2016, Oracle and/or its affiliates. All rights reserved.
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

package com.mysql.cj.mysqla.io;

import java.io.BufferedOutputStream;
import java.io.IOException;

import com.mysql.cj.api.mysqla.io.PacketSender;
import com.mysql.cj.mysqla.MysqlaUtils;

/**
 * Simple implementation of {@link PacketSender} which handles the transmission of logical MySQL packets to the provided output stream. Large packets will be
 * split into multiple chunks.
 */
public class SimplePacketSender implements PacketSender {
    private BufferedOutputStream outputStream;

    public SimplePacketSender(BufferedOutputStream outputStream) {
        this.outputStream = outputStream;
    }

    public void send(byte[] packet, int packetLen, byte packetSequence) throws IOException {
        PacketSplitter packetSplitter = new PacketSplitter(packetLen);
        while (packetSplitter.nextPacket()) {
            this.outputStream.write(MysqlaUtils.encodeMysqlThreeByteInteger(packetSplitter.getPacketLen()));
            this.outputStream.write(packetSequence++);
            this.outputStream.write(packet, packetSplitter.getOffset(), packetSplitter.getPacketLen());
        }
        this.outputStream.flush();
    }

    @Override
    public PacketSender undecorateAll() {
        return this;
    }

    @Override
    public PacketSender undecorate() {
        return this;
    }
}
