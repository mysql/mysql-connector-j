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
