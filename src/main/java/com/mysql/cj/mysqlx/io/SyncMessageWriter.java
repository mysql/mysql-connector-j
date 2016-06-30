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

package com.mysql.cj.mysqlx.io;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import com.google.protobuf.MessageLite;
import com.mysql.cj.api.io.PacketSentTimeHolder;
import com.mysql.cj.core.Messages;
import com.mysql.cj.core.exceptions.CJCommunicationsException;
import com.mysql.cj.core.exceptions.CJPacketTooBigException;

/**
 * Synchronous-only implementation of {@link MessageWriter}.
 */
public class SyncMessageWriter implements MessageWriter, PacketSentTimeHolder {
    /**
     * Header length of X Protocol packet.
     */
    static final int HEADER_LEN = 5;

    private BufferedOutputStream outputStream;
    private long lastPacketSentTime = 0;
    private int maxAllowedPacket = -1;

    public SyncMessageWriter(BufferedOutputStream os) {
        this.outputStream = os;
    }

    /**
     * Send a message.
     *
     * @param msg
     *            the message to send
     * @throws CJCommunicationsException
     *             to wrap any occurring IOException
     */
    public void write(MessageLite msg) {
        try {
            int type = MessageWriter.getTypeForMessageClass(msg.getClass());
            int size = 1 + msg.getSerializedSize();
            if (this.maxAllowedPacket > 0 && size > this.maxAllowedPacket) {
                throw new CJPacketTooBigException(Messages.getString("PacketTooBigException.1", new Object[] { size, this.maxAllowedPacket }));
            }
            // for debugging
            // System.err.println("Initiating write of message (size=" + size + ", tag=" + ClientMessages.Type.valueOf(type) + ")");
            byte[] sizeHeader = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN).putInt(size).array();
            this.outputStream.write(sizeHeader);
            this.outputStream.write(type);
            msg.writeTo(this.outputStream);
            this.outputStream.flush();
            this.lastPacketSentTime = System.currentTimeMillis();
        } catch (IOException ex) {
            throw new CJCommunicationsException("Unable to write message", ex);
        }
    }

    public long getLastPacketSentTime() {
        return this.lastPacketSentTime;
    }

    public void setMaxAllowedPacket(int maxAllowedPacket) {
        this.maxAllowedPacket = maxAllowedPacket;
    }
}
