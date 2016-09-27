/*
  Copyright (c) 2016, Oracle and/or its affiliates. All rights reserved.

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

import java.io.IOException;
import java.util.Optional;

import com.mysql.cj.api.log.Log;
import com.mysql.cj.api.mysqla.io.PacketHeader;
import com.mysql.cj.api.mysqla.io.PacketPayload;
import com.mysql.cj.api.mysqla.io.PacketReader;
import com.mysql.cj.core.Messages;
import com.mysql.cj.core.util.StringUtils;
import com.mysql.cj.mysqla.MysqlaConstants;

/**
 * A decorating {@link PacketReader} which traces all received packets to the provided logger.
 */
public class TracingPacketReader implements PacketReader {

    /** Max number of bytes to dump when tracing the protocol */
    private final static int MAX_PACKET_DUMP_LENGTH = 1024;

    private PacketReader packetReader;
    private Log log;

    public TracingPacketReader(PacketReader packetReader, Log log) {
        this.packetReader = packetReader;
        this.log = log;
    }

    @Override
    public PacketHeader readHeader() throws IOException {
        PacketHeader hdr = this.packetReader.readHeader();

        StringBuilder traceMessageBuf = new StringBuilder();

        traceMessageBuf.append(Messages.getString("PacketReader.3"));
        traceMessageBuf.append(hdr.getPacketLength());
        traceMessageBuf.append(Messages.getString("PacketReader.4"));
        traceMessageBuf.append(StringUtils.dumpAsHex(hdr.getBuffer(), MysqlaConstants.HEADER_LENGTH));

        this.log.logTrace(traceMessageBuf.toString());

        return hdr;
    }

    @Override
    public PacketPayload readPayload(Optional<PacketPayload> reuse, int packetLength) throws IOException {
        PacketPayload buf = this.packetReader.readPayload(reuse, packetLength);

        StringBuilder traceMessageBuf = new StringBuilder();

        traceMessageBuf.append(Messages.getString(reuse.isPresent() ? "PacketReader.5" : "PacketReader.6"));
        traceMessageBuf.append(StringUtils.dumpAsHex(buf.getByteBuffer(), packetLength < MAX_PACKET_DUMP_LENGTH ? packetLength : MAX_PACKET_DUMP_LENGTH));

        if (packetLength > MAX_PACKET_DUMP_LENGTH) {
            traceMessageBuf.append(Messages.getString("PacketReader.7"));
            traceMessageBuf.append(MAX_PACKET_DUMP_LENGTH);
            traceMessageBuf.append(Messages.getString("PacketReader.8"));
        }

        this.log.logTrace(traceMessageBuf.toString());

        return buf;
    }

    @Override
    public byte getPacketSequence() {
        return this.packetReader.getPacketSequence();
    }

    @Override
    public void resetPacketSequence() {
        this.packetReader.resetPacketSequence();
    }

    @Override
    public PacketReader undecorateAll() {
        return this.packetReader.undecorateAll();
    }

    @Override
    public PacketReader undecorate() {
        return this.packetReader;
    }

}
