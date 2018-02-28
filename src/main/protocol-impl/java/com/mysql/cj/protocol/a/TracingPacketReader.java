/*
 * Copyright (c) 2016, 2018, Oracle and/or its affiliates. All rights reserved.
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
import com.mysql.cj.log.Log;
import com.mysql.cj.protocol.MessageReader;
import com.mysql.cj.util.StringUtils;

/**
 * A decorating {@link PacketReader} which traces all received packets to the provided logger.
 */
public class TracingPacketReader implements MessageReader<NativePacketHeader, NativePacketPayload> {

    /** Max number of bytes to dump when tracing the protocol */
    private final static int MAX_PACKET_DUMP_LENGTH = 1024;

    private MessageReader<NativePacketHeader, NativePacketPayload> packetReader;
    private Log log;

    public TracingPacketReader(MessageReader<NativePacketHeader, NativePacketPayload> packetReader, Log log) {
        this.packetReader = packetReader;
        this.log = log;
    }

    @Override
    public NativePacketHeader readHeader() throws IOException {
        NativePacketHeader hdr = this.packetReader.readHeader();

        StringBuilder traceMessageBuf = new StringBuilder();

        traceMessageBuf.append(Messages.getString("PacketReader.3"));
        traceMessageBuf.append(hdr.getMessageSize());
        traceMessageBuf.append(Messages.getString("PacketReader.4"));
        traceMessageBuf.append(StringUtils.dumpAsHex(hdr.getBuffer().array(), NativeConstants.HEADER_LENGTH));

        this.log.logTrace(traceMessageBuf.toString());

        return hdr;
    }

    @Override
    public NativePacketPayload readMessage(Optional<NativePacketPayload> reuse, NativePacketHeader header) throws IOException {
        int packetLength = header.getMessageSize();
        NativePacketPayload buf = this.packetReader.readMessage(reuse, header);

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
