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

import java.io.IOException;

import com.mysql.cj.api.log.Log;
import com.mysql.cj.api.mysqla.io.PacketSender;
import com.mysql.cj.core.util.StringUtils;

/**
 * A decorating {@link PacketSender} which traces all sent packets to the provided logger.
 */
public class TracingPacketSender implements PacketSender {
    private PacketSender packetSender;
    private String host;
    private long serverThreadId;
    private Log log;

    public TracingPacketSender(PacketSender packetSender, Log log, String host, long serverThreadId) {
        this.packetSender = packetSender;
        this.host = host;
        this.serverThreadId = serverThreadId;
        this.log = log;
    }

    public void setServerThreadId(long serverThreadId) {
        this.serverThreadId = serverThreadId;
    }

    /**
     * Log the packet details to the provided logger.
     */
    private void logPacket(byte[] packet, int packetLen, byte packetSequence) {
        StringBuilder traceMessageBuf = new StringBuilder();

        traceMessageBuf.append("send packet payload:\n");
        traceMessageBuf.append("host: '");
        traceMessageBuf.append(this.host);
        traceMessageBuf.append("' serverThreadId: '");
        traceMessageBuf.append(this.serverThreadId);
        traceMessageBuf.append("' packetLen: '");
        traceMessageBuf.append(packetLen);
        traceMessageBuf.append("' packetSequence: '");
        traceMessageBuf.append(packetSequence);
        traceMessageBuf.append("'\n");
        traceMessageBuf.append(StringUtils.dumpAsHex(packet, packetLen));

        this.log.logTrace(traceMessageBuf.toString());
    }

    public void send(byte[] packet, int packetLen, byte packetSequence) throws IOException {
        logPacket(packet, packetLen, packetSequence);

        this.packetSender.send(packet, packetLen, packetSequence);
    }

    @Override
    public PacketSender undecorateAll() {
        return this.packetSender.undecorateAll();
    }

    @Override
    public PacketSender undecorate() {
        return this.packetSender;
    }
}
