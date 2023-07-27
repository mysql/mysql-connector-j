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

package com.mysql.cj.protocol.x;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.CompletionHandler;
import java.util.concurrent.CompletableFuture;

import com.google.protobuf.MessageLite;
import com.mysql.cj.Messages;
import com.mysql.cj.exceptions.CJCommunicationsException;
import com.mysql.cj.exceptions.CJPacketTooBigException;
import com.mysql.cj.protocol.MessageSender;
import com.mysql.cj.protocol.PacketSentTimeHolder;

/**
 * Synchronous-only implementation of {@link MessageSender}.
 */
public class SyncMessageSender implements MessageSender<XMessage>, PacketSentTimeHolder {

    /**
     * Header length of X Protocol packet.
     */
    static final int HEADER_LEN = 5;

    private OutputStream outputStream;
    private long lastPacketSentTime = 0;
    private long previousPacketSentTime = 0;
    private int maxAllowedPacket = -1;

    /** Lock to protect async writes from sync ones. */
    Object waitingAsyncOperationMonitor = new Object();

    public SyncMessageSender(OutputStream os) {
        this.outputStream = os;
    }

    @Override
    public void send(XMessage message) {
        synchronized (this.waitingAsyncOperationMonitor) {
            MessageLite msg = message.getMessage();
            try {
                int type = MessageConstants.getTypeForMessageClass(msg.getClass());
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
                this.previousPacketSentTime = this.lastPacketSentTime;
                this.lastPacketSentTime = System.currentTimeMillis();
            } catch (IOException ex) {
                throw new CJCommunicationsException("Unable to write message", ex);
            }
        }
    }

    @Override
    public CompletableFuture<?> send(XMessage message, CompletableFuture<?> future, Runnable callback) {
        synchronized (this.waitingAsyncOperationMonitor) {
            CompletionHandler<Long, Void> resultHandler = new ErrorToFutureCompletionHandler<>(future, callback);
            MessageLite msg = message.getMessage();
            try {
                send(message);
                long result = 4 + 1 + msg.getSerializedSize();
                resultHandler.completed(result, null);
            } catch (Throwable t) {
                resultHandler.failed(t, null);
            }
            return future;
        }
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
    public void setMaxAllowedPacket(int maxAllowedPacket) {
        this.maxAllowedPacket = maxAllowedPacket;
    }

}
