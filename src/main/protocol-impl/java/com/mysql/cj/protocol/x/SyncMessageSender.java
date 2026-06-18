/*
 * Copyright (c) 2015, 2026, Oracle and/or its affiliates.
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

package com.mysql.cj.protocol.x;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.CompletionHandler;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

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

    private OutputStream outputStream;
    private long lastPacketSentTime = 0;
    private long previousPacketSentTime = 0;
    private int maxAllowedPacket = -1;

    /** Lock to protect async writes from sync ones. */
    final Lock syncOperationLock = new ReentrantLock();

    public SyncMessageSender(OutputStream os) {
        this.outputStream = os;
    }

    @Override
    public void send(XMessage message) {
        this.syncOperationLock.lock();
        try {
            MessageLite msg = message.getMessage();
            try {
                int type = MessageConstants.getTypeForMessageClass(msg.getClass());
                long frameLength = (long) XMessageHeader.MESSAGE_TYPE_LENGTH + msg.getSerializedSize();
                if (frameLength > Integer.MAX_VALUE) {
                    throw new CJPacketTooBigException(Messages.getString("PacketTooBigException.1", new Object[] { frameLength, Integer.MAX_VALUE }));
                }
                if (this.maxAllowedPacket > 0 && frameLength > this.maxAllowedPacket) {
                    throw new CJPacketTooBigException(Messages.getString("PacketTooBigException.1", new Object[] { frameLength, this.maxAllowedPacket }));
                }
                // for debugging
                // System.err.println("Initiating write of message (size=" + frameLength + ", tag=" + ClientMessages.Type.valueOf(type) + ")");
                byte[] sizeHeader = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN).putInt((int) frameLength).array();
                this.outputStream.write(sizeHeader);
                this.outputStream.write(type);
                msg.writeTo(this.outputStream);
                this.outputStream.flush();
                this.previousPacketSentTime = this.lastPacketSentTime;
                this.lastPacketSentTime = System.currentTimeMillis();
            } catch (IOException ex) {
                throw new CJCommunicationsException("Unable to write message", ex);
            }
        } finally {
            this.syncOperationLock.unlock();
        }
    }

    @Override
    public CompletableFuture<?> send(XMessage message, CompletableFuture<?> future, Runnable callback) {
        this.syncOperationLock.lock();
        try {
            CompletionHandler<Long, Void> resultHandler = new ErrorToFutureCompletionHandler<>(future, callback);
            MessageLite msg = message.getMessage();
            try {
                send(message);
                long result = XMessageHeader.HEADER_LENGTH + msg.getSerializedSize();
                resultHandler.completed(result, null);
            } catch (Throwable t) {
                resultHandler.failed(t, null);
            }
            return future;
        } finally {
            this.syncOperationLock.unlock();
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
