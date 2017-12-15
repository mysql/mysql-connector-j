/*
 * Copyright (c) 2015, 2017, Oracle and/or its affiliates. All rights reserved.
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

package com.mysql.cj.x.io;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.AsynchronousSocketChannel;
import java.nio.channels.CompletionHandler;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import com.google.protobuf.CodedOutputStream;
import com.google.protobuf.MessageLite;
import com.mysql.cj.api.x.io.MessageWriter;
import com.mysql.cj.core.Messages;
import com.mysql.cj.core.exceptions.CJCommunicationsException;
import com.mysql.cj.core.exceptions.CJPacketTooBigException;

/**
 * Asynchronous message writer.
 */
public class AsyncMessageWriter implements MessageWriter {
    /**
     * Header length of X Protocol packet.
     */
    private static final int HEADER_LEN = 5;

    private int maxAllowedPacket = -1;

    /**
     * Channel wrapper is the destination to which we write marshalled messages.
     */
    private SerializingBufferWriter bufferWriter;

    public AsyncMessageWriter(AsynchronousSocketChannel channel) {
        this.bufferWriter = new SerializingBufferWriter(channel);
    }

    /**
     * Synchronously write a message.
     */
    public void write(MessageLite msg) {
        CompletableFuture<Void> f = new CompletableFuture<>();
        // write a message asynchronously that will notify the future when complete
        writeAsync(msg, new ErrorToFutureCompletionHandler<Long>(f, () -> f.complete(null)));
        // wait on the future to return
        try {
            f.get();
        } catch (ExecutionException ex) {
            throw new CJCommunicationsException("Failed to write message", ex.getCause());
        } catch (InterruptedException ex) {
            throw new CJCommunicationsException("Failed to write message", ex);
        }
    }

    /**
     * Asynchronously write a message with a notification being delivered to <code>callback</code> upon completion of write of entire message.
     *
     * @param msg
     *            message extending {@link MessageLite}
     * @param callback
     *            an optional callback to receive notification of when the message is completely written
     */
    public void writeAsync(MessageLite msg, CompletionHandler<Long, Void> callback) {
        int type = MessageWriter.getTypeForMessageClass(msg.getClass());
        int size = msg.getSerializedSize();
        int payloadSize = size + 1;
        // we check maxAllowedPacket against payloadSize as that's considered the "packet size" (not including 4 byte size header)
        if (this.maxAllowedPacket > 0 && payloadSize > this.maxAllowedPacket) {
            throw new CJPacketTooBigException(Messages.getString("PacketTooBigException.1", new Object[] { size, this.maxAllowedPacket }));
        }
        // for debugging
        //System.err.println("Initiating write of message (size=" + payloadSize + ", tag=" + com.mysql.cj.mysqlx.protobuf.Mysqlx.ClientMessages.Type.valueOf(type) + ")");
        ByteBuffer messageBuf = ByteBuffer.allocate(HEADER_LEN + size).order(ByteOrder.LITTLE_ENDIAN).putInt(payloadSize);
        messageBuf.put((byte) type);
        try {
            // directly access the ByteBuffer's backing array as protobuf's CodedOutputStream.newInstance(ByteBuffer) is giving a stream that doesn't actually
            // write any data
            msg.writeTo(CodedOutputStream.newInstance(messageBuf.array(), HEADER_LEN, size + HEADER_LEN));
            messageBuf.position(messageBuf.limit());
        } catch (IOException ex) {
            throw new CJCommunicationsException("Unable to write message", ex);
        }
        messageBuf.flip();
        this.bufferWriter.queueBuffer(messageBuf, callback);
    }

    public void setMaxAllowedPacket(int maxAllowedPacket) {
        this.maxAllowedPacket = maxAllowedPacket;
    }

    /**
     * Allow overwriting the channel once the writer has been established. Required for SSL/TLS connections when the encryption doesn't start until we send the
     * capability flag to X Plugin.
     * 
     * @param channel
     *            {@link AsynchronousSocketChannel}
     */
    public void setChannel(AsynchronousSocketChannel channel) {
        this.bufferWriter.setChannel(channel);
    }
}
