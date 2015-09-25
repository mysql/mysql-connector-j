/*
  Copyright (c) 2015, Oracle and/or its affiliates. All rights reserved.

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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.AsynchronousSocketChannel;
import java.nio.channels.CompletionHandler;
import java.util.LinkedList;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import com.google.protobuf.CodedOutputStream;
import com.google.protobuf.MessageLite;

import com.mysql.cj.core.exceptions.CJCommunicationsException;
import com.mysql.cj.core.exceptions.CJPacketTooBigException;
import com.mysql.cj.mysqlx.protobuf.Mysqlx.ClientMessages;

/**
 * Asynchronous message writer.
 */
public class AsyncMessageWriter implements CompletionHandler<Long, Void>, MessageWriter {
    /**
     * Header length of MySQL-X packet.
     */
    private static final int HEADER_LEN = 5;

    private AsynchronousSocketChannel channel;
    private int maxAllowedPacket = -1;
    /**
     * Maintain a queue of pending writes.
     */
    private Queue<ByteBuffer> pendingWrites = new LinkedList<ByteBuffer>();
    /**
     * Map the byte buffer identity (System.identityHashCode(ByteBuffer)) to the completion listener for each buffer's write. Identity is used as ByteBuffer's
     * hashCode() method changes when the position within the buffer changes.
     */
    private Map<Integer, SentListener> bufToListener = new ConcurrentHashMap<>();

    public AsyncMessageWriter(AsynchronousSocketChannel channel) {
        this.channel = channel;
    }

    /**
     * Interface used to notify a listener of completion of an async message write.
     */
    @FunctionalInterface
    public static interface SentListener {
        void completed();

        default void error(Throwable ex) {
            ex.printStackTrace();
        }
    }

    /**
     * Initiate a write of the current pending buffers. This method can only be called when no other writes are in progress. This method should be called under
     * a mutex for this.pendingWrites.
     */
    private void initiateWrite() {
        ByteBuffer bufs[] = this.pendingWrites.toArray(new ByteBuffer[this.pendingWrites.size()]); 
        this.channel.write(bufs, 0, this.pendingWrites.size(), 0L, TimeUnit.MILLISECONDS, null, this); 
    }

    /**
     * Queue a message to be written to the socket. This method uses a mutex on the buffer list to synchronize for the following cases:
     * <li>The buffer list becomes empty after we check and miss writing to the socket.</li>
     * <li>LinkedList is not thread-safe.</li>
     */
    private void queueMessage(ByteBuffer messageBuf, SentListener callback) {
        if (callback != null) {
            this.bufToListener.put(System.identityHashCode(messageBuf), callback);
        }
        synchronized (this.pendingWrites) {
            this.pendingWrites.add(messageBuf);
            // if there's no write in progress, we need to initiate a write of this message. otherwise the completion of the current write will do it
            if (this.pendingWrites.size() == 1) {
                initiateWrite();
            }
        }
    }

    /**
     * Synchronously write a message.
     */
    public void write(MessageLite msg) {
        CompletableFuture<Void> f = new CompletableFuture<>();
        // write a message asynchronously that will notify the future when complete
        writeAsync(msg, new SentListener() {
                public void completed() {
                    f.complete(null);
                }

                public void error(Throwable ex) {
                    f.completeExceptionally(ex);
                }
            });
        // wait on the future to return
        try {
            f.get();
        } catch (InterruptedException | ExecutionException ex) {
            throw new CJCommunicationsException("Failed to write message", ex);
        }
    }

    /**
     * Asynchronously write a message with a notification being delivered to <code>callback</code> upon completion of write of entire message.
     *
     * @param msg
     * @param callback an optional callback to receive notification of when the message is completely written
     */
    public void writeAsync(MessageLite msg, SentListener callback) {
        int type = MessageWriter.getTypeForMessageClass(msg.getClass());
        int size = msg.getSerializedSize();
        int payloadSize = size + 1;
        // we check maxAllowedPacket against payloadSize as that's considered the "packet size" (not including 4 byte size header)
        if (this.maxAllowedPacket > 0 && payloadSize > maxAllowedPacket) {
            throw new CJPacketTooBigException(size, this.maxAllowedPacket);
        }
        // for debugging
        // System.err.println("Initiating write of message (size=" + payloadSize + ", tag=" + ClientMessages.Type.valueOf(type) + ")");
        ByteBuffer messageBuf = ByteBuffer.allocate(HEADER_LEN + size).order(ByteOrder.LITTLE_ENDIAN).putInt(payloadSize);
        messageBuf.put((byte) type);
        try {
            // directly access the ByteBuffer's backing array as protobuf's CodedOutputStream.newInstance(ByteBuffer) is giving a stream that doesn't actually
            // write any data
            msg.writeTo(CodedOutputStream.newInstance(messageBuf.array(), HEADER_LEN, size + HEADER_LEN));
        } catch (IOException ex) {
            throw new CJCommunicationsException("Unable to write message", ex);
        }
        messageBuf.rewind();
        queueMessage(messageBuf, callback);
    }

    /**
     * Completion handler for channel writes.
     */
    public void completed(Long bytesWritten, Void v) {
        if (bytesWritten == 0) {
            throw new IllegalArgumentException("Shouldn't be 0");
        }
        // collect completed writes to notify after initiating the next write
        LinkedList<ByteBuffer> completedWrites = new LinkedList<>();
        synchronized (this.pendingWrites) {
            while (this.pendingWrites.peek() != null && !this.pendingWrites.peek().hasRemaining()) {
                completedWrites.add(this.pendingWrites.remove());
            }
            if (this.pendingWrites.size() > 0) {
                initiateWrite();
            }
        }
        // notify (possibly long-running) listener(s) after initiating write and releasing lock
        completedWrites.stream()
                .map(System::identityHashCode)
                .map(this.bufToListener::remove)
                .filter(Objects::nonNull)
                .forEach(SentListener::completed);
    }

    public void failed(Throwable exc, Void v) {
        // TODO what is the state of the channel on a failed write?
        // TODO should we notify all pending writes as failed?
        exc.printStackTrace();
    }

    public void setMaxAllowedPacket(int maxAllowedPacket) {
        this.maxAllowedPacket = maxAllowedPacket;
    }
}
