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

package com.mysql.cj.protocol;

import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousSocketChannel;
import java.nio.channels.CompletionHandler;
import java.nio.channels.ReadPendingException;
import java.nio.channels.WritePendingException;
import java.util.LinkedList;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.TimeUnit;

/**
 * A layer over {@link AsynchronousSocketChannel} that serializes all incoming write requests. This means we queue any incoming buffer and don't begin writing
 * it until the previous buffer has been written fully. All buffers are transmitted atomically with respect to the caller/callback.
 */
public class SerializingBufferWriter implements CompletionHandler<Long, Void> {

    // TODO make WRITES_AT_ONCE configurable
    private static int WRITES_AT_ONCE = 200; // Empirical value. Helps improving i/o rate for large number of concurrent asynchronous requests 

    protected AsynchronousSocketChannel channel;

    /**
     * Maintain a queue of pending writes.
     */
    private Queue<ByteBufferWrapper> pendingWrites = new LinkedList<>();

    /**
     * Keeps the link between ByteBuffer to be written and the CompletionHandler
     * object to be invoked for this one write operation.
     */
    private static class ByteBufferWrapper {
        private ByteBuffer buffer;
        private CompletionHandler<Long, Void> handler = null;

        ByteBufferWrapper(ByteBuffer buffer, CompletionHandler<Long, Void> completionHandler) {
            this.buffer = buffer;
            this.handler = completionHandler;
        }

        public ByteBuffer getBuffer() {
            return this.buffer;
        }

        public CompletionHandler<Long, Void> getHandler() {
            return this.handler;
        }
    }

    public SerializingBufferWriter(AsynchronousSocketChannel channel) {
        this.channel = channel;
    }

    /**
     * Initiate a write of the current pending buffers. This method can only be called when no other writes are in progress. This method should be called under
     * a mutex for this.pendingWrites to prevent concurrent writes to the channel.
     */
    private void initiateWrite() {
        try {
            // We must limit the number of buffers which may be sent at once with gathering write because of two reasons:
            // 1. Operating systems impose a limit on the number of buffers that may be used in an I/O operation, for example default Linux kernel value is 1024.
            // When the number of buffers exceeds this limit, then the I/O operation is performed with the maximum number of buffers allowed by the operating system.
            // That slows down the I/O significantly and could even hang it in case of asynchronous I/O when server response can't be read because write operation has drained all available buffers.
            // 2. With a large number of small asynchronous requests pendingWrites queue is filled much faster than it's freed so that the OS limit can be reached easily.
            ByteBuffer bufs[] = this.pendingWrites.stream().limit(WRITES_AT_ONCE).map(ByteBufferWrapper::getBuffer).toArray(size -> new ByteBuffer[size]);
            this.channel.write(bufs, 0, bufs.length, 0L, TimeUnit.MILLISECONDS, null, this);
        } catch (ReadPendingException | WritePendingException t) {
            return;
        } catch (Throwable t) {
            failed(t, null);
        }
    }

    /**
     * Queue a buffer to be written to the channel. This method uses a mutex on the buffer list to synchronize for the following cases:
     * <ul>
     * <li>The buffer list becomes empty after we check and miss writing to the channel.</li>
     * <li>LinkedList is not thread-safe.</li>
     * </ul>
     * 
     * @param buf
     *            {@link ByteBuffer}
     * @param callback
     *            {@link CompletionHandler}
     */
    public void queueBuffer(ByteBuffer buf, CompletionHandler<Long, Void> callback) {
        synchronized (this.pendingWrites) {
            this.pendingWrites.add(new ByteBufferWrapper(buf, callback));
            // if there's no write in progress, we need to initiate a write of this buffer. otherwise the completion of the current write will do it
            if (this.pendingWrites.size() == 1) {
                initiateWrite();
            }
        }
    }

    /**
     * Completion handler for channel writes.
     * 
     * @param bytesWritten
     *            number of processed bytes
     * @param v
     *            Void
     */
    public void completed(Long bytesWritten, Void v) {
        // collect completed writes to notify after initiating the next write
        LinkedList<CompletionHandler<Long, Void>> completedWrites = new LinkedList<>();
        synchronized (this.pendingWrites) {
            while (this.pendingWrites.peek() != null && !this.pendingWrites.peek().getBuffer().hasRemaining() && completedWrites.size() < WRITES_AT_ONCE) {
                completedWrites.add(this.pendingWrites.remove().getHandler());
            }
            // notify handler(s) before initiating write to satisfy ordering guarantees
            completedWrites.stream().filter(Objects::nonNull).forEach(l -> {
                // prevent exceptions in handler from blocking other notifications
                try {
                    l.completed(0L, null);
                } catch (Throwable ex) {
                    // presumably unexpected, notify so futures don't block
                    try {
                        l.failed(ex, null);
                    } catch (Throwable ex2) {
                        // nothing we can do here
                        ex2.printStackTrace(); // TODO log error normally instead of sysout
                    }
                }
            });
            if (this.pendingWrites.size() > 0) {
                initiateWrite();
            }
        }
    }

    public void failed(Throwable t, Void v) {
        // error writing, can't continue
        try {
            this.channel.close();
        } catch (Exception ex) {
        }

        LinkedList<CompletionHandler<Long, Void>> failedWrites = new LinkedList<>();
        synchronized (this.pendingWrites) {
            while (this.pendingWrites.peek() != null) {
                ByteBufferWrapper bw = this.pendingWrites.remove();
                if (bw.getHandler() != null) {
                    failedWrites.add(bw.getHandler());
                }
            }
        }

        failedWrites.forEach((CompletionHandler<Long, Void> l) -> {
            try {
                l.failed(t, null);
            } catch (Exception ex) {
            }
        });
        failedWrites.clear();
    }

    /**
     * Allow overwriting the channel once the writer has been established. Required for SSL/TLS connections when the encryption doesn't start until we send the
     * capability flag to X Plugin.
     * 
     * @param channel
     *            {@link AsynchronousSocketChannel}
     */
    public void setChannel(AsynchronousSocketChannel channel) {
        this.channel = channel;
    }
}
