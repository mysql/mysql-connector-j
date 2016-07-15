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

package com.mysql.cj.mysqlx.io;

import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousSocketChannel;
import java.nio.channels.CompletionHandler;
import java.nio.channels.ReadPendingException;
import java.nio.channels.WritePendingException;
import java.util.LinkedList;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * A layer over {@link AsynchronousSocketChannel} that serializes all incoming write requests. This means we queue any incoming buffer and don't begin writing
 * it until the previous buffer has been written fully. All buffers are transmitted atomically with respect to the caller/callback.
 */
public class SerializingBufferWriter implements CompletionHandler<Long, Void> {

    protected AsynchronousSocketChannel channel;

    /**
     * Maintain a queue of pending writes.
     */
    private Queue<ByteBuffer> pendingWrites = new LinkedList<ByteBuffer>();

    /**
     * Map the byte buffer identity (System.identityHashCode(ByteBuffer)) to the completion handler for each buffer's write. Identity is used as ByteBuffer's
     * hashCode() method changes when the position within the buffer changes.
     */
    private Map<Integer, CompletionHandler<Long, Void>> bufToHandler = new ConcurrentHashMap<>();

    public SerializingBufferWriter(AsynchronousSocketChannel channel) {
        this.channel = channel;
    }

    /**
     * Initiate a write of the current pending buffers. This method can only be called when no other writes are in progress. This method should be called under
     * a mutex for this.pendingWrites to prevent concurrent writes to the channel.
     */
    private void initiateWrite() {
        try {
            ByteBuffer bufs[] = this.pendingWrites.toArray(new ByteBuffer[this.pendingWrites.size()]);
            this.channel.write(bufs, 0, this.pendingWrites.size(), 0L, TimeUnit.MILLISECONDS, null, this);
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
     */
    public void queueBuffer(ByteBuffer buf, CompletionHandler<Long, Void> callback) {
        if (callback != null) {
            this.bufToHandler.put(System.identityHashCode(buf), callback);
        }
        synchronized (this.pendingWrites) {
            this.pendingWrites.add(buf);
            // if there's no write in progress, we need to initiate a write of this buffer. otherwise the completion of the current write will do it
            if (this.pendingWrites.size() == 1) {
                initiateWrite();
            }
        }
    }

    /**
     * Completion handler for channel writes.
     */
    public void completed(Long bytesWritten, Void v) {
        // collect completed writes to notify after initiating the next write
        LinkedList<ByteBuffer> completedWrites = new LinkedList<>();
        synchronized (this.pendingWrites) {
            while (this.pendingWrites.peek() != null && !this.pendingWrites.peek().hasRemaining()) {
                completedWrites.add(this.pendingWrites.remove());
            }
            // notify handler(s) before initiating write to satisfy ordering guarantees
            completedWrites.stream().map(System::identityHashCode).map(this.bufToHandler::remove).filter(Objects::nonNull).forEach(l -> {
                // prevent exceptions in handler from blocking other notifications
                try {
                    l.completed(0L, null);
                } catch (Throwable ex) {
                    // presumably unexpected, notify so futures don't block
                    try {
                        l.failed(ex, null);
                    } catch (Throwable ex2) {
                        // nothing we can do here
                        ex2.printStackTrace();
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
        this.bufToHandler.values().forEach((CompletionHandler<Long, Void> l) -> {
            try {
                l.failed(t, null);
            } catch (Exception ex) {
            }
        });
        this.bufToHandler.clear();
        synchronized (this.pendingWrites) {
            this.pendingWrites.clear();
        }
    }

    /**
     * Allow overwriting the channel once the writer has been established. Required for SSL/TLS connections when the encryption doesn't start until we send the
     * capability flag to X Plugin.
     */
    public void setChannel(AsynchronousSocketChannel channel) {
        this.channel = channel;
    }
}
