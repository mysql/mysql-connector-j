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

import java.io.IOException;
import java.net.SocketAddress;
import java.net.SocketOption;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousSocketChannel;
import java.nio.channels.CompletionHandler;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLEngineResult;
import javax.net.ssl.SSLEngineResult.Status;
import javax.net.ssl.SSLException;

import com.mysql.cj.core.exceptions.CJCommunicationsException;

/**
 * Byte channel that encrypts outgoing packets. We have to extend {@link AsynchronousSocketChannel} to get the gathering `write()' operation.
 */
public class TlsEncryptingByteChannel extends AsynchronousSocketChannel {
    /** Underlying channel. Only used to propagate `close()' et al. Otherwise wrapped by `bufferWriter'. */
    private AsynchronousSocketChannel channel;
    /** Output stream. */
    private SerializingBufferWriter bufferWriter;
    /** Encryption framework. */
    private SSLEngine sslEngine;
    /** Queue of buffers for re-use. */
    private LinkedBlockingQueue<ByteBuffer> cipherTextBuffers = new LinkedBlockingQueue<>();

    /**
     * Internal class used for easy propagation of error to the {@link CompletionHandler}.
     */
    private static class ErrorPropagatingCompletionHandler<V> implements CompletionHandler<V, Void> {
        private CompletionHandler<Long, ?> target;
        private Runnable success;

        public ErrorPropagatingCompletionHandler(CompletionHandler<Long, ?> target, Runnable success) {
            this.target = target;
            this.success = success;
        }

        public void completed(V result, Void attachment) {
            this.success.run();
        }

        public void failed(Throwable ex, Void attachment) {
            this.target.failed(ex, null);
        }
    }

    public TlsEncryptingByteChannel(AsynchronousSocketChannel channel, SSLEngine sslEngine) {
        super(null);
        this.channel = channel;
        this.bufferWriter = new SerializingBufferWriter(channel);
        this.sslEngine = sslEngine;
    }

    /**
     * Is the array of buffers drained? (I.e. are we done writing?)
     */
    private boolean isDrained(ByteBuffer[] buffers) {
        for (ByteBuffer b : buffers) {
            if (b.hasRemaining()) {
                return false;
            }
        }
        return true;
    }

    /**
     * Handle a request to write an array of buffers to the channel. We build one or more encrypted packets and write them to the server.
     *
     * @param srcs
     *            source buffers to write
     * @param offset
     *            offset into buffer array
     * @param length
     *            number of buffers
     * @param timeout
     *            ignored
     * @param unit
     *            ignored
     * @param attachment
     *            ignored
     * @param handler
     *            completion handler to be called when all buffers have been written
     */
    @Override
    public <A> void write(ByteBuffer[] srcs, int offset, int length, long timeout, TimeUnit unit, A attachment, CompletionHandler<Long, ? super A> handler) {
        try {
            long totalWriteSize = 0;
            while (true) {
                ByteBuffer cipherText = getCipherTextBuffer();
                SSLEngineResult res = this.sslEngine.wrap(srcs, offset, length, cipherText);
                if (res.getStatus() != Status.OK) {
                    handler.failed(new CJCommunicationsException("Unacceptable SSLEngine result: " + res), null);
                }
                totalWriteSize += res.bytesConsumed();
                cipherText.flip();
                if (isDrained(srcs)) {
                    // if we've encrypted all the source buffers, queue the last write
                    long finalTotal = totalWriteSize;
                    Runnable successHandler = () -> {
                        handler.completed(finalTotal, null);
                        putCipherTextBuffer(cipherText);
                    };
                    this.bufferWriter.queueBuffer(cipherText, new ErrorPropagatingCompletionHandler<Long>(handler, successHandler));
                    break;
                }
                // otherwise, only propagate errors
                this.bufferWriter.queueBuffer(cipherText, new ErrorPropagatingCompletionHandler<Long>(handler, () -> putCipherTextBuffer(cipherText)));
                continue;
            }
        } catch (SSLException ex) {
            handler.failed(new CJCommunicationsException(ex), null);
        } catch (Throwable ex) {
            handler.failed(ex, null);
        }
    }

    /**
     * Acquire a new buffer to use as the destination and subsequent transmission of encrypted data.
     */
    private ByteBuffer getCipherTextBuffer() {
        ByteBuffer buf = this.cipherTextBuffers.poll();
        if (buf == null) {
            return ByteBuffer.allocate(this.sslEngine.getSession().getPacketBufferSize());
        }
        buf.clear();
        return buf;
    }

    /**
     * Release a used buffer.
     */
    private void putCipherTextBuffer(ByteBuffer buf) {
        if (this.cipherTextBuffers.size() < 10) {
            this.cipherTextBuffers.offer(buf);
        }
        // otherwise, we lose a reference and it get's GC'd
    }

    @Override
    public AsynchronousSocketChannel bind(SocketAddress local) throws IOException {
        throw new UnsupportedOperationException();
    }

    public Set<SocketOption<?>> supportedOptions() {
        throw new UnsupportedOperationException();
    }

    public <T> T getOption(SocketOption<T> name) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> AsynchronousSocketChannel setOption(SocketOption<T> name, T value) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public AsynchronousSocketChannel shutdownInput() throws IOException {
        return this.channel.shutdownInput();
    }

    @Override
    public AsynchronousSocketChannel shutdownOutput() throws IOException {
        return this.channel.shutdownOutput();
    }

    @Override
    public SocketAddress getRemoteAddress() throws IOException {
        return this.channel.getRemoteAddress();
    }

    @Override
    public <A> void connect(SocketAddress remote, A attachment, CompletionHandler<Void, ? super A> handler) {
        handler.failed(new UnsupportedOperationException(), null);
    }

    @Override
    public Future<Void> connect(SocketAddress remote) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <A> void read(ByteBuffer dst, long timeout, TimeUnit unit, A attachment, CompletionHandler<Integer, ? super A> handler) {
        handler.failed(new UnsupportedOperationException(), null);
    }

    @Override
    public Future<Integer> read(ByteBuffer dst) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <A> void read(ByteBuffer[] dsts, int offset, int length, long timeout, TimeUnit unit, A attachment, CompletionHandler<Long, ? super A> handler) {
        handler.failed(new UnsupportedOperationException(), null);
    }

    @Override
    public <A> void write(ByteBuffer src, long timeout, TimeUnit unit, A attachment, CompletionHandler<Integer, ? super A> handler) {
        handler.failed(new UnsupportedOperationException(), null);
    }

    @Override
    public Future<Integer> write(ByteBuffer src) {
        throw new UnsupportedOperationException();
    }

    @Override
    public SocketAddress getLocalAddress() throws IOException {
        return this.channel.getLocalAddress();
    }

    public void close() throws IOException {
        this.channel.close();
    }

    public boolean isOpen() {
        return this.channel.isOpen();
    }
}
