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

import java.io.IOException;
import java.net.SocketAddress;
import java.net.SocketOption;
import java.nio.BufferOverflowException;
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

import com.mysql.cj.exceptions.AssertionFailedException;
import com.mysql.cj.exceptions.CJCommunicationsException;

/**
 * FilterInputStream-esque byte channel that decrypts incoming packets. We proxy calls to the read method from the caller. We replace the provided completion
 * handler with our own handler that decrypts the incoming message and an then delegates to the original handler.
 *
 * <p>
 * Note: This implementation does not support attachments for reads. They are not used in AsyncMessageReader which this class is in direct support
 * of.
 * </p>
 */
public class TlsAsynchronousSocketChannel extends AsynchronousSocketChannel implements CompletionHandler<Integer, Void> {
    private static final ByteBuffer emptyBuffer = ByteBuffer.allocate(0);
    /** The underlying input stream. */
    private AsynchronousSocketChannel channel;
    /** Encryption facility. */
    private SSLEngine sslEngine;
    /** Buffer for cipher text data. This is where reads from the underlying channel will be directed to. */
    private ByteBuffer cipherTextBuffer;
    /** Buffer for clear text data. This is where the SSLEngine will write the result of decrypting the cipher text buffer. */
    private ByteBuffer clearTextBuffer;
    /** Handler for the next buffer received. */
    private CompletionHandler<Integer, ?> handler;
    private ByteBuffer dst;

    /** Output stream. */
    private SerializingBufferWriter bufferWriter;
    /** Queue of buffers for re-use. */
    private LinkedBlockingQueue<ByteBuffer> cipherTextBuffers = new LinkedBlockingQueue<>();

    /**
     * Create a new decrypting input stream.
     *
     * @param in
     *            The underlying inputstream to read encrypted data from.
     * @param sslEngine
     *            A configured {@link SSLEngine} which has already completed the handshake.
     */
    public TlsAsynchronousSocketChannel(AsynchronousSocketChannel in, SSLEngine sslEngine) {
        super(null);
        this.sslEngine = sslEngine;

        this.channel = in;
        this.sslEngine = sslEngine;
        this.cipherTextBuffer = ByteBuffer.allocate(sslEngine.getSession().getPacketBufferSize());
        this.cipherTextBuffer.flip();
        this.clearTextBuffer = ByteBuffer.allocate(sslEngine.getSession().getApplicationBufferSize());
        this.clearTextBuffer.flip();

        this.bufferWriter = new SerializingBufferWriter(this.channel);
    }

    /**
     * Completion handler for a read. Prepare the buffer for decryption and continue with {@link #decryptAndDispatch()}.
     */
    public void completed(Integer result, Void attachment) {
        if (result < 0) {
            CompletionHandler<Integer, ?> h = this.handler;
            this.handler = null;
            h.completed(result, null);
            return;
        }
        this.cipherTextBuffer.flip();
        decryptAndDispatch();
    }

    public void failed(Throwable exc, Void attachment) {
        CompletionHandler<Integer, ?> h = this.handler;
        this.handler = null;
        h.failed(exc, null);
    }

    /**
     * Handle the read callback from the underlying stream. Modulo error handling, we do the following:
     * <ul>
     * <li>Attempt to decrypt the current cipher text buffer.</li>
     * <li>If successful, deliver as much as possible to the client's completion handler.</li>
     * <li>If not successful, we will need to read more data to accumulate enough to decrypt. Issue a new read request.</li>
     * </ul>
     */
    private synchronized void decryptAndDispatch() {
        try {
            this.clearTextBuffer.clear();
            SSLEngineResult res = this.sslEngine.unwrap(this.cipherTextBuffer, this.clearTextBuffer);
            switch (res.getStatus()) {
                case BUFFER_UNDERFLOW:
                    // Check if we need to enlarge the peer network packet buffer
                    final int newPeerNetDataSize = this.sslEngine.getSession().getPacketBufferSize();
                    if (newPeerNetDataSize > this.cipherTextBuffer.capacity()) {
                        // enlarge the peer network packet buffer
                        ByteBuffer newPeerNetData = ByteBuffer.allocate(newPeerNetDataSize);
                        newPeerNetData.put(this.cipherTextBuffer);
                        newPeerNetData.flip();
                        this.cipherTextBuffer = newPeerNetData;
                    } else {
                        this.cipherTextBuffer.compact();
                    }

                    // continue reading, not enough to decrypt yet
                    this.channel.read(this.cipherTextBuffer, null, this);
                    return;
                case BUFFER_OVERFLOW:
                    // not enough space in clearTextBuffer to decrypt packet. bug?
                    throw new BufferOverflowException();
                case OK:
                    this.clearTextBuffer.flip();
                    dispatchData();
                    break;
                case CLOSED:
                    this.handler.completed(-1, null);
            }
        } catch (Throwable ex) {
            failed(ex, null);
        }
    }

    /**
     * Main entry point from caller.
     */
    @Override
    public <A> void read(ByteBuffer dest, long timeout, TimeUnit unit, A attachment, CompletionHandler<Integer, ? super A> hdlr) {
        try {
            if (this.handler != null) {
                hdlr.completed(0, null);
            }
            this.handler = hdlr;
            this.dst = dest;
            if (this.clearTextBuffer.hasRemaining()) {
                // copy any remaining data directly to client
                dispatchData();
            } else if (this.cipherTextBuffer.hasRemaining()) {
                // otherwise, decrypt ciphertext data remaining from last time
                decryptAndDispatch();
            } else {
                // otherwise, issue a new read request
                this.cipherTextBuffer.clear();
                this.channel.read(this.cipherTextBuffer, null, this);
            }
        } catch (Throwable ex) {
            hdlr.failed(ex, null);
        }
    }

    @Override
    public <A> void read(ByteBuffer[] dsts, int offset, int length, long timeout, TimeUnit unit, A attachment, CompletionHandler<Long, ? super A> hdlr) {
        hdlr.failed(new UnsupportedOperationException(), null);
    }

    /**
     * Dispatch data to the caller's buffer and signal the completion handler. This represents the end of one completed read operation. The handler and
     * destination will be reset for the next request.
     */
    private synchronized void dispatchData() {
        int transferred = Math.min(this.dst.remaining(), this.clearTextBuffer.remaining());
        if (this.clearTextBuffer.remaining() > this.dst.remaining()) {
            // the ByteBuffer bulk copy only works if the src has <= remaining of the dst. narrow the view of src here to make use of i
            int newLimit = this.clearTextBuffer.position() + transferred;
            ByteBuffer src = this.clearTextBuffer.duplicate();
            src.limit(newLimit);
            this.dst.put(src);
            this.clearTextBuffer.position(this.clearTextBuffer.position() + transferred);
        } else {
            this.dst.put(this.clearTextBuffer);
        }
        // use a temporary to allow caller to initiate a new read in the callback
        CompletionHandler<Integer, ?> h = this.handler;
        this.handler = null;
        if (this.channel.isOpen()) {
            // If channel is still open then force the call through sun.nio.ch.Invoker to avoid deep levels of recursion.
            // If we directly call the handler, we may grow a huge stack when the caller only reads small portions of the buffer and issues a new read request.
            // The Invoker will dispatch the call on the thread pool for the AsynchronousSocketChannel
            this.channel.read(TlsAsynchronousSocketChannel.emptyBuffer, null, new CompletionHandler<Integer, Void>() {
                public void completed(Integer result, Void attachment) {
                    h.completed(transferred, null);
                }

                public void failed(Throwable t, Void attachment) {
                    // There should be no way to get here as the read on empty buf will immediately direct control to the `completed' method
                    t.printStackTrace(); // TODO log error normally instead of sysout
                    h.failed(AssertionFailedException.shouldNotHappen(new Exception(t)), null);
                }
            });
        } else {
            h.completed(transferred, null);
        }
    }

    public void close() throws IOException {
        this.channel.close();
    }

    public boolean isOpen() {
        return this.channel.isOpen();
    }

    /**
     * Unused. Should not be called.
     */
    @Override
    public Future<Integer> read(ByteBuffer dest) {
        throw new UnsupportedOperationException("This channel does not support direct reads");
    }

    /**
     * Unused. Should not be called.
     */
    @Override
    public Future<Integer> write(ByteBuffer src) {
        throw new UnsupportedOperationException("This channel does not support writes");
    }

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

    /**
     * Is the array of buffers drained? (I.e. are we done writing?)
     * 
     * @param buffers
     *            array of {@link ByteBuffer} objects
     * @return true if we're done
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
     * @param hdlr
     *            completion handler to be called when all buffers have been written
     */
    @Override
    public <A> void write(ByteBuffer[] srcs, int offset, int length, long timeout, TimeUnit unit, A attachment, CompletionHandler<Long, ? super A> hdlr) {
        try {
            long totalWriteSize = 0;
            while (true) {
                ByteBuffer cipherText = getCipherTextBuffer();
                SSLEngineResult res = this.sslEngine.wrap(srcs, offset, length, cipherText);
                if (res.getStatus() != Status.OK) {
                    hdlr.failed(new CJCommunicationsException("Unacceptable SSLEngine result: " + res), null);
                }
                totalWriteSize += res.bytesConsumed();
                cipherText.flip();
                if (isDrained(srcs)) {
                    // if we've encrypted all the source buffers, queue the last write
                    long finalTotal = totalWriteSize;
                    Runnable successHandler = () -> {
                        hdlr.completed(finalTotal, null);
                        putCipherTextBuffer(cipherText);
                    };
                    this.bufferWriter.queueBuffer(cipherText, new ErrorPropagatingCompletionHandler<Long>(hdlr, successHandler));
                    break;
                }
                // otherwise, only propagate errors
                this.bufferWriter.queueBuffer(cipherText, new ErrorPropagatingCompletionHandler<Long>(hdlr, () -> putCipherTextBuffer(cipherText)));
                continue;
            }
        } catch (SSLException ex) {
            hdlr.failed(new CJCommunicationsException(ex), null);
        } catch (Throwable ex) {
            hdlr.failed(ex, null);
        }
    }

    @Override
    public <A> void write(ByteBuffer src, long timeout, TimeUnit unit, A attachment, CompletionHandler<Integer, ? super A> hdlr) {
        hdlr.failed(new UnsupportedOperationException(), null);
    }

    /**
     * Acquire a new buffer to use as the destination and subsequent transmission of encrypted data.
     * 
     * @return {@link ByteBuffer}
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
     * 
     * @param buf
     *            {@link ByteBuffer}
     */
    private void putCipherTextBuffer(ByteBuffer buf) {
        if (this.cipherTextBuffers.size() < 10) {
            this.cipherTextBuffers.offer(buf);
        }
        // otherwise, we lose a reference and it get's GC'd
    }

    @Override
    public <T> T getOption(SocketOption<T> name) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Set<SocketOption<?>> supportedOptions() {
        throw new UnsupportedOperationException();
    }

    @Override
    public AsynchronousSocketChannel bind(SocketAddress local) throws IOException {
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
    public <A> void connect(SocketAddress remote, A attachment, CompletionHandler<Void, ? super A> hdlr) {
        hdlr.failed(new UnsupportedOperationException(), null);
    }

    @Override
    public Future<Void> connect(SocketAddress remote) {
        throw new UnsupportedOperationException();
    }

    @Override
    public SocketAddress getLocalAddress() throws IOException {
        return this.channel.getLocalAddress();
    }

}
