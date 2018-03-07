/*
 * Copyright (c) 2015, 2018, Oracle and/or its affiliates. All rights reserved.
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

import static org.junit.Assert.assertEquals;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.SocketOption;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousSocketChannel;
import java.nio.channels.CompletionHandler;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import com.mysql.cj.conf.DefaultPropertySet;
import com.mysql.cj.conf.PropertySet;
import com.mysql.cj.exceptions.CJCommunicationsException;
import com.mysql.cj.exceptions.ExceptionInterceptor;
import com.mysql.cj.exceptions.FeatureNotAvailableException;
import com.mysql.cj.exceptions.SSLParamsException;
import com.mysql.cj.log.Log;
import com.mysql.cj.protocol.FullReadInputStream;
import com.mysql.cj.protocol.MessageListener;
import com.mysql.cj.protocol.NetworkResources;
import com.mysql.cj.protocol.ServerSession;
import com.mysql.cj.protocol.SocketConnection;
import com.mysql.cj.protocol.SocketFactory;
import com.mysql.cj.x.protobuf.Mysqlx.ServerMessages;

/**
 * Tests for {@link AsyncMessageReader}.
 */
public class AsyncMessageReaderTest {

    /**
     * Base implementation of a mock test channel. Provides facilities to manipulate the channel that the reader is using.
     */
    static class BaseTestChannel extends AsynchronousSocketChannel {
        protected BaseTestChannel() {
            super(null);
        }

        public boolean open = true;
        CompletionHandler<Integer, ?> readHandler;
        ByteBuffer readBuf;
        CompletionHandler<Integer, ?> writeHandler;

        public boolean isOpen() {
            return this.open;
        }

        public void close() {
            this.open = false;
        }

        @Override
        public Future<Integer> read(ByteBuffer dst) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Future<Integer> write(ByteBuffer src) {
            throw new UnsupportedOperationException();
        }

        @Override
        public <A> void read(ByteBuffer dst, long timeout, TimeUnit unit, A attachment, CompletionHandler<Integer, ? super A> handler) {
            this.readHandler = handler;
            this.readBuf = dst;
        }

        @Override
        public <A> void read(ByteBuffer[] dsts, int offset, int length, long timeout, TimeUnit unit, A attachment, CompletionHandler<Long, ? super A> handler) {
        }

        @Override
        public <A> void write(ByteBuffer src, long timeout, TimeUnit unit, A attachment, CompletionHandler<Integer, ? super A> handler) {
            this.writeHandler = handler;
        }

        @Override
        public <A> void write(ByteBuffer[] srcs, int offset, int length, long timeout, TimeUnit unit, A attachment,
                CompletionHandler<Long, ? super A> handler) {
        }

        public void completeRead(int bytes, byte[] data) {
            if (data != null) {
                this.readBuf.put(data);
            }
            this.readHandler.completed(bytes, null);
        }

        public void failRead() {
            this.readHandler.failed(new Exception("Forced failure"), null);
        }

        @Override
        public <T> T getOption(SocketOption<T> name) throws IOException {
            return null;
        }

        @Override
        public Set<SocketOption<?>> supportedOptions() {
            return null;
        }

        @Override
        public AsynchronousSocketChannel bind(SocketAddress local) throws IOException {
            return null;
        }

        @Override
        public <T> AsynchronousSocketChannel setOption(SocketOption<T> name, T value) throws IOException {
            return null;
        }

        @Override
        public AsynchronousSocketChannel shutdownInput() throws IOException {
            return null;
        }

        @Override
        public AsynchronousSocketChannel shutdownOutput() throws IOException {
            return null;
        }

        @Override
        public SocketAddress getRemoteAddress() throws IOException {
            return null;
        }

        @Override
        public <A> void connect(SocketAddress remote, A attachment, CompletionHandler<Void, ? super A> handler) {
        }

        @Override
        public Future<Void> connect(SocketAddress remote) {
            return null;
        }

        @Override
        public SocketAddress getLocalAddress() throws IOException {
            return null;
        }
    }

    static class BaseTestSocketConnection implements SocketConnection {

        AsynchronousSocketChannel channel;

        public BaseTestSocketConnection(AsynchronousSocketChannel channel) {
            this.channel = channel;
        }

        @Override
        public void connect(String host, int port, PropertySet propertySet, ExceptionInterceptor exceptionInterceptor, Log log, int loginTimeout) {
        }

        @Override
        public void performTlsHandshake(ServerSession serverSession) throws SSLParamsException, FeatureNotAvailableException, IOException {
            // TODO Auto-generated method stub

        }

        @Override
        public void forceClose() {
        }

        @Override
        public NetworkResources getNetworkResources() {
            return null;
        }

        @Override
        public String getHost() {
            return null;
        }

        @Override
        public int getPort() {
            return 0;
        }

        @Override
        public Socket getMysqlSocket() {
            return null;
        }

        @Override
        public FullReadInputStream getMysqlInput() {
            return null;
        }

        @Override
        public void setMysqlInput(InputStream mysqlInput) {
        }

        @Override
        public BufferedOutputStream getMysqlOutput() {
            return null;
        }

        @Override
        public boolean isSSLEstablished() {
            return false;
        }

        @Override
        public SocketFactory getSocketFactory() {
            return null;
        }

        @Override
        public void setSocketFactory(SocketFactory socketFactory) {
        }

        @Override
        public ExceptionInterceptor getExceptionInterceptor() {
            return null;
        }

        @Override
        public PropertySet getPropertySet() {
            return null;
        }

        @Override
        public AsynchronousSocketChannel getAsynchronousSocketChannel() {
            return this.channel;
        }

    }

    /**
     * Test that an operation does not hang due to waiting on a socket that is closed. This happens when the socket is closed after a message listener is
     * added. If the close message (read with size = -1) is not properly propagated to the SyncReader, the thread will hang waiting for data.
     *
     * Bug#22972057
     */
    @Test
    public void testBug22972057() {
        BaseTestChannel channel = new BaseTestChannel();
        BaseTestSocketConnection sc = new BaseTestSocketConnection(channel);
        AsyncMessageReader reader = new AsyncMessageReader(new DefaultPropertySet(), sc);
        reader.start();

        // close the socket after the read is pending
        new Thread(() -> {
            try {
                Thread.sleep(100);
            } catch (InterruptedException ex) {
                ex.printStackTrace();
            }
            // this is what we get from the socket when it's closed/RST
            channel.completeRead(-1, null);
        }).start();

        // interrupt this test thread if it does happen to hang
        Thread testThread = Thread.currentThread();
        Thread interruptThread = new Thread(() -> {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException ex) {
                return;
            }
            testThread.interrupt();
        });
        interruptThread.start();

        try {
            // block trying to read which should fail when the socket is later closed
            reader.readMessage(null, ServerMessages.Type.OK_VALUE);
        } catch (CJCommunicationsException ex) {
            assertEquals("Socket closed", ex.getMessage());
        } catch (IOException ioe) {
            throw new XProtocolError(ioe.getMessage(), ioe);
        } finally {
            // cancel the interrupt thread
            interruptThread.interrupt();
        }
    }

    /**
     * Same bug above exists for the "pending message" feature.
     *
     * Bug#22972057
     */
    @Test
    public void testBug22972057_getNextMessageClass() {
        BaseTestChannel channel = new BaseTestChannel();
        BaseTestSocketConnection sc = new BaseTestSocketConnection(channel);
        AsyncMessageReader reader = new AsyncMessageReader(new DefaultPropertySet(), sc);
        reader.start();

        // close the socket after the read is pending
        new Thread(() -> {
            try {
                Thread.sleep(100);
            } catch (InterruptedException ex) {
                ex.printStackTrace();
            }
            // this is what we get from the socket when it's closed/RST
            channel.completeRead(-1, null);
        }).start();

        // interrupt this test thread if it does happen to hang
        Thread testThread = Thread.currentThread();
        Thread interruptThread = new Thread(() -> {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException ex) {
                return;
            }
            testThread.interrupt();
        });
        interruptThread.start();

        try {
            // block trying to peek the pending message which should fail when the socket is later closed
            reader.readHeader();
        } catch (CJCommunicationsException ex) {
            assertEquals("Failed to peek pending message", ex.getMessage());
            assertEquals("Socket closed", ex.getCause().getMessage());
        } catch (IOException ioe) {
            throw new XProtocolError(ioe.getMessage(), ioe);
        } finally {
            // cancel the interrupt thread
            interruptThread.interrupt();
        }
    }

    /**
     * Make sure all entry points throw an error when the reader is closed.
     */
    @Test
    public void errorAfterClosed() {
        BaseTestChannel channel = new BaseTestChannel();
        BaseTestSocketConnection sc = new BaseTestSocketConnection(channel);
        AsyncMessageReader reader = new AsyncMessageReader(new DefaultPropertySet(), sc);
        reader.start();

        channel.close();

        try {
            reader.readHeader();
        } catch (CJCommunicationsException ex) {
            // expected
        } catch (IOException ioe) {
            throw new XProtocolError(ioe.getMessage(), ioe);
        }

        try {
            reader.readMessage(null, ServerMessages.Type.OK_VALUE);
        } catch (CJCommunicationsException ex) {
            // expected
        } catch (IOException ioe) {
            throw new XProtocolError(ioe.getMessage(), ioe);
        }

        try {
            reader.pushMessageListener(new MessageListener<XMessage>() {
                @Override
                public Boolean createFromMessage(XMessage message) {
                    return true;
                }
            });
        } catch (CJCommunicationsException ex) {
            // expected
        }
    }
}
