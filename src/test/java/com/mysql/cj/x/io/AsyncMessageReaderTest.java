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

import static org.junit.Assert.assertEquals;

import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousByteChannel;
import java.nio.channels.CompletionHandler;
import java.util.concurrent.Future;

import org.junit.Test;

import com.google.protobuf.GeneratedMessage;
import com.mysql.cj.api.x.io.MessageListener;
import com.mysql.cj.core.conf.DefaultPropertySet;
import com.mysql.cj.core.exceptions.CJCommunicationsException;
import com.mysql.cj.x.protobuf.Mysqlx.Ok;

/**
 * Tests for {@link AsyncMessageReader}.
 */
public class AsyncMessageReaderTest {

    /**
     * Base implementation of a mock test channel. Provides facilities to manipulate the channel that the reader is using.
     */
    static class BaseTestChannel implements AsynchronousByteChannel {
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

        public Future<Integer> read(ByteBuffer dst) {
            throw new UnsupportedOperationException();
        }

        public Future<Integer> write(ByteBuffer src) {
            throw new UnsupportedOperationException();
        }

        public <A> void read(ByteBuffer dst, A attachment, CompletionHandler<Integer, ? super A> handler) {
            this.readHandler = handler;
            this.readBuf = dst;
        }

        public <A> void write(ByteBuffer src, A attachment, CompletionHandler<Integer, ? super A> handler) {
            this.writeHandler = handler;
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
        AsyncMessageReader reader = new AsyncMessageReader(new DefaultPropertySet(), channel);
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
            reader.read(Ok.class);
        } catch (CJCommunicationsException ex) {
            assertEquals("Socket closed", ex.getMessage());
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
        AsyncMessageReader reader = new AsyncMessageReader(new DefaultPropertySet(), channel);
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
            reader.getNextMessageClass();
        } catch (CJCommunicationsException ex) {
            assertEquals("Failed to peek pending message", ex.getMessage());
            assertEquals("Socket closed", ex.getCause().getMessage());
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
        AsyncMessageReader reader = new AsyncMessageReader(new DefaultPropertySet(), channel);
        reader.start();

        channel.close();

        try {
            reader.getNextMessageClass();
        } catch (CJCommunicationsException ex) {
            // expected
        }

        try {
            reader.read(Ok.class);
        } catch (CJCommunicationsException ex) {
            // expected
        }

        try {
            reader.pushMessageListener(new MessageListener() {
                public Boolean apply(Class<? extends GeneratedMessage> msgClass, GeneratedMessage msg) {
                    return true;
                }
            });
        } catch (CJCommunicationsException ex) {
            // expected
        }
    }
}
