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
import java.nio.channels.AsynchronousCloseException;
import java.nio.channels.AsynchronousSocketChannel;
import java.nio.channels.CompletionHandler;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.BiFunction;
import java.util.function.Function;

import com.google.protobuf.CodedInputStream;
import com.google.protobuf.GeneratedMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Parser;

import com.mysql.cj.core.exceptions.AssertionFailedException;
import com.mysql.cj.core.exceptions.CJCommunicationsException;
import com.mysql.cj.core.exceptions.WrongArgumentException;
import com.mysql.cj.mysqlx.MysqlxError;
import com.mysql.cj.mysqlx.io.MessageConstants;
import com.mysql.cj.mysqlx.protobuf.Mysqlx.Error;
import com.mysql.cj.mysqlx.protobuf.Mysqlx.ServerMessages;
import com.mysql.cj.mysqlx.protobuf.MysqlxNotice.Frame;

/**
 * Asynchronous message reader for the MySQL-X protobuf-encoded messages.
 */
public class AsyncMessageReader implements CompletionHandler<Integer, Void>, MessageReader {
    /**
     * Sink for messages that are read asynchonously from the socket.
     *
     * @return whether the listener is done receiving messages.
     */
    @FunctionalInterface
    public static interface MessageListener extends BiFunction<Class<? extends GeneratedMessage>, GeneratedMessage, Boolean> {
        default void closed() {
        }

        default void error(Throwable ex) {
            ex.printStackTrace();
        }
    }

    /** Size of the message that will be read. */
    private int messageSize;
    /** Type tag of the message to read (indicates parser to use). */
    private int messageType;
    /** Static buffer to store the packet header. */
    private ByteBuffer headerBuf = ByteBuffer.allocate(5).order(ByteOrder.LITTLE_ENDIAN);
    /** Dynamic buffer to store the message body. */
    private ByteBuffer messageBuf;
    /** The channel that we operate on. */
    private AsynchronousSocketChannel channel;
    /**
     * The current <code>MessageListener</code>. This is set to <code>null</code> immediately following the listener's indicator that it is done reading
     * messages. It is set again when the next message is read and the next <code>MessageListener</code> is taken from the queue.
     */
    private MessageListener currentMessageListener;
    /** Queue of <code>MessageListener</code>s waiting to process messages. */
    private BlockingQueue<MessageListener> messageListenerQueue = new LinkedBlockingQueue<MessageListener>();

    private Class<? extends GeneratedMessage> pendingMsgClass;
    private Object pendingMsgMonitor = new Object();

    /** Possible state of reading messages. */
    private static enum ReadingState {
        /** Waiting to read the header. */
        READING_HEADER,
        /** Waiting to read the message body. */
        READING_MESSAGE
    };
    private ReadingState state;

    public AsyncMessageReader(AsynchronousSocketChannel channel) {
        this.channel = channel;
    }

    /**
     * Start the message reader on the provided channel.
     */
    public void start() {
        readHeader();
    }

    /**
     * Queue a {@link MessageListener} to receive messages.
     */
    public void pushMessageListener(MessageListener l) {
        if (!this.channel.isOpen()) {
            throw new CJCommunicationsException("async closed");
        }

        this.messageListenerQueue.add(l);
    }

    /**
     * Get the current or next {@link MessageListener}. This method works according to the following algorithm:
     * <ul>
     * <li>If there's a "current" {@link MessageListener} (indicated on last dispatch that it wanted more messages), it is returned.</li>
     * <li>If there's no current {@link MessageListener}, the queue is checked for the next one. If there's one in the queue, it is returned.</li>
     * <li>If there's no current and none in the queue, we either return <code>null</code> if <code>block</code> is <code>false</code> or wait for one to be put
     * in the queue if <code>block</code> is true.</li>
     * </ul>
     * <P>This method assigns to "current" the returned message listener.
     *
     * @param block whether to block waiting for a <code>MessageListener</code>
     * @return the new current <code>MessageListener</code>
     */
    private MessageListener getMessageListener(boolean block) {
        if (this.currentMessageListener == null) {
            if (block) {
                try {
                    this.currentMessageListener = this.messageListenerQueue.take();
                } catch (InterruptedException ex) {
                    // TODO: how to handle interrupts?
                }
            } else {
                this.currentMessageListener = this.messageListenerQueue.poll();
            }
        }
        return this.currentMessageListener;
    }

    /**
     * Consume the header data in {@link headerBuf} and initiate the reading of the message body.
     *
     * @note This method is the only place <i>state</i> is changed.
     */
    private void readHeader() {
        this.state = ReadingState.READING_HEADER;

        // loop again if we're still waiting for more data
        if (this.headerBuf.position() < 5) {
            this.channel.read(this.headerBuf, null, this);
            return;
        }

        // process the completed header and initiate message reading
        this.headerBuf.clear();
        this.messageSize = this.headerBuf.getInt() - 1;
        this.messageType = this.headerBuf.get();
        // for debugging
        // System.err.println("Initiating read of message (size=" + this.messageSize + ", tag=" + ServerMessages.Type.valueOf(this.messageType) + ")");
        this.headerBuf.clear(); // clear for next message
        this.state = ReadingState.READING_MESSAGE;
        // TODO: re-use buffers if possible. Note that synchronization will be necessary to prevent overwriting re-used buffers while still being parsed by
        // previous read. Also the buffer will have to be managed accordingly so that "remaining" isn't longer than the message otherwise it may consume
        // data from the next header+message
        this.messageBuf = ByteBuffer.allocate(this.messageSize);
        synchronized (this) {
            this.channel.read(this.messageBuf, null, this);
        }
    }

    /**
     * Read &amp; consume the message body and dispatch the message to the current/next {@link MessageListener}.
     */
    private void readMessage() {
        // loop again if we're still waiting for more data
        if (this.messageBuf.position() < this.messageSize) {
            this.channel.read(this.messageBuf, null, this);
            return;
        }

        // copy these before initiating the next read to prevent them being overwritten in another thread
        int type = this.messageType;
        ByteBuffer buf = this.messageBuf;
        this.messageType = 0;
        this.messageBuf = null;

        Class<? extends GeneratedMessage> messageClass = MessageConstants.MESSAGE_TYPE_TO_CLASS.get(type);
        if (messageClass == null) {
            // check if there's a mapping that we don't explicitly handle
            ServerMessages.Type serverMessageMapping = ServerMessages.Type.valueOf(type);
            throw AssertionFailedException.shouldNotHappen("Unknown message type: " + type + " (server messages mapping: " + serverMessageMapping + ")");
        }

        // dispatch the message to the listener before starting next read to ensure in-order delivery
        dispatchMessage(messageClass, parseMessage(messageClass, buf));
        readHeader();
    }

    /**
     * Parse a message.
     */
    private GeneratedMessage parseMessage(Class<? extends GeneratedMessage> messageClass, ByteBuffer buf) {
        try {
            Parser<? extends GeneratedMessage> parser = MessageConstants.MESSAGE_CLASS_TO_PARSER.get(messageClass);
            buf.clear(); // reset position
            return parser.parseFrom(CodedInputStream.newInstance(buf));
        } catch (InvalidProtocolBufferException ex) {
            throw AssertionFailedException.shouldNotHappen(ex);
        }
    }

    private void dispatchMessage(Class<? extends GeneratedMessage> messageClass, GeneratedMessage message) {
        if (messageClass == Frame.class && ((Frame) message).getScope() == Frame.Scope.GLOBAL) {
            throw new RuntimeException("TODO: implement me");
        }

        // if there's no message listener waiting, expose the message class as pending for the next read
        if (getMessageListener(false) == null) {
            synchronized (this.pendingMsgMonitor) {
                this.pendingMsgClass = messageClass;
                this.pendingMsgMonitor.notify();
            }
        }

        // we repeatedly deliver messages to the current listener until he yields control and we move on to the next
        boolean currentListenerDone = getMessageListener(true).apply(messageClass, message);
        synchronized (this.pendingMsgMonitor) {
            this.pendingMsgClass = null;
        }
        if (currentListenerDone) {
            this.currentMessageListener = null;
        }
    }

    /**
     * Handler for "read completed" event. We check the state and handle the incoming data.
     */
    public void completed(Integer bytesRead, Void v) {
        if (bytesRead < 0) {
            // async socket closed
            try {
                this.channel.close();
            } catch (IOException ex) {
                throw AssertionFailedException.shouldNotHappen(ex);
            } finally {
                if (this.currentMessageListener == null) {
                    this.currentMessageListener = this.messageListenerQueue.poll();
                }
                if (this.currentMessageListener != null) {
                    this.currentMessageListener.closed();
                }
                // it's "done" after sending a closed() or error() signal
                this.currentMessageListener = null;
            }
            return;
        }

        if (this.state == ReadingState.READING_HEADER) {
            readHeader();
        } else {
            readMessage();
        }
    }

    public void failed(Throwable exc, Void v) {
        if (getMessageListener(false) != null) {
            if (AsynchronousCloseException.class.equals(exc.getClass())) {
                this.currentMessageListener.closed();
            } else {
                this.currentMessageListener.error(exc);
            }
        }
        // it's "done" after sending a closed() or error() signal
        this.currentMessageListener = null;
    }

    public Class<? extends GeneratedMessage> getNextMessageClass() {
        synchronized (this.pendingMsgMonitor) {
            if (this.pendingMsgClass == null) {
                try {
                    this.pendingMsgMonitor.wait();
                } catch (InterruptedException ex) {
                    // shouldn't happen
                    ex.printStackTrace();
                }
            }
            return this.pendingMsgClass;
        }
    }

    public <T extends GeneratedMessage> T read(final Class<T> expectedClass) {
        SyncReader<T> r = new SyncReader<>(this, expectedClass);
        return r.read();
    }

    /**
     * Sychronously read a message and propagate any errors to the current thread.
     */
    private static final class SyncReader<T> implements MessageListener {
        private CompletableFuture<Function<BiFunction<Class<? extends GeneratedMessage>, GeneratedMessage, T>, T>> future = new CompletableFuture<>();
        private Class<T> expectedClass;

        public SyncReader(AsyncMessageReader rdr, Class<T> expectedClass) {
            this.expectedClass = expectedClass;
            rdr.pushMessageListener(this);
        }

        public Boolean apply(Class<? extends GeneratedMessage> msgClass, GeneratedMessage msg) {
            this.future.complete(c -> c.apply(msgClass, msg));
            return true; /* done reading? */
        }

        public void error(Throwable ex) {
            this.future.completeExceptionally(ex);
        }

        public T read() {
            try {
                return this.future.thenApply(f -> f.apply((msgClass, msg) -> {
                                    if (Error.class.equals(msgClass)) {
                                        throw new MysqlxError(Error.class.cast(msg));
                                    }
                                    // ensure that parsed message class matches incoming tag
                                    if (!msgClass.equals(expectedClass)) {
                                        throw new WrongArgumentException("Unexpected message class. Expected '" + expectedClass.getSimpleName() + "' but actually received '" +
                                                msgClass.getSimpleName() + "'");
                                    }
                                    return this.expectedClass.cast(msg);
                                })).get();
            } catch (ExecutionException ex) {
                if (MysqlxError.class.equals(ex.getCause().getClass())) {
                    // wrap the other thread's exception and include this thread's context
                    throw new MysqlxError((MysqlxError) ex.getCause());
                } else {
                    throw new CJCommunicationsException(ex);
                }
            } catch (InterruptedException ex) {
                throw new CJCommunicationsException(ex);
            }
        }
    }
}
