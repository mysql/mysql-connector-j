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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousCloseException;
import java.nio.channels.CompletionHandler;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.BiFunction;
import java.util.function.Function;

import com.google.protobuf.CodedInputStream;
import com.google.protobuf.GeneratedMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Parser;
import com.mysql.cj.conf.PropertyDefinitions;
import com.mysql.cj.conf.PropertySet;
import com.mysql.cj.conf.ReadableProperty;
import com.mysql.cj.exceptions.AssertionFailedException;
import com.mysql.cj.exceptions.CJCommunicationsException;
import com.mysql.cj.exceptions.WrongArgumentException;
import com.mysql.cj.protocol.Message;
import com.mysql.cj.protocol.MessageHeader;
import com.mysql.cj.protocol.MessageListener;
import com.mysql.cj.protocol.MessageReader;
import com.mysql.cj.protocol.SocketConnection;
import com.mysql.cj.x.protobuf.Mysqlx.Error;
import com.mysql.cj.x.protobuf.Mysqlx.ServerMessages;
import com.mysql.cj.x.protobuf.MysqlxNotice.Frame;

/**
 * Asynchronous low-level message reader for X Protocol protobuf-encoded messages.
 * The <i>MessageReader</i> will generally be used in one of two ways (See note regarding exceptions for Error messages):
 * <ul>
 * <li>The next message type is known and it's an error to read any other type of message. The caller will generally call the reader like so:
 * 
 * <pre>
 * MessageType msg = reader.readPayload(null, 0, ServerMessages.Type.THE_TYPE_VALUE);
 * </pre>
 * 
 * </li>
 * <li>The next message type is not known and the caller must conditionally decide what to do based on the type of the next message. The {@link
 * #readHeader()} method supports this use case. The caller will generally call the reader like so:
 * 
 * <pre>
 * XMessageHeader header = reader.readHeader();
 * if (header.getMessageType() == ServerMessages.Type.TYPE_1_VALUE) {
 *     MessageType1 msg1 = reader.readPayload(null, 0, ServerMessages.Type.TYPE_1_VALUE);
 *     // do something with msg1
 * } else if (header.getMessageType() == ServerMessages.Type.TYPE_2_VALUE) {
 *     MessageType2 msg2 = reader.readPayload(null, 0, ServerMessages.Type.TYPE_2_VALUE);
 *     // do something with msg2
 * }
 * </pre>
 * 
 * </li>
 * </ul>
 * <p>
 * If the <i>MessageReader</i> encounters an <i>Error</i> message, it will throw a {@link XProtocolError} exception to indicate that an error was returned from
 * the server.
 * </p>
 * <p>
 * All external interaction should know about message <i>types</i> listed in com.mysql.cj.x.protobuf.Mysqlx.ServerMessages.
 * </p>
 * TODO: write about async usage
 */
public class AsyncMessageReader implements CompletionHandler<Integer, Void>, MessageReader<XMessageHeader, XMessage> {
    private XMessageHeader header;
    /** Dynamic buffer to store the message body. */
    private ByteBuffer messageBuf;
    private PropertySet propertySet;
    /** The channel that we operate on. */
    private SocketConnection sc;
    /**
     * The current <code>MessageListener</code>. This is set to <code>null</code> immediately following the listener's indicator that it is done reading
     * messages. It is set again when the next message is read and the next <code>MessageListener</code> is taken from the queue.
     */
    private MessageListener<XMessage> currentMessageListener;
    /** Queue of <code>MessageListener</code>s waiting to process messages. */
    private BlockingQueue<MessageListener<XMessage>> messageListenerQueue = new LinkedBlockingQueue<>();

    private CompletableFuture<XMessageHeader> pendingMsgHeader;
    /** Lock to protect the pending message. */
    private Object pendingMsgMonitor = new Object();
    /** Have we been signalled to stop after the next message? */
    private boolean stopAfterNextMessage = false;

    /** Possible state of reading messages. */
    private static enum ReadingState {
        /** Waiting to read the header. */
        READING_HEADER,
        /** Waiting to read the message body. */
        READING_MESSAGE
    };

    private ReadingState state;

    public AsyncMessageReader(PropertySet propertySet, SocketConnection socketConnection) {
        this.propertySet = propertySet;
        this.sc = socketConnection;
    }

    /**
     * Start the message reader on the provided channel.
     */
    public void start() {
        readMessageHeader();
    }

    /**
     * Signal to the reader that it should stop reading messages after delivering the next message.
     */
    public void stopAfterNextMessage() {
        this.stopAfterNextMessage = true;
    }

    /**
     * Queue a {@link MessageListener} to receive messages.
     * 
     * @param l
     *            {@link MessageListener}
     */
    public void pushMessageListener(MessageListener<XMessage> l) {
        if (!this.sc.getAsynchronousSocketChannel().isOpen()) {
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
     * <P>
     * This method assigns to "current" the returned message listener.
     *
     * @param block
     *            whether to block waiting for a <code>MessageListener</code>
     * @return the new current <code>MessageListener</code>
     */
    private MessageListener<XMessage> getMessageListener(boolean block) {
        if (this.currentMessageListener == null) {
            if (block) {
                try {
                    this.currentMessageListener = this.messageListenerQueue.take();
                } catch (InterruptedException ex) {
                    throw new CJCommunicationsException(ex);
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
     * This method is the only place <i>state</i> is changed.
     */
    private void readMessageHeader() {
        this.state = ReadingState.READING_HEADER;
        if (this.header == null) {
            this.header = new XMessageHeader();
        }

        if (this.header.getBuffer().position() < 5) {
            this.sc.getAsynchronousSocketChannel().read(this.header.getBuffer(), null, this);
            return; // loop to #completed() again if we're still waiting for more data
        }

        this.state = ReadingState.READING_MESSAGE;
        // TODO: re-use buffers if possible. Note that synchronization will be necessary to prevent overwriting re-used buffers while still being parsed by
        // previous read. Also the buffer will have to be managed accordingly so that "remaining" isn't longer than the message otherwise it may consume
        // data from the next header+message
        this.messageBuf = ByteBuffer.allocate(this.header.getMessageSize());
        readMessage();
    }

    /**
     * Read &amp; consume the message body and dispatch the message to the current/next {@link MessageListener}.
     */
    private void readMessage() {
        if (this.messageBuf.position() < this.header.getMessageSize()) {
            this.sc.getAsynchronousSocketChannel().read(this.messageBuf, null, this);
            return; // loop to #completed() again if we're still waiting for more data
        }

        // copy these before initiating the next read to prevent them being overwritten in another thread
        ByteBuffer buf = this.messageBuf;
        this.messageBuf = null;

        Class<? extends GeneratedMessage> messageClass = MessageConstants.getMessageClassForType(this.header.getMessageType());

        // Capture this flag value before dispatching the message, otherwise we risk having a different value when using it later on.
        boolean localStopAfterNextMessage = this.stopAfterNextMessage;

        // dispatch the message to the listener before starting next read to ensure in-order delivery
        buf.flip();
        dispatchMessage(this.header, parseMessage(messageClass, buf));

        // As this is where the read loop begins, we can escape it here if requested.
        // But we always read the next message if the current one is a notice.
        if (localStopAfterNextMessage && messageClass != Frame.class) {
            this.stopAfterNextMessage = false; // TODO it's a suspicious action, can we really change the global variable value here after we stated that it may be reset after dispatchMessage() ?
            this.header = null;
            return;
        }

        this.header = null;
        readMessageHeader();
    }

    /**
     * Parse a message.
     * 
     * @param messageClass
     *            class extending {@link GeneratedMessage}
     * @param buf
     *            message buffer
     * @return {@link GeneratedMessage}
     */
    private GeneratedMessage parseMessage(Class<? extends GeneratedMessage> messageClass, ByteBuffer buf) {
        try {
            Parser<? extends GeneratedMessage> parser = MessageConstants.MESSAGE_CLASS_TO_PARSER.get(messageClass);
            return parser.parseFrom(CodedInputStream.newInstance(buf));
        } catch (InvalidProtocolBufferException ex) {
            throw AssertionFailedException.shouldNotHappen(ex);
        }
    }

    /**
     * Dispatch a message to a listener or "peek-er" once it has been read and parsed.
     * 
     * @param messageClass
     *            class extending {@link GeneratedMessage}
     * @param message
     *            {@link GeneratedMessage}
     */
    private void dispatchMessage(XMessageHeader hdr, GeneratedMessage message) {
        if (message.getClass() == Frame.class && ((Frame) message).getScope() == Frame.Scope.GLOBAL) {
            // we don't yet have any global notifications defined.
            throw new RuntimeException("TODO: implement me");
        }

        // if there's no message listener waiting, expose the message class as pending for the next read
        if (getMessageListener(false) == null) {
            synchronized (this.pendingMsgMonitor) {
                this.pendingMsgHeader = CompletableFuture.completedFuture(hdr);
                this.pendingMsgMonitor.notify();
            }
        }

        getMessageListener(true);
        // we must ensure that the message has been delivered and the pending message is cleared atomically under the pending message lock. otherwise the
        // pending message may still be seen after the message has been delivered but before the pending message is cleared
        //
        // t1-nio-thread                                         | t2-user-thread
        // ------------------------------------------------------+------------------------------------------------------
        // pendingMsgClass exposed - no current listener         |
        //                                                       | listener added
        // getMessageListener(true) returns                      |
        // dispatchMessage(), in currentMessageListener.apply()  |
        //                                                       | getNextMessageClass(), pendingMsgClass != null
        //                                                       | pendingMsgClass returned, but already being delivered
        //                                                       |    in other thread
        // pendingMsgClass = null                                |
        //
        synchronized (this.pendingMsgMonitor) {
            // we repeatedly deliver messages to the current listener until he yields control and we move on to the next
            boolean currentListenerDone = this.currentMessageListener.createFromMessage(new XMessage(message));
            if (currentListenerDone) {
                this.currentMessageListener = null;
            }
            // clear this after the message is delivered
            this.pendingMsgHeader = null;
        }
    }

    /**
     * Handler for "read completed" event. We check the state and handle the incoming data.
     */
    public void completed(Integer bytesRead, Void v) {
        // async socket closed
        if (bytesRead < 0) {
            try {
                this.sc.getAsynchronousSocketChannel().close();
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
                // in case we have a getNextMessageClass() request pending
                synchronized (this.pendingMsgMonitor) {
                    this.pendingMsgHeader = new CompletableFuture<>();
                    this.pendingMsgHeader.completeExceptionally(new CJCommunicationsException("Socket closed"));
                    this.pendingMsgMonitor.notify();
                }
            }
            return;
        }

        try {
            if (this.state == ReadingState.READING_HEADER) {
                readMessageHeader();
            } else {
                readMessage();
            }
        } catch (Throwable t) {
            // error reading => illegal state, close connection
            try {
                this.sc.getAsynchronousSocketChannel().close();
            } catch (Exception ex) {
            }
            // notify all listeners of error
            if (this.currentMessageListener != null) {
                try {
                    this.currentMessageListener.error(t);
                } catch (Exception ex) {
                }
            }
            this.messageListenerQueue.forEach(l -> {
                try {
                    l.error(t);
                } catch (Exception ex) {
                }
            });
            // in case we have a getNextMessageClass() request pending
            synchronized (this.pendingMsgMonitor) {
                this.pendingMsgHeader = new CompletableFuture<>();
                this.pendingMsgHeader.completeExceptionally(t);
                this.pendingMsgMonitor.notify();
            }
            this.messageListenerQueue.clear();
        }
    }

    /**
     * Handler for "read failed" event.
     */
    public void failed(Throwable exc, Void v) {
        if (getMessageListener(false) != null) {
            // force any error to unblock pending message listener
            synchronized (this.pendingMsgMonitor) {
                this.pendingMsgMonitor.notify();
            }
            if (AsynchronousCloseException.class.equals(exc.getClass())) {
                this.currentMessageListener.closed();
            } else {
                this.currentMessageListener.error(exc);
            }
        }
        // it's "done" after sending a closed() or error() signal
        this.currentMessageListener = null;
    }

    /**
     * Peek into the pending message for it's class/type. This method blocks until a message is available. A message will not become available to peek until
     * there are no pending message listeners on this reader.
     *
     * @return the header of the next message to be delivered
     */
    @Override
    public XMessageHeader readHeader() throws IOException {
        XMessageHeader mh;

        synchronized (this.pendingMsgMonitor) {
            if (!this.sc.getAsynchronousSocketChannel().isOpen()) {
                throw new CJCommunicationsException("async closed");
            }

            while (this.pendingMsgHeader == null) {
                try {
                    this.pendingMsgMonitor.wait();
                } catch (InterruptedException ex) {
                    throw new CJCommunicationsException(ex);
                }
            }

            try {
                // get the message header before releasing the lock (when the future may be overwritten)
                mh = this.pendingMsgHeader.get();

            } catch (ExecutionException ex) {
                throw new CJCommunicationsException("Failed to peek pending message", ex.getCause());
            } catch (InterruptedException ex) {
                throw new CJCommunicationsException(ex);
            }
        }

        if (mh.getMessageType() == ServerMessages.Type.ERROR_VALUE) {
            // this will cause a the error to be read and thrown as an exception. this can't be called under the lock as it will block the message delivery
            readMessage(null, mh);
        }
        return mh;
    }

    /**
     * Synchronously read a message of the given type into to the given {@link Message} instance or into the new one if not present.
     * 
     * @param reuse
     *            {@link Message} object to reuse
     * @param hdr
     *            {@link MessageHeader} object
     * @return
     * @throws IOException
     * @throws CJCommunicationsException
     *             wrapping an {@link IOException} during read or parse
     * @throws XProtocolError
     *             if an <i>Error</i> message is received from the server
     */
    @Override
    public XMessage readMessage(Optional<XMessage> reuse, XMessageHeader hdr) throws IOException {
        Class<? extends GeneratedMessage> msgClass = MessageConstants.getMessageClassForType(hdr.getMessageType());
        return readSync(msgClass);
    }

    /**
     * Synchronously read a message of the given type into to the given {@link Message} instance or into the new one if not present.
     * 
     * @param reuse
     *            {@link Message} object to reuse
     * @param expectedType
     *            Expected type of message
     * @return
     * @throws IOException
     * @throws WrongArgumentException
     *             if the message is of a different type
     * @throws CJCommunicationsException
     *             wrapping an {@link IOException} during read or parse
     * @throws XProtocolError
     *             if an <i>Error</i> message is received from the server
     */
    @Override
    public XMessage readMessage(Optional<XMessage> reuse, int expectedType) throws IOException {
        Class<? extends GeneratedMessage> msgClass = MessageConstants.getMessageClassForType(expectedType);
        return readSync(msgClass);
    }

    private <T extends GeneratedMessage> XMessage readSync(final Class<T> expectedClass) {
        SyncReader<T> r = new SyncReader<>(this.propertySet, this, expectedClass);
        return new XMessage(r.read());
    }

    /**
     * Synchronously read a single message and propagate any errors to the current thread.
     */
    private static final class SyncReader<T> implements MessageListener<XMessage> {
        private CompletableFuture<Function<BiFunction<Class<? extends GeneratedMessage>, GeneratedMessage, T>, T>> future = new CompletableFuture<>();
        private Class<T> expectedClass;
        private ReadableProperty<Integer> asyncTimeout;

        public SyncReader(PropertySet propertySet, AsyncMessageReader rdr, Class<T> expectedClass) {
            this.asyncTimeout = propertySet.getIntegerReadableProperty(PropertyDefinitions.PNAME_asyncResponseTimeout);
            this.expectedClass = expectedClass;
            rdr.pushMessageListener(this);
        }

        @SuppressWarnings("unchecked")
        @Override
        public Boolean createFromMessage(XMessage msg) {
            return this.future.complete(c -> c.apply((Class<? extends GeneratedMessage>) msg.getMessage().getClass(), (GeneratedMessage) msg.getMessage()));
        }

        public void error(Throwable ex) {
            this.future.completeExceptionally(ex);
        }

        public void closed() {
            this.future.completeExceptionally(new CJCommunicationsException("Socket closed"));
        }

        /**
         * Read the message and transform any error to a {@link XProtocolError} and throw it as an exception.
         * 
         * @return message of type T
         */
        public T read() {
            try {
                return this.future.thenApply(f -> f.apply((msgClass, msg) -> {
                    if (Error.class.equals(msgClass)) {
                        throw new XProtocolError(Error.class.cast(msg));
                    }
                    // ensure that parsed message class matches incoming tag
                    if (!msgClass.equals(this.expectedClass)) {
                        throw new WrongArgumentException("Unexpected message class. Expected '" + this.expectedClass.getSimpleName()
                                + "' but actually received '" + msgClass.getSimpleName() + "'");
                    }
                    return this.expectedClass.cast(msg);
                })).get(this.asyncTimeout.getValue(), TimeUnit.SECONDS);
            } catch (ExecutionException ex) {
                if (XProtocolError.class.equals(ex.getCause().getClass())) {
                    // wrap the other thread's exception and include this thread's context
                    throw new XProtocolError((XProtocolError) ex.getCause());
                }
                throw new CJCommunicationsException(ex.getCause().getMessage(), ex.getCause());
            } catch (InterruptedException | TimeoutException ex) {
                throw new CJCommunicationsException(ex);
            }
        }
    }

}
