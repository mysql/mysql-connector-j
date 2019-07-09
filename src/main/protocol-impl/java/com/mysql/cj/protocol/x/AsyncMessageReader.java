/*
 * Copyright (c) 2015, 2019, Oracle and/or its affiliates. All rights reserved.
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
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.google.protobuf.CodedInputStream;
import com.google.protobuf.GeneratedMessageV3;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Parser;
import com.mysql.cj.conf.PropertyKey;
import com.mysql.cj.conf.PropertySet;
import com.mysql.cj.conf.RuntimeProperty;
import com.mysql.cj.exceptions.AssertionFailedException;
import com.mysql.cj.exceptions.CJCommunicationsException;
import com.mysql.cj.exceptions.WrongArgumentException;
import com.mysql.cj.protocol.MessageListener;
import com.mysql.cj.protocol.MessageReader;
import com.mysql.cj.protocol.SocketConnection;
import com.mysql.cj.x.protobuf.Mysqlx.Error;
import com.mysql.cj.x.protobuf.Mysqlx.ServerMessages;
import com.mysql.cj.x.protobuf.MysqlxNotice.Frame;

/**
 * Asynchronous low-level message reader for Protocol buffers encoded messages delivered by X Protocol.
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
public class AsyncMessageReader implements MessageReader<XMessageHeader, XMessage> {
    private static int READ_AHEAD_DEPTH = 10;

    CompletedRead currentReadResult;
    /** Dynamic buffer to store the message body. */
    ByteBuffer messageBuf;
    private PropertySet propertySet;
    /** The channel that we operate on. */
    SocketConnection sc;

    CompletionHandler<Integer, Void> headerCompletionHandler = new HeaderCompletionHandler();
    CompletionHandler<Integer, Void> messageCompletionHandler = new MessageCompletionHandler();

    RuntimeProperty<Integer> asyncTimeout;

    /**
     * The current <code>MessageListener</code>. This is set to <code>null</code> immediately following the listener's indicator that it is done reading
     * messages. It is set again when the next message is read and the next <code>MessageListener</code> is taken from the queue.
     */
    MessageListener<XMessage> currentMessageListener;
    /** Queue of <code>MessageListener</code>s waiting to process messages. */
    private BlockingQueue<MessageListener<XMessage>> messageListenerQueue = new LinkedBlockingQueue<>();

    BlockingQueue<CompletedRead> pendingCompletedReadQueue = new LinkedBlockingQueue<>(READ_AHEAD_DEPTH);

    CompletableFuture<XMessageHeader> pendingMsgHeader;
    /** Lock to protect the pending message. */
    Object pendingMsgMonitor = new Object();
    /** Have we been signalled to stop after the next message? */
    boolean stopAfterNextMessage = false;

    private static class CompletedRead {
        public XMessageHeader header = null;
        public GeneratedMessageV3 message = null;

        public CompletedRead() {
        }
    }

    public AsyncMessageReader(PropertySet propertySet, SocketConnection socketConnection) {
        this.propertySet = propertySet;
        this.sc = socketConnection;
        this.asyncTimeout = this.propertySet.getIntegerProperty(PropertyKey.xdevapiAsyncResponseTimeout);
    }

    public void start() {
        this.headerCompletionHandler.completed(0, null); // initiates header read cycle
    }

    public void stopAfterNextMessage() {
        this.stopAfterNextMessage = true;
    }

    private void checkClosed() {
        if (!this.sc.getAsynchronousSocketChannel().isOpen()) {
            throw new CJCommunicationsException("Socket closed");
        }
    }

    public void pushMessageListener(MessageListener<XMessage> l) {
        checkClosed();
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
    MessageListener<XMessage> getMessageListener(boolean block) {
        try {
            if (this.currentMessageListener == null) {
                this.currentMessageListener = block ? this.messageListenerQueue.take() : this.messageListenerQueue.poll();
            }
            return this.currentMessageListener;
        } catch (InterruptedException ex) {
            throw new CJCommunicationsException(ex);
        }
    }

    private class HeaderCompletionHandler implements CompletionHandler<Integer, Void> {

        public HeaderCompletionHandler() {
        }

        /**
         * Handler for "read completed" event. Consume the header data in header.headerBuf and initiate the reading of the message body.
         */
        @Override
        public void completed(Integer bytesRead, Void attachment) {
            if (bytesRead < 0) { // async socket closed
                onError(new CJCommunicationsException("Socket closed"));
                return;
            }

            try {
                if (AsyncMessageReader.this.currentReadResult == null) {
                    AsyncMessageReader.this.currentReadResult = new CompletedRead();
                    AsyncMessageReader.this.currentReadResult.header = new XMessageHeader();
                }

                if (AsyncMessageReader.this.currentReadResult.header.getBuffer().position() < 5) {
                    AsyncMessageReader.this.sc.getAsynchronousSocketChannel().read(AsyncMessageReader.this.currentReadResult.header.getBuffer(), null, this);
                    return; // loop to #completed() again if we're still waiting for more data
                }

                //AsyncMessageReader.this.state = ReadingState.READING_MESSAGE;
                // TODO: re-use buffers if possible. Note that synchronization will be necessary to prevent overwriting re-used buffers while still being parsed by
                // previous read. Also the buffer will have to be managed accordingly so that "remaining" isn't longer than the message otherwise it may consume
                // data from the next header+message
                AsyncMessageReader.this.messageBuf = ByteBuffer.allocate(AsyncMessageReader.this.currentReadResult.header.getMessageSize());
                // if there's no message listener waiting, expose the message class as pending for the next read
                if (getMessageListener(false) == null) {
                    synchronized (AsyncMessageReader.this.pendingMsgMonitor) {
                        AsyncMessageReader.this.pendingMsgHeader = CompletableFuture.completedFuture(AsyncMessageReader.this.currentReadResult.header);
                        AsyncMessageReader.this.pendingMsgMonitor.notify();
                    }
                }

                AsyncMessageReader.this.messageCompletionHandler.completed(0, null); // initiates message read cycle

            } catch (Throwable t) {
                onError(t); // error reading => illegal state, close connection
            }
        }

        /**
         * Handler for "read failed" event.
         */
        @Override
        public void failed(Throwable exc, Void attachment) {
            if (getMessageListener(false) != null) {
                // force any error to unblock pending message listener
                synchronized (AsyncMessageReader.this.pendingMsgMonitor) {
                    AsyncMessageReader.this.pendingMsgMonitor.notify();
                }
                if (AsynchronousCloseException.class.equals(exc.getClass())) {
                    AsyncMessageReader.this.currentMessageListener.error(new CJCommunicationsException("Socket closed", exc));
                } else {
                    AsyncMessageReader.this.currentMessageListener.error(exc);
                }
            }
            // it's "done" after sending a closed() or error() signal
            AsyncMessageReader.this.currentMessageListener = null;
        }

    }

    private class MessageCompletionHandler implements CompletionHandler<Integer, Void> {

        public MessageCompletionHandler() {
        }

        /**
         * Read and consume the message body and dispatch the message to the current/next {@link MessageListener}.
         */
        @Override
        public void completed(Integer bytesRead, Void attachment) {
            if (bytesRead < 0) { // async socket closed
                onError(new CJCommunicationsException("Socket closed"));
                return;
            }

            try {
                if (AsyncMessageReader.this.messageBuf.position() < AsyncMessageReader.this.currentReadResult.header.getMessageSize()) {
                    AsyncMessageReader.this.sc.getAsynchronousSocketChannel().read(AsyncMessageReader.this.messageBuf, null, this);
                    return; // loop to #completed() again if we're still waiting for more data
                }

                // copy these before initiating the next read to prevent them being overwritten in another thread
                ByteBuffer buf = AsyncMessageReader.this.messageBuf;
                AsyncMessageReader.this.messageBuf = null;

                Class<? extends GeneratedMessageV3> messageClass = MessageConstants
                        .getMessageClassForType(AsyncMessageReader.this.currentReadResult.header.getMessageType());

                // Capture this flag value before dispatching the message, otherwise we risk having a different value when using it later on.
                boolean localStopAfterNextMessage = AsyncMessageReader.this.stopAfterNextMessage;

                // dispatch the message to the listener before starting next read to ensure in-order delivery
                buf.flip();
                AsyncMessageReader.this.currentReadResult.message = parseMessage(messageClass, buf);
                AsyncMessageReader.this.pendingCompletedReadQueue.add(AsyncMessageReader.this.currentReadResult);
                AsyncMessageReader.this.currentReadResult = null;

                dispatchMessage();

                // As this is where the read loop begins, we can escape it here if requested.
                // But we always read the next message if the current one is a notice.
                if (localStopAfterNextMessage && messageClass != Frame.class) {
                    AsyncMessageReader.this.stopAfterNextMessage = false; // TODO it's a suspicious action, can we really change the global variable value here after we stated that it may be reset after dispatchMessage() ?
                    AsyncMessageReader.this.currentReadResult = null;
                    return;
                }

                AsyncMessageReader.this.headerCompletionHandler.completed(0, null); // initiates header read cycle

            } catch (Throwable t) {
                onError(t); // error reading => illegal state, close connection
            }
        }

        @Override
        public void failed(Throwable exc, Void attachment) {
            if (getMessageListener(false) != null) {
                // force any error to unblock pending message listener
                synchronized (AsyncMessageReader.this.pendingMsgMonitor) {
                    AsyncMessageReader.this.pendingMsgMonitor.notify();
                }
                if (AsynchronousCloseException.class.equals(exc.getClass())) {
                    AsyncMessageReader.this.currentMessageListener.error(new CJCommunicationsException("Socket closed", exc));
                } else {
                    AsyncMessageReader.this.currentMessageListener.error(exc);
                }
            }
            // it's "done" after sending a closed() or error() signal
            AsyncMessageReader.this.currentMessageListener = null;
        }

        /**
         * Parse a message.
         * 
         * @param messageClass
         *            class extending {@link GeneratedMessageV3}
         * @param buf
         *            message buffer
         * @return {@link GeneratedMessageV3}
         */
        private GeneratedMessageV3 parseMessage(Class<? extends GeneratedMessageV3> messageClass, ByteBuffer buf) {
            try {
                Parser<? extends GeneratedMessageV3> parser = MessageConstants.MESSAGE_CLASS_TO_PARSER.get(messageClass);
                return parser.parseFrom(CodedInputStream.newInstance(buf));
            } catch (InvalidProtocolBufferException ex) {
                throw AssertionFailedException.shouldNotHappen(ex);
            }
        }

    }

    /**
     * Dispatch a message to a listener or "peek-er" once it has been read and parsed.
     */
    void dispatchMessage() {

        if (this.pendingCompletedReadQueue.isEmpty()) {
            return;
        }

        if (getMessageListener(true) != null) {
            CompletedRead res;
            try {
                res = this.pendingCompletedReadQueue.take();
            } catch (InterruptedException e) {
                throw new CJCommunicationsException("Failed to peek pending message", e);
            }

            GeneratedMessageV3 message = res.message;

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
                // we repeatedly deliver messages to the current listener until it yields control and we move on to the next
                if (this.currentMessageListener.processMessage(new XMessage(message))) {
                    this.currentMessageListener = null;
                }
                // clear this after the message is delivered
                this.pendingMsgHeader = null;
            }
        }
    }

    void onError(Throwable t) {
        try {
            this.sc.getAsynchronousSocketChannel().close();
        } catch (Exception ex) {
            // ignore
        }

        // notify all listeners of error
        if (this.currentMessageListener != null) {
            try {
                this.currentMessageListener.error(t);
            } catch (Exception ex) {
            }
            this.currentMessageListener = null;
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
            checkClosed();

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

    @Override
    public XMessage readMessage(Optional<XMessage> reuse, XMessageHeader hdr) throws IOException {
        return readMessage(reuse, hdr.getMessageType());
    }

    @Override
    public XMessage readMessage(Optional<XMessage> reuse, int expectedType) throws IOException {
        Class<? extends GeneratedMessageV3> expectedClass = MessageConstants.getMessageClassForType(expectedType);

        CompletableFuture<XMessage> future = new CompletableFuture<>();
        SyncXMessageListener<? extends GeneratedMessageV3> r = new SyncXMessageListener<>(future, expectedClass);
        pushMessageListener(r);

        try {
            return future.get(this.asyncTimeout.getValue(), TimeUnit.SECONDS);
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

    /**
     * Synchronously read a single message and propagate any errors to the current thread.
     * 
     * @param <T>
     *            GeneratedMessage type
     */
    private static final class SyncXMessageListener<T extends GeneratedMessageV3> implements MessageListener<XMessage> {
        private CompletableFuture<XMessage> future;
        private Class<T> expectedClass;
        List<Notice> notices = null;

        public SyncXMessageListener(CompletableFuture<XMessage> future, Class<T> expectedClass) {
            this.future = future;
            this.expectedClass = expectedClass;
        }

        @SuppressWarnings("unchecked")
        @Override
        public boolean processMessage(XMessage msg) {
            Class<? extends GeneratedMessageV3> msgClass = (Class<? extends GeneratedMessageV3>) msg.getMessage().getClass();
            if (Error.class.equals(msgClass)) {
                this.future.completeExceptionally(new XProtocolError(Error.class.cast(msg.getMessage())));
                return true; // done reading
            } else if (this.expectedClass.equals(msgClass)) {
                this.future.complete(msg.addNotices(this.notices));
                this.notices = null;
                return true; // done reading
            } else if (Frame.class.equals(msgClass)) {
                if (this.notices == null) {
                    this.notices = new ArrayList<>();
                }
                this.notices.add(Notice.getInstance(msg));
                return false; // proceed with reading the next message
            }
            this.future.completeExceptionally(new WrongArgumentException("Unhandled msg class (" + msgClass + ") + msg=" + msg.getMessage()));
            return true; // done reading
        }

        public void error(Throwable ex) {
            this.future.completeExceptionally(ex);
        }
    }
}
