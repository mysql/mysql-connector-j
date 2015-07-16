/*
  Copyright (c) 2015, Oracle and/or its affiliates. All rights reserved.

  The MySQL Connector/J is licensed under the terms of the GPLv2
  <http://www.gnu.org/licenses/old-licenses/gpl-2.0.html>, like most MySQL Connectors.
  There are special exceptions to the terms and conditions of the GPLv2 as it is applied to
  this software, see the FLOSS License Exception
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
import java.nio.ByteOrder;
import java.nio.channels.AsynchronousSocketChannel;
import java.nio.channels.CompletionHandler;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.function.BiFunction;

import com.google.protobuf.CodedInputStream;
import com.google.protobuf.GeneratedMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Parser;

import static com.mysql.cj.mysqlx.protobuf.Mysqlx.Error;
import static com.mysql.cj.mysqlx.protobuf.Mysqlx.ServerMessages;
import static com.mysql.cj.mysqlx.protobuf.MysqlxNotice.Frame;
import com.mysql.cj.core.exceptions.AssertionFailedException;
import com.mysql.cj.core.exceptions.CJCommunicationsException;
import com.mysql.cj.core.exceptions.WrongArgumentException;
import com.mysql.cj.mysqlx.io.MessageConstants;
import com.mysql.cj.mysqlx.MysqlxError;

/**
 * Asynchronous message reader for the MySQL-X protobuf-encoded messages.
 */
public class AsyncMessageReader implements CompletionHandler<Integer, Void>, MessageReader {
    /**
     * Sink for messages that are read asynchonously from the socket.
     *
     * @return whether the listener is done receiving messages.
     */
    public static interface MessageListener extends BiFunction<Class<? extends GeneratedMessage>, GeneratedMessage, Boolean> {
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
    /** Synchronous wrapper for satisfying synchronous methods of interface. Will generally be used and is initialized eagerly. */
    private AsyncToSyncMessageReader syncMessageReader = new AsyncToSyncMessageReader(this);

    // // TODO: use this if we can initiate the next message read before parsing in the current thread (see note in readMessage())
    // private Semaphore semaphore = new Semaphore(0);

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
        this.messageListenerQueue.add(l);
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
        //System.err.println("Initiating read of message (size=" + this.messageSize + ", tag=" + ServerMessages.Type.valueOf(this.messageType) + ")");
        this.headerBuf.clear(); // clear for next message
        this.state = ReadingState.READING_MESSAGE;
        // TODO: re-use buffers if possible. Note that synchronization will be necessary to prevent overwriting re-used buffers while still being parsed by
        // previous read. Also the buffer will have to be managed accordingly so that "remaining" isn't longer than the message otherwise it may consume
        // data from the next header+message
        this.messageBuf = ByteBuffer.allocate(this.messageSize);
        synchronized (this) {
            this.channel.read(this.messageBuf, null, this);
            // try {
            //     this.semaphore.acquire();
            // } catch (InterruptedException ex) {
            //     ex.printStackTrace();
            //     // TODO: how to handle interrupts?
            // }
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
        dispatchMessage(messageClass, parseMessage(messageClass, buf));

        // we start the next message read BEFORE blocking up this thread with message parsing and delivery.

        // we should be able to do that ^ but the Invoker class may call us re-entrantly (c.f. mayInvokeDirect()) before we dispatch the method which causes
        // issues here. this happens when there are multiple messages in the same packet and the socket channel has immediate data
        // synchronized (this) {
        //     semaphore.release();
        // }
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

        if (this.currentMessageListener == null) {
            try {
                this.currentMessageListener = this.messageListenerQueue.take();
            } catch (InterruptedException ex) {
                // TODO: how to handle interrupts?
            }
        }

        // we repeatedly deliver messages to the current listener until he yields control and we move on to the next
        boolean currentListenerDone = this.currentMessageListener.apply(messageClass, message);
        if (currentListenerDone) {
            this.currentMessageListener = null;
        }
    }

    /**
     * Handler for "read completed" event. We check the state and handle the incoming data.
     */
    public void completed(Integer bytesRead, Void v) {
        if (bytesRead < 0) {
            // TODO: (is this correct?) channel closed? we can just "shutdown" this way
            return;
        }
        if (this.state == ReadingState.READING_HEADER) {
            readHeader();
        } else {
            readMessage();
        }
    }

    public void failed(Throwable exc, Void v) {
        // TODO: expose this to the caller
        System.err.println("Failed to read");
        exc.printStackTrace();
    }

    public Class<? extends GeneratedMessage> getNextMessageClass() {
        return this.syncMessageReader.getNextMessageClass();
    }

    public <T extends GeneratedMessage> T read(Class<T> expectedClass) {
        return this.syncMessageReader.read(expectedClass);
    }

    /**
     * A synchronous message reader on top of the async message reader. This implemention is not thread-safe.
     */
    private static class AsyncToSyncMessageReader implements MessageListener {
        private AsyncMessageReader asyncMessageReader;
        private Class<? extends GeneratedMessage> messageClass;
        private GeneratedMessage message;
        private Semaphore semaphore = new Semaphore(0);

        public AsyncToSyncMessageReader(AsyncMessageReader asyncMessageReader) {
            this.asyncMessageReader = asyncMessageReader;
        }

        /**
         * Synchronously read the new message in the stream.
         */
        private void next() {
            this.asyncMessageReader.pushMessageListener(this);
            try {
                // wait for the read to complete before returning to the caller (can't rely on object monitor here)
                semaphore.acquire();
            } catch (InterruptedException ex) {
                throw new CJCommunicationsException(ex);
            }

            // throw an error/exception if we *unexpectedly* received an Error message
            if (Error.class.equals(this.messageClass)) {
                MysqlxError err = new MysqlxError((Error) this.message);
                // clear state for next read
                this.messageClass = null;
                this.message = null;
                throw err;
            }
        }

        public Boolean apply(Class<? extends GeneratedMessage> messageClass, GeneratedMessage message) {
            if (this.message != null) {
                throw AssertionFailedException.shouldNotHappen("A new message was received before current has been consumed");
            }
            this.messageClass = messageClass;
            this.message = message;
            this.semaphore.release();
            return true;
        }

        public Class<? extends GeneratedMessage> getNextMessageClass() {
            if (this.message == null) {
                next();
            }
            return this.messageClass;
        }

        public <T extends GeneratedMessage> T read(Class<T> expectedClass) {
            if (this.message == null) {
                next();
            }

            // ensure that parsed message class matches incoming tag
            if (!this.messageClass.equals(expectedClass)) {
                throw new WrongArgumentException("Unexpected message class. Expected '" + expectedClass.getSimpleName() + "' but actually received '" +
                        this.messageClass.getSimpleName() + "'");
            }

            T result = (T) this.message;
            this.messageClass = null;
            this.message = null;
            return result;
        }
    }
}
