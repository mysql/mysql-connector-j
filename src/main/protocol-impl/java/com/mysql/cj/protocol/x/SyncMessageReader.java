/*
 * Copyright (c) 2015, 2024, Oracle and/or its affiliates.
 *
 * This program is free software; you can redistribute it and/or modify it under the terms of the GNU General Public License, version 2.0, as published by
 * the Free Software Foundation.
 *
 * This program is designed to work with certain software that is licensed under separate terms, as designated in a particular file or component or in
 * included license documentation. The authors of MySQL hereby grant you an additional permission to link the program and your derivative works with the
 * separately licensed software that they have either included with the program or referenced in the documentation.
 *
 * Without limiting anything contained in the foregoing, this file, which is part of MySQL Connector/J, is also subject to the Universal FOSS Exception,
 * version 1.0, a copy of which can be found at http://oss.oracle.com/licenses/universal-foss-exception.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License, version 2.0, for more details.
 *
 * You should have received a copy of the GNU General Public License along with this program; if not, write to the Free Software Foundation, Inc.,
 * 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA
 */

package com.mysql.cj.protocol.x;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.Parser;
import com.mysql.cj.exceptions.CJCommunicationsException;
import com.mysql.cj.exceptions.MysqlErrorNumbers;
import com.mysql.cj.exceptions.WrongArgumentException;
import com.mysql.cj.protocol.FullReadInputStream;
import com.mysql.cj.protocol.MessageListener;
import com.mysql.cj.protocol.MessageReader;
import com.mysql.cj.protocol.Protocol.ProtocolEventHandler;
import com.mysql.cj.protocol.Protocol.ProtocolEventListener.EventType;
import com.mysql.cj.protocol.x.Notice.XWarning;
import com.mysql.cj.x.protobuf.Mysqlx.Error;
import com.mysql.cj.x.protobuf.Mysqlx.ServerMessages;
import com.mysql.cj.x.protobuf.MysqlxNotice.Frame;

/**
 * Synchronous-only implementation of {@link MessageReader}. This implementation wraps a {@link java.io.InputStream}.
 */
public class SyncMessageReader implements MessageReader<XMessageHeader, XMessage> {

    /** Stream as a source of messages. */
    private FullReadInputStream inputStream;

    LinkedList<XMessageHeader> headersQueue = new LinkedList<>();
    LinkedList<Message> messagesQueue = new LinkedList<>();

    /** Queue of <code>MessageListener</code>s waiting to process messages. */
    BlockingQueue<MessageListener<XMessage>> messageListenerQueue = new LinkedBlockingQueue<>();

    /** Lock to protect the pending message. */
    final Lock dispatchingThreadLock = new ReentrantLock();
    /** Lock to protect async reads from sync ones. */
    final Lock syncOperationLock = new ReentrantLock();

    Thread dispatchingThread = null;

    private ProtocolEventHandler protocolEventHandler = null;

    public SyncMessageReader(FullReadInputStream inputStream, ProtocolEventHandler protocolEventHandler) {
        this.inputStream = inputStream;
        this.protocolEventHandler = protocolEventHandler;
    }

    @Override
    public XMessageHeader readHeader() throws IOException {
        // waiting for ListenersDispatcher completion to perform sync call
        this.syncOperationLock.lock();
        try {
            XMessageHeader header;
            if ((header = this.headersQueue.peek()) == null) {
                header = readHeaderLocal();
            }
            if (header.getMessageType() == ServerMessages.Type.ERROR_VALUE) {
                throw new XProtocolError(readMessageLocal(Error.class, true));
            }
            return header;
        } finally {
            this.syncOperationLock.unlock();
        }
    }

    public int getNextNonNoticeMessageType() throws IOException {
        this.syncOperationLock.lock();
        try {
            if (!this.headersQueue.isEmpty()) {
                for (XMessageHeader hdr : this.headersQueue) {
                    if (hdr.getMessageType() != ServerMessages.Type.NOTICE_VALUE) {
                        return hdr.getMessageType();
                    }
                }
            }

            XMessageHeader header;
            do {
                header = readHeaderLocal();
                if (header.getMessageType() == ServerMessages.Type.ERROR_VALUE) {
                    Error msg;
                    this.messagesQueue.addLast(msg = readMessageLocal(Error.class, false));
                    throw new XProtocolError(msg);

                } else if (header.getMessageType() == ServerMessages.Type.NOTICE_VALUE) {
                    this.messagesQueue.addLast(readMessageLocal(Frame.class, false));
                }
            } while (header.getMessageType() == ServerMessages.Type.NOTICE_VALUE);

            return header.getMessageType();
        } finally {
            this.syncOperationLock.unlock();
        }
    }

    private XMessageHeader readHeaderLocal() throws IOException {
        XMessageHeader header;
        try {
            /*
             * Note that the "header" per-se is the size of all data following the header. This currently includes the message type tag (1 byte) and the
             * message bytes. However since we know the type tag is present we also read it as part of the header. This may change in the future if session
             * multiplexing is supported by the protocol. The protocol will be able to accommodate it but we will have to separate reading data after the
             * header (size).
             */
            byte[] buf = new byte[5];
            this.inputStream.readFully(buf);
            header = new XMessageHeader(buf);
            this.headersQueue.add(header);
        } catch (IOException ex) {
            // TODO close socket?
            throw new CJCommunicationsException("Cannot read packet header", ex);
        }
        return header;
    }

    @SuppressWarnings("unchecked")
    private <T extends Message> T readMessageLocal(Class<T> messageClass, boolean fromQueue) {
        XMessageHeader header;
        if (fromQueue) {
            header = this.headersQueue.poll();
            T msg = (T) this.messagesQueue.poll();
            if (msg != null) {
                return msg;
            }
        } else {
            header = this.headersQueue.getLast();
        }

        Parser<T> parser = (Parser<T>) MessageConstants.MESSAGE_CLASS_TO_PARSER.get(messageClass);
        byte[] packet = new byte[header.getMessageSize()];

        try {
            this.inputStream.readFully(packet);
        } catch (IOException ex) {
            // TODO close socket?
            throw new CJCommunicationsException("Cannot read packet payload", ex);
        }

        try {
            T msg = parser.parseFrom(packet);

            if (msg instanceof Frame && ((Frame) msg).getType() == Frame.Type.WARNING_VALUE && ((Frame) msg).getScope() == Frame.Scope.GLOBAL) {
                XWarning w = new XWarning((Frame) msg);
                int code = (int) w.getCode();
                if (code == MysqlErrorNumbers.ER_SERVER_SHUTDOWN || code == MysqlErrorNumbers.ER_IO_READ_ERROR
                        || code == MysqlErrorNumbers.ER_SESSION_WAS_KILLED) {

                    CJCommunicationsException ex = new CJCommunicationsException(w.getMessage());
                    ex.setVendorCode(code);
                    if (this.protocolEventHandler != null) {
                        this.protocolEventHandler.invokeListeners(
                                code == MysqlErrorNumbers.ER_SERVER_SHUTDOWN ? EventType.SERVER_SHUTDOWN : EventType.SERVER_CLOSED_SESSION, ex);
                    }

                    throw ex;
                }
            }

            return msg;

        } catch (InvalidProtocolBufferException ex) {
            throw new WrongArgumentException(ex);
        }
    }

    @Override
    public XMessage readMessage(Optional<XMessage> reuse, XMessageHeader hdr) throws IOException {
        return readMessage(reuse, hdr.getMessageType());
    }

    @Override
    public XMessage readMessage(Optional<XMessage> reuse, int expectedType) throws IOException {
        // waiting for ListenersDispatcher completion to perform sync call
        this.syncOperationLock.lock();
        try {
            try {
                Class<? extends Message> expectedClass = MessageConstants.getMessageClassForType(expectedType);

                List<Notice> notices = null;
                XMessageHeader hdr;
                while ((hdr = readHeader()).getMessageType() == ServerMessages.Type.NOTICE_VALUE && expectedType != ServerMessages.Type.NOTICE_VALUE) {
                    if (notices == null) {
                        notices = new ArrayList<>();
                    }
                    notices.add(Notice
                            .getInstance(new XMessage(readMessageLocal(MessageConstants.getMessageClassForType(ServerMessages.Type.NOTICE_VALUE), true))));
                }

                Class<? extends Message> messageClass = MessageConstants.getMessageClassForType(hdr.getMessageType());
                // ensure that parsed message class matches incoming tag
                if (expectedClass != messageClass) {
                    throw new WrongArgumentException("Unexpected message class. Expected '" + expectedClass.getSimpleName() + "' but actually received '"
                            + messageClass.getSimpleName() + "'");
                }

                return new XMessage(readMessageLocal(messageClass, true)).addNotices(notices);
            } catch (IOException e) {
                throw new XProtocolError(e.getMessage(), e);
            }
        } finally {
            this.syncOperationLock.unlock();
        }
    }

    @Override
    public void pushMessageListener(final MessageListener<XMessage> listener) {
        try {
            this.messageListenerQueue.put(listener);
        } catch (InterruptedException e) {
            throw new CJCommunicationsException("Cannot queue message listener.", e);
        }

        this.dispatchingThreadLock.lock();
        try {
            if (this.dispatchingThread == null) {
                ListenersDispatcher ld = new ListenersDispatcher();
                this.dispatchingThread = new Thread(ld, "Message listeners dispatching thread");
                this.dispatchingThread.start();

                // We must ensure that ListenersDispatcher is really started before leaving the mutually exclusive block. Otherwise the following race condition
                // is possible: if next operation is executed synchronously it could consume results of the previous asynchronous operation.
                int millis = 5000; // TODO expose via properties ?
                while (!ld.started) {
                    try {
                        Thread.sleep(10);
                        millis = millis - 10;
                    } catch (InterruptedException e) {
                        throw new XProtocolError(e.getMessage(), e);
                    }
                    if (millis <= 0) {
                        throw new XProtocolError("Timeout for starting ListenersDispatcher exceeded.");
                    }
                }
            }
        } finally {
            this.dispatchingThreadLock.unlock();
        }
    }

    private class ListenersDispatcher implements Runnable {

        /**
         * The timeout value for queue.poll(timeout, unit) defining the time after which we close and unregister the dispatching thread.
         * On the other hand, a bigger timeout value allows us to keep dispatcher thread running while multiple concurrent asynchronous read operations are
         * pending, thus avoiding the delays for new dispatching threads creation.
         */
        private static final long POLL_TIMEOUT = 100; // TODO expose via connection property
        boolean started = false;

        public ListenersDispatcher() {
        }

        @Override
        public void run() {
            SyncMessageReader.this.syncOperationLock.lock();
            try {
                this.started = true;
                try {
                    while (true) {
                        MessageListener<XMessage> l;
                        if ((l = SyncMessageReader.this.messageListenerQueue.poll(POLL_TIMEOUT, TimeUnit.MILLISECONDS)) == null) {
                            SyncMessageReader.this.dispatchingThreadLock.lock();
                            try {
                                if (SyncMessageReader.this.messageListenerQueue.peek() == null) {
                                    SyncMessageReader.this.dispatchingThread = null;
                                    break;
                                }
                            } finally {
                                SyncMessageReader.this.dispatchingThreadLock.unlock();
                            }
                        } else {
                            try {
                                XMessage msg = null;
                                do {
                                    XMessageHeader hdr = readHeader();
                                    msg = readMessage(null, hdr);
                                } while (!l.processMessage(msg));
                            } catch (Throwable t) {
                                l.error(t);
                            }
                        }
                    }
                } catch (InterruptedException e) {
                    throw new CJCommunicationsException("Read operation interrupted.", e);
                }
            } finally {
                SyncMessageReader.this.syncOperationLock.unlock();
            }
        }

    }

}
