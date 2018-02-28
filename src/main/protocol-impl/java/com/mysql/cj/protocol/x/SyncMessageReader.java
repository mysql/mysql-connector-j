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
import java.util.Optional;

import com.google.protobuf.GeneratedMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Parser;
import com.mysql.cj.exceptions.CJCommunicationsException;
import com.mysql.cj.exceptions.WrongArgumentException;
import com.mysql.cj.protocol.FullReadInputStream;
import com.mysql.cj.protocol.MessageReader;
import com.mysql.cj.x.protobuf.Mysqlx.Error;

/**
 * Synchronous-only implementation of {@link MessageReader}. This implementation wraps an {@link java.io.InputStream}.
 */
public class SyncMessageReader implements MessageReader<XMessageHeader, XMessage> {
    /** Stream as a source of messages. */
    private FullReadInputStream inputStream;
    /** Have we already read the header for the next message? */
    private boolean hasReadHeader = false;
    private XMessageHeader header;

    public SyncMessageReader(FullReadInputStream inputStream) {
        this.inputStream = inputStream;
    }

    /**
     * Read the header for the next message.
     *
     * <p>
     * Note that the "header" per-se is the size of all data following the header. This currently includes the message type tag (1 byte) and the message
     * bytes. However since we know the type tag is present we also read it as part of the header. This may change in the future if session multiplexing is
     * supported by the protocol. The protocol will be able to accommodate it but we will have to separate reading data after the header (size).
     * 
     * @throws IOException
     *             in case of reading error
     */
    private void readMessageHeader() throws IOException {
        byte[] len = new byte[5];
        this.inputStream.readFully(len);
        this.header = new XMessageHeader(len);
        this.hasReadHeader = true;
    }

    /**
     * Clear the stored header.
     */
    private void clearHeader() {
        this.hasReadHeader = false;
        this.header = null;
    }

    @SuppressWarnings("unchecked")
    @Override
    public XMessageHeader readHeader() throws IOException {
        if (!this.hasReadHeader) {
            try {
                readMessageHeader();
            } catch (IOException ex) {
                throw new CJCommunicationsException("Cannot read packet header", ex);
            }
        }
        int type = this.header.getMessageType(); // forces header read if necessary

        Class<? extends GeneratedMessage> messageClass = MessageConstants.getMessageClassForType(type);

        if (messageClass == Error.class) {
            // throw an error/exception if receive an Error message
            throw new XProtocolError(readAndParse((Parser<Error>) MessageConstants.MESSAGE_CLASS_TO_PARSER.get(Error.class)));
        }

        return this.header;
    }

    private <T extends GeneratedMessage> T readAndParse(Parser<T> parser) {
        byte[] packet = new byte[this.header.getMessageSize()];

        try {
            // for debugging
            // System.err.println("Initiating read of message (size=" + this.payloadSize + ", tag=" + ServerMessages.Type.valueOf(this.messageType) + ")");
            this.inputStream.readFully(packet);
        } catch (IOException ex) {
            throw new CJCommunicationsException("Cannot read packet payload", ex);
        }

        try {
            return parser.parseFrom(packet);
        } catch (InvalidProtocolBufferException ex) {
            // wrap the protobuf exception. No further information is available
            throw new WrongArgumentException(ex);
        } finally {
            // this must happen if we *successfully* read a packet. CJCommunicationsException will be thrown above if not
            clearHeader();
        }
    }

    @Override
    public XMessage readMessage(Optional<XMessage> reuse, XMessageHeader hdr) throws IOException {
        Class<? extends GeneratedMessage> messageClass = MessageConstants.getMessageClassForType(hdr.getMessageType());
        return new XMessage(readAndParse(messageClass));
    }

    @Override
    public XMessage readMessage(Optional<XMessage> reuse, int expectedType) throws IOException {
        try {
            Class<? extends GeneratedMessage> messageClass = MessageConstants.getMessageClassForType(readHeader().getMessageType());
            Class<? extends GeneratedMessage> expectedClass = MessageConstants.getMessageClassForType(expectedType);

            // ensure that parsed message class matches incoming tag
            if (expectedClass != messageClass) {
                throw new WrongArgumentException("Unexpected message class. Expected '" + expectedClass.getSimpleName() + "' but actually received '"
                        + messageClass.getSimpleName() + "'");
            }

            return new XMessage(readAndParse(messageClass));
        } catch (IOException e) {
            throw new XProtocolError(e.getMessage(), e);
        }
    }

    @SuppressWarnings("unchecked")
    private <T extends GeneratedMessage> T readAndParse(Class<T> messageClass) {
        return readAndParse((Parser<T>) MessageConstants.MESSAGE_CLASS_TO_PARSER.get(messageClass));
    }
}
