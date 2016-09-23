/*
  Copyright (c) 2015, 2016, Oracle and/or its affiliates. All rights reserved.

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

import com.google.protobuf.GeneratedMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Parser;
import com.mysql.cj.core.exceptions.AssertionFailedException;
import com.mysql.cj.core.exceptions.CJCommunicationsException;
import com.mysql.cj.core.exceptions.WrongArgumentException;
import com.mysql.cj.core.io.FullReadInputStream;
import com.mysql.cj.mysqlx.MysqlxError;
import com.mysql.cj.mysqlx.protobuf.Mysqlx.Error;
import com.mysql.cj.mysqlx.protobuf.Mysqlx.ServerMessages;

/**
 * Synchronous-only implementation of {@link MessageReader}. This implementation wraps an {@link java.io.InputStream}.
 */
public class SyncMessageReader implements MessageReader {
    /** Stream as a source of messages. */
    private FullReadInputStream inputStream;
    /** Have we already read the header for the next message? */
    private boolean hasReadHeader = false;
    /** Type tag of message. */
    private int messageType = -1;
    /** Payload size from header. The payload is the type tag + encoded message data. */
    private int payloadSize = -1;

    public SyncMessageReader(FullReadInputStream inputStream) {
        this.inputStream = inputStream;
    }

    /**
     * Read the header for the next message.
     *
     * <p>
     * Note that the "header" per-se is the size of all data following the header. This currently includes the message type tag (1 byte) and the message
     * bytes. However since we know the type tag is present we also read it as part of the header. This may change in the future if session multiplexing is
     * supported by the protocol. The protocol will be able to accomodate it but we will have to separate reading data after the header (size).
     */
    private void readHeader() throws IOException {
        byte[] len = new byte[4];
        this.inputStream.readFully(len);
        this.payloadSize = ByteBuffer.wrap(len).order(ByteOrder.LITTLE_ENDIAN).getInt();
        this.messageType = this.inputStream.read();
        this.hasReadHeader = true;
    }

    /**
     * Clear the stored header.
     */
    private void clearHeader() {
        this.hasReadHeader = false;
        this.messageType = -1;
        this.payloadSize = -1;
    }

    /**
     * Get the message type of the next message, possibly blocking indefinitely until the message is received.
     */
    private int getNextMessageType() {
        if (!this.hasReadHeader) {
            try {
                readHeader();
            } catch (IOException ex) {
                throw new CJCommunicationsException("Cannot read packet header", ex);
            }
        }
        return this.messageType;
    }

    @SuppressWarnings("unchecked")
    public Class<? extends GeneratedMessage> getNextMessageClass() {
        int type = getNextMessageType(); // forces header read if necessary
        Class<? extends GeneratedMessage> messageClass = MessageConstants.MESSAGE_TYPE_TO_CLASS.get(type);
        if (messageClass == null) {
            // check if there's a mapping that we don't explicitly handle
            ServerMessages.Type serverMessageMapping = ServerMessages.Type.valueOf(type);
            throw AssertionFailedException.shouldNotHappen("Unknown message type: " + type + " (server messages mapping: " + serverMessageMapping + ")");
        } else if (messageClass == Error.class) {
            // throw an error/exception if receive an Error message
            throw new MysqlxError(readAndParse((Parser<Error>) MessageConstants.MESSAGE_CLASS_TO_PARSER.get(Error.class)));
        }

        return messageClass;
    }

    /**
     * @todo
     */
    private <T extends GeneratedMessage> T readAndParse(Parser<T> parser) {
        byte[] packet = new byte[this.payloadSize - 1];

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

    @SuppressWarnings("unchecked")
    public <T extends GeneratedMessage> T read(Class<T> expectedClass) {
        Class<? extends GeneratedMessage> messageClass = getNextMessageClass();

        // ensure that parsed message class matches incoming tag
        if (expectedClass != messageClass) {
            throw new WrongArgumentException(
                    "Unexpected message class. Expected '" + expectedClass.getSimpleName() + "' but actually received '" + messageClass.getSimpleName() + "'");
        }

        return readAndParse((Parser<T>) MessageConstants.MESSAGE_CLASS_TO_PARSER.get(messageClass));
    }
}
