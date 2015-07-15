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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import com.google.protobuf.GeneratedMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Parser;

import static com.mysql.cj.mysqlx.protobuf.Mysqlx.Error;
import static com.mysql.cj.mysqlx.protobuf.Mysqlx.ServerMessages;
import com.mysql.cj.core.io.FullReadInputStream;
import com.mysql.cj.core.exceptions.AssertionFailedException;
import com.mysql.cj.core.exceptions.CJCommunicationsException;
import com.mysql.cj.core.exceptions.WrongArgumentException;
import com.mysql.cj.mysqlx.MysqlxError;

/**
 * Low-level message reader for MySQL-X protocol. The <i>MessageReader</i> will generally be used in one of two ways (See note regarding exceptions for Error messages):
 * <ul>
 * <li>The next message type is known and it's an assertion failure to read any other type of message. The caller will generally call the reader like so:
 * <pre>MessageType msg = reader.read(MessageType.class);</pre></li>
 * <li>The next message type is not known and the caller must conditionally decided what to do based on the type of the next message. The {@link
 * getNextMessageType(Class)} supports this user class. The caller will generally call the reader like so:
 * <pre>if (reader.getNextMessageType() == MessageType1.class) {
 *   MessageType1 msg1 = reader.read(MessageType1.class);
 *   // do something with msg1
 * } else if (reader.getNextMessageType() == MessageType2.class) {
 *   MessageType2 msg2 = reader.read(MessageType2.class);
 *   // do something with msg2
 * }</pre></li>
 * </ul>
 * <p/>
 * If the <i>MessageReader</i> encounters an <i>Error</i> message, it will throw a {@link MysqlxError} exception to indicate that an error was returned from the
 * server. The only situation in which this will not happen is if the caller explicitly requests to read an <i>Error</i> message.
 * <p/>
 * All external interaction should only know about message <i>classes</i>. Message type tags are an implementation detail hidden in the <i>MessageReader</i>.
 */
public class MessageReader {

    private FullReadInputStream inputStream;
    /** Have we already read the header for the next message? */
    private boolean hasReadHeader = false;
    /** Type tag of message. */
    private int type = -1;
    /** Payload size from header. The payload is the type tag + encoded message data. */
    private int payloadSize = -1;

    public MessageReader(FullReadInputStream inputStream) {
        this.inputStream = inputStream;
    }

    /**
     * Read the header for the next message.
     * 
     * <p>Note that the "header" per-se is the size of all data following the header. This currently includes the message type tag (1 byte) and the message
     * bytes. However since we know the type tag is present we also read it as part of the header. This may change in the future if session multiplexing is
     * supported by the protocol. The protocol will be able to accomodate it but we will have to separate reading data after the header (size).
     */
    private void readHeader() throws IOException {
        byte[] len = new byte[4];
        this.inputStream.readFully(len);
        this.payloadSize = ByteBuffer.wrap(len).order(ByteOrder.LITTLE_ENDIAN).getInt();
        this.type = this.inputStream.read();
        this.hasReadHeader = true;
    }

    /**
     * Clear the stored header.
     */
    private void clearHeader() {
        this.hasReadHeader = false;
        this.type = -1;
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
        return this.type;
    }

    /**
     * Get the class of the next message, possibly blocking indefinitely until the message is received.
     */
    public Class<? extends GeneratedMessage> getNextMessageClass() {
        int type = getNextMessageType(); // forces header read if necessary
        Class<? extends GeneratedMessage> messageClass = MessageConstants.MESSAGE_TYPE_TO_CLASS.get(type);
        if (messageClass == null) {
            // check if there's a mapping that we don't explicitly handle
            ServerMessages.Type serverMessageMapping = ServerMessages.Type.valueOf(type);
            throw AssertionFailedException.shouldNotHappen("Unknown message type: " + type + " (server messages mapping: " + serverMessageMapping + ")");
        }
        return messageClass;
    }

    /**
     * Throw an exception in response to an <i>Error</i> message received from the server.
     */
    private void throwErrorFromServer(Error msg) {
        throw new MysqlxError(msg);
    }

    /**
     * Read the next message in the stream. Block until the message is read fully.
     *
     * @param expectedClass the class of the expected message
     * @return the next message of type T
     * @throws WrongArgumentException if the expected message type is not the next message (exception will be thrown in *caller* context)
     * @throws MysqlxError if an <i>Error</i> message is encountered when not requested
     * @throws CJCommunicationsException wrapping an {@link IOException}
     */
    public <T extends GeneratedMessage> T read(Class<T> expectedClass) {
        Class<? extends GeneratedMessage> messageClass = getNextMessageClass();
        byte[] packet = new byte[this.payloadSize - 1];

        try {
            this.inputStream.readFully(packet);
        } catch (IOException ex) {
            throw new CJCommunicationsException("Cannot read packet payload", ex);
        }

        try {
            Parser<? extends GeneratedMessage> parser = MessageConstants.MESSAGE_CLASS_TO_PARSER.get(messageClass);

            GeneratedMessage msg = parser.parseFrom(packet);
            // throw an error/exception if we *unexpectedly* received an Error message
            if (type == ServerMessages.Type.ERROR_VALUE && !expectedClass.equals(msg.getClass())) {
                throwErrorFromServer((Error) msg);
            }
 
            // ensure that parsed message class matches incoming tag
            if (!expectedClass.equals(msg.getClass())) {
                throw new WrongArgumentException("Unexpected message class. Expected '" + expectedClass.getSimpleName() + "' but actually received '" +
                        msg.getClass().getSimpleName() + "'");
            }

            return (T) msg;
        } catch (InvalidProtocolBufferException ex) {
            // wrap the protobuf exception. No further information is available
            throw new WrongArgumentException(ex);
        } finally {
            // this must happen if we *successfully* read a packet
            clearHeader();
        }
    }
}
