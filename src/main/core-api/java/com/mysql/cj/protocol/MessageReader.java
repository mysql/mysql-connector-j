/*
 * Copyright (c) 2018, 2022, Oracle and/or its affiliates.
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

package com.mysql.cj.protocol;

import java.io.IOException;
import java.util.Optional;

import com.mysql.cj.exceptions.CJOperationNotSupportedException;
import com.mysql.cj.exceptions.ExceptionFactory;

public interface MessageReader<H extends MessageHeader, M extends Message> {

    /**
     * Read the next message header from server, possibly blocking indefinitely until the message is received.
     * 
     * @return {@link MessageHeader} of the next message
     * @throws IOException
     *             if an error occurs
     */
    H readHeader() throws IOException;

    /**
     * Read the next message header from server, possibly blocking indefinitely until the message is received,
     * and cache it so that the next {@link #readHeader()} return the same header.
     * 
     * @return {@link MessageHeader} of the next message
     * @throws IOException
     *             if an error occurs
     */
    default H probeHeader() throws IOException {
        return readHeader();
    }

    /**
     * Read message from server into to the given {@link Message} instance or into the new one if not present.
     * For asynchronous channel it synchronously reads the next message in the stream, blocking until the message is read fully.
     * Could throw CJCommunicationsException wrapping an {@link IOException} during read or parse
     * 
     * @param reuse
     *            {@link Message} object to reuse. May be ignored by implementation.
     * @param header
     *            {@link MessageHeader} instance
     * @return {@link Message} instance
     * @throws IOException
     *             if an error occurs
     */
    M readMessage(Optional<M> reuse, H header) throws IOException;

    /**
     * Read message from server into to the given {@link Message} instance or into the new one if not present
     * and cache it so that the next {@link #readMessage(Optional, MessageHeader)} return the same message.
     * For asynchronous channel it synchronously reads the next message in the stream, blocking until the message is read fully.
     * Could throw CJCommunicationsException wrapping an {@link IOException} during read or parse
     * 
     * @param reuse
     *            {@link Message} object to reuse. May be ignored by implementation.
     * @param header
     *            {@link MessageHeader} instance
     * @return {@link Message} instance
     * @throws IOException
     *             if an error occurs
     */
    default M probeMessage(Optional<M> reuse, H header) throws IOException {
        return readMessage(reuse, header);
    }

    /**
     * Read message from server into to the given {@link Message} instance or into the new one if not present.
     * For asynchronous channel it synchronously reads the next message in the stream, blocking until the message is read fully.
     * Could throw WrongArgumentException if the expected message type is not the next message (exception will be thrown in *caller* context).
     * 
     * @param reuse
     *            {@link Message} object to reuse. May be ignored by implementation.
     * @param expectedType
     *            Expected type of message.
     * @return {@link Message} instance
     * @throws IOException
     *             if an error occurs
     */
    default M readMessage(Optional<M> reuse, int expectedType) throws IOException {
        throw ExceptionFactory.createException(CJOperationNotSupportedException.class, "Not supported");
    }

    /**
     * Skips the next packet, or the current one if previously probed, by reading and discarding it.
     * 
     * @throws IOException
     *             if an error occurs
     */
    default void skipPacket() throws IOException {
        readMessage(Optional.empty(), readHeader());
    }

    /**
     * Queue a {@link MessageListener} to receive messages delivered asynchronously.
     * 
     * @param l
     *            {@link MessageListener}
     */
    default void pushMessageListener(MessageListener<M> l) {
        throw ExceptionFactory.createException(CJOperationNotSupportedException.class, "Not supported");
    }

    /**
     * Get last message sequence number, as it was stored by {@link #readHeader()}.
     * 
     * @return number
     */
    default byte getMessageSequence() {
        return 0;
    }

    /**
     * Set stored message sequence number to 0.
     */
    default void resetMessageSequence() {
        // no-op
    }

    /**
     * Return a MessageReader instance free of decorators.
     * 
     * @return {@link MessageReader}
     */
    default MessageReader<H, M> undecorateAll() {
        return this;
    }

    /**
     * Return the previous MessageReader instance from the decorators chain or the current MessageReader
     * if it is the first entry in a chain.
     * 
     * @return {@link MessageReader}
     */
    default MessageReader<H, M> undecorate() {
        return this;
    }

    /**
     * Start reading messages reader from the provided channel.
     */
    default void start() {
        // no-op
    }

    /**
     * Signal to the reader that it should stop reading messages after reading the next message.
     */
    default void stopAfterNextMessage() {
        // no-op
    }

}
