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

package com.mysql.cj.protocol;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;

import com.mysql.cj.exceptions.CJOperationNotSupportedException;
import com.mysql.cj.exceptions.ExceptionFactory;

/**
 * This interface provides a facility for sending messages to server. The destination, transmission method, etc are determined by the implementation.
 *
 * @param <M>
 *            Message type
 */
public interface MessageSender<M extends Message> {

    /**
     * Synchronously send the message to server.
     *
     * @param message
     *            byte array containing a message
     * @param messageLen
     *            length of the message
     * @param messageSequence
     *            message sequence index (used in a native protocol)
     * @throws IOException
     *             if an error occurs
     */
    default void send(byte[] message, int messageLen, byte messageSequence) throws IOException {
        throw ExceptionFactory.createException(CJOperationNotSupportedException.class, "Not supported");
    }

    /**
     * Synchronously send the message to server.
     *
     * @param message
     *            {@link Message} instance
     */
    default void send(M message) {
        throw ExceptionFactory.createException(CJOperationNotSupportedException.class, "Not supported");
    }

    /**
     * Asynchronously write a message with a notification being delivered to <code>callback</code> upon completion of write of entire message.
     *
     * @param message
     *            message extending {@link Message}
     * @param future
     *            a Future returning operation result
     * @param callback
     *            a callback to receive notification of when the message is completely written
     * @return result
     */
    default CompletableFuture<?> send(M message, CompletableFuture<?> future, Runnable callback) {
        throw ExceptionFactory.createException(CJOperationNotSupportedException.class, "Not supported");
    }

    /**
     * Set max allowed packet size.
     *
     * @param maxAllowedPacket
     *            max allowed packet size
     */
    default void setMaxAllowedPacket(int maxAllowedPacket) {
        throw ExceptionFactory.createException(CJOperationNotSupportedException.class, "Not supported");
    }

    /**
     * Return a PacketSender instance free of decorators.
     *
     * @return
     *         {@link MessageSender} instance
     */
    default MessageSender<M> undecorateAll() {
        return this;
    }

    /**
     * Return the previous PacketSender instance from the decorators chain or the current PacketSender
     * if it is the first entry in a chain.
     *
     * @return
     *         {@link MessageSender} instance
     */
    default MessageSender<M> undecorate() {
        return this;
    }

}
