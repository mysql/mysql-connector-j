/*
 * Copyright (c) 2015, 2017, Oracle and/or its affiliates. All rights reserved.
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

package com.mysql.cj.api.x.io;

import java.io.IOException;

import com.google.protobuf.ByteString;
import com.google.protobuf.GeneratedMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Parser;
import com.mysql.cj.core.exceptions.CJCommunicationsException;
import com.mysql.cj.core.exceptions.WrongArgumentException;
import com.mysql.cj.x.core.XDevAPIError;

/**
 * Low-level message reader for X protocol. The <i>MessageReader</i> will generally be used in one of two ways (See note regarding exceptions for Error
 * messages):
 * <ul>
 * <li>The next message type is known and it's an error to read any other type of message. The caller will generally call the reader like so:
 * 
 * <pre>
 * MessageType msg = reader.read(MessageType.class);
 * </pre>
 * 
 * </li>
 * <li>The next message type is not known and the caller must conditionally decided what to do based on the type of the next message. The {@link
 * #getNextMessageClass()} method supports this use case. The caller will generally call the reader like so:
 * 
 * <pre>
 * if (reader.getNextMessageClass() == MessageType1.class) {
 *     MessageType1 msg1 = reader.read(MessageType1.class);
 *     // do something with msg1
 * } else if (reader.getNextMessageClass() == MessageType2.class) {
 *     MessageType2 msg2 = reader.read(MessageType2.class);
 *     // do something with msg2
 * }
 * </pre>
 * 
 * </li>
 * </ul>
 * <p>
 * If the <i>MessageReader</i> encounters an <i>Error</i> message, it will throw a {@link XDevAPIError} exception to indicate that an error was returned from
 * the
 * server.
 * </p>
 * <p>
 * All external interaction should only know about message <i>classes</i>. Message type tags are an implementation detail hidden in the <i>MessageReader</i>.
 * </p>
 * TODO: write about async usage
 */
public interface MessageReader {
    /**
     * Get the class of the next message, possibly blocking indefinitely until the message is received.
     *
     * @return the class of the received message
     * @throws XDevAPIError
     *             if an <i>Error</i> message is received from the server
     */
    Class<? extends GeneratedMessage> getNextMessageClass();

    /**
     * Synchronously read the next message in the stream. Block until the message is read fully.
     *
     * @param <T>
     *            message type extending {@link GeneratedMessage}
     * @param expectedClass
     *            the class of the expected message
     * @return the next message of type T
     * @throws WrongArgumentException
     *             if the expected message type is not the next message (exception will be thrown in *caller* context)
     * @throws XDevAPIError
     *             if an <i>Error</i> message is received from the server
     * @throws CJCommunicationsException
     *             wrapping an {@link IOException} during read or parse
     */
    <T extends GeneratedMessage> T read(Class<T> expectedClass);

    @SuppressWarnings("unchecked")
    public static <T extends GeneratedMessage> T parseNotice(ByteString payload, Class<T> noticeClass) {
        try {
            Parser<T> parser = (Parser<T>) MessageConstants.MESSAGE_CLASS_TO_PARSER.get(noticeClass);
            return parser.parseFrom(payload);
        } catch (InvalidProtocolBufferException ex) {
            throw new CJCommunicationsException(ex);
        }
    }
}
