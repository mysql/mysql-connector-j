/*
 * Copyright (c) 2018, 2020, Oracle and/or its affiliates.
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

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import com.mysql.cj.protocol.MessageHeader;

public class XMessageHeader implements MessageHeader {
    public static final int MESSAGE_SIZE_LENGTH = 4;
    public static final int MESSAGE_TYPE_LENGTH = 1;
    public static final int HEADER_LENGTH = MESSAGE_SIZE_LENGTH + MESSAGE_TYPE_LENGTH;

    private ByteBuffer headerBuf;
    /** Type tag of the message to read (indicates parser to use). */
    private int messageType = -1;
    /** Size of the message that will be read. */
    private int messageSize = -1;

    public XMessageHeader() {
        this.headerBuf = ByteBuffer.allocate(HEADER_LENGTH).order(ByteOrder.LITTLE_ENDIAN);
    }

    public XMessageHeader(byte[] buf) {
        this.headerBuf = ByteBuffer.wrap(buf).order(ByteOrder.LITTLE_ENDIAN);
    }

    private void parseBuffer() {
        if (this.messageSize == -1) {
            this.headerBuf.position(0); // process the completed header and initiate message reading
            this.messageSize = this.headerBuf.getInt() - 1;
            this.messageType = this.headerBuf.get();
        }
    }

    @Override
    public ByteBuffer getBuffer() {
        return this.headerBuf;
    }

    @Override
    public int getMessageSize() {
        parseBuffer();
        return this.messageSize;
    }

    @Override
    public byte getMessageSequence() {
        return 0;
    }

    public int getMessageType() {
        parseBuffer();
        return this.messageType;
    }
}
