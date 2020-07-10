/*
 * Copyright (c) 2015, 2020, Oracle and/or its affiliates.
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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.google.protobuf.ByteString;
import com.mysql.cj.exceptions.WrongArgumentException;
import com.mysql.cj.x.protobuf.Mysqlx.ClientMessages;
import com.mysql.cj.x.protobuf.Mysqlx.Ok;
import com.mysql.cj.x.protobuf.MysqlxSession.AuthenticateStart;
import com.mysql.cj.x.protobuf.MysqlxSession.Reset;

public class SyncMessageWriterTest {
    private ByteArrayOutputStream outputStream;
    private SyncMessageSender writer;

    @BeforeEach
    public void setUp() {
        this.outputStream = new ByteArrayOutputStream();
        this.writer = new SyncMessageSender(new BufferedOutputStream(this.outputStream));
    }

    /**
     * Test that we can (properly) write a complete message.
     * 
     * @throws IOException
     */
    @Test
    public void testCompleteWriteMessage() throws IOException {
        // construct and write the message
        AuthenticateStart.Builder msgBuilder = AuthenticateStart.newBuilder();
        msgBuilder.setMechName("Unit-Test");
        msgBuilder.setAuthData(ByteString.copyFromUtf8("some-auth-data"));
        AuthenticateStart msg = msgBuilder.build();
        this.writer.send(new XMessage(msg));

        // verify the written packet
        byte[] sentBytes = this.outputStream.toByteArray();
        int msgSize = msg.getSerializedSize();
        assertTrue(msgSize < Byte.MAX_VALUE, "Required for rest of test, should never fail");
        int payloadSize = msgSize + 1;
        // message size (4 bytes little endian)
        assertEquals(payloadSize, sentBytes[0]);
        assertEquals(0, sentBytes[1]);
        assertEquals(0, sentBytes[2]);
        assertEquals(0, sentBytes[3]);
        assertEquals(ClientMessages.Type.SESS_AUTHENTICATE_START_VALUE, sentBytes[4], "Type tag");
        assertEquals(payloadSize + 4, sentBytes.length, "Entire packet size should be header bytes + serialized message");
    }

    @Test
    public void testBadMessageClass() {
        try {
            // try sending "Ok" which is a server-sent message. should fail with exception
            this.writer.send(new XMessage(Ok.getDefaultInstance()));
            fail("Writing OK message should fail");
        } catch (WrongArgumentException ex) {
            // expected
        }
    }

    @Test
    public void testLastPacketSentTime() throws InterruptedException {
        long start = System.currentTimeMillis();
        this.writer.send(new XMessage(Reset.getDefaultInstance()));
        long lastSent1 = this.writer.getLastPacketSentTime();
        assertTrue(lastSent1 >= start);
        Thread.sleep(50);
        this.writer.send(new XMessage(Reset.getDefaultInstance()));
        long lastSent2 = this.writer.getLastPacketSentTime();
        assertTrue(lastSent2 >= lastSent1);
    }
}
