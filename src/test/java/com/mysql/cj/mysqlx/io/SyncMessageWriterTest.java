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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

import org.junit.Before;
import org.junit.Test;

import com.google.protobuf.ByteString;
import com.mysql.cj.core.exceptions.WrongArgumentException;
import com.mysql.cj.mysqlx.protobuf.Mysqlx.ClientMessages;
import com.mysql.cj.mysqlx.protobuf.Mysqlx.Ok;
import com.mysql.cj.mysqlx.protobuf.MysqlxSession.AuthenticateStart;
import com.mysql.cj.mysqlx.protobuf.MysqlxSession.Reset;

public class SyncMessageWriterTest {
    private ByteArrayOutputStream outputStream;
    private SyncMessageWriter writer;

    @Before
    public void setUp() {
        this.outputStream = new ByteArrayOutputStream();
        this.writer = new SyncMessageWriter(new BufferedOutputStream(this.outputStream));
    }

    /**
     * Test that we can (properly) write a complete message.
     */
    @Test
    public void testCompleteWriteMessage() throws IOException {
        // construct and write the message
        AuthenticateStart.Builder msgBuilder = AuthenticateStart.newBuilder();
        msgBuilder.setMechName("Unit-Test");
        msgBuilder.setAuthData(ByteString.copyFromUtf8("some-auth-data"));
        AuthenticateStart msg = msgBuilder.build();
        this.writer.write(msg);

        // verify the written packet
        byte[] sentBytes = this.outputStream.toByteArray();
        int msgSize = msg.getSerializedSize();
        assertTrue("Required for rest of test, should never fail", msgSize < Byte.MAX_VALUE);
        int payloadSize = msgSize + 1;
        // message size (4 bytes little endian)
        assertEquals(payloadSize, sentBytes[0]);
        assertEquals(0, sentBytes[1]);
        assertEquals(0, sentBytes[2]);
        assertEquals(0, sentBytes[3]);
        assertEquals("Type tag", ClientMessages.Type.SESS_AUTHENTICATE_START_VALUE, sentBytes[4]);
        assertEquals("Entire packet size should be header bytes + serialized message", payloadSize + 4, sentBytes.length);
    }

    @Test
    public void testBadMessageClass() {
        try {
            // try sending "Ok" which is a server-sent message. should fail with exception
            this.writer.write(Ok.getDefaultInstance());
            fail("Writing OK message should fail");
        } catch (WrongArgumentException ex) {
            // expected
        }
    }

    @Test
    public void testLastPacketSentTime() throws InterruptedException {
        long start = System.currentTimeMillis();
        this.writer.write(Reset.getDefaultInstance());
        long lastSent1 = this.writer.getLastPacketSentTime();
        assertTrue(lastSent1 >= start);
        Thread.sleep(50);
        this.writer.write(Reset.getDefaultInstance());
        long lastSent2 = this.writer.getLastPacketSentTime();
        assertTrue(lastSent2 >= lastSent1);
    }
}
