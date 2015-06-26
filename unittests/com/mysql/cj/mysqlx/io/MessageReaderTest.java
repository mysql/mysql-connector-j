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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.google.protobuf.GeneratedMessage;
import org.junit.Before;
import org.junit.Test;

import static com.mysql.cj.mysqlx.protobuf.Mysqlx.Error;
import static com.mysql.cj.mysqlx.protobuf.Mysqlx.Ok;
import static com.mysql.cj.mysqlx.protobuf.Mysqlx.ServerMessages;
import com.mysql.cj.core.io.FullReadInputStream;
import com.mysql.cj.mysqlx.MysqlxError;

/**
 * Tests for MessageReader.
 */
public class MessageReaderTest {
    private MessageReader reader;

    private static final byte[] okMsgPacket = serializeMessage(Ok.newBuilder().build(), ServerMessages.Type.OK_VALUE);
    private static final byte[] errMsgPacket = serializeMessage(Error.newBuilder().setMsg("oops").setCode(5432).setSqlState("12S34")
            .setSeverity(Error.Severity.FATAL).build(), ServerMessages.Type.ERROR_VALUE);

    @Before
    public void setUp() {
    }

    /**
     * Serialize a message for testing.
     */
    private static byte[] serializeMessage(GeneratedMessage msg, int type) {
        int packetLen = msg.getSerializedSize() + MessageWriter.HEADER_LEN;
        byte[] packet = ByteBuffer.allocate(packetLen).putInt(packetLen).put((byte) type).put(msg.toByteArray()).array();
        return packet;
    }

    @Test
    public void testReadKnownMessageType() throws IOException {
        reader = new MessageReader(new FullReadInputStream(new ByteArrayInputStream(okMsgPacket)));
        // will throw a ClassCastException if failed
        Ok msg = reader.read();
        assertTrue(msg.isInitialized());
    }

    @Test
    public void testReadWrongMessageType() throws IOException {
        reader = new MessageReader(new FullReadInputStream(new ByteArrayInputStream(okMsgPacket)));
        // will throw a ClassCastException if failed
        try {
            Error msg = reader.read();
            fail("Should not be able to read an error message when one is not present");
            assertTrue(msg.isInitialized()); // to squelch compiler warnings
        } catch (ClassCastException ex) {
            assertEquals("com.mysql.cj.mysqlx.protobuf.Mysqlx$Ok cannot be cast to com.mysql.cj.mysqlx.protobuf.Mysqlx$Error", ex.getMessage());
        }
    }

    @Test
    public void testUnexpectedError() throws IOException {
        reader = new MessageReader(new FullReadInputStream(new ByteArrayInputStream(errMsgPacket)));
        try {
            // attempt to read an Ok packet
            reader.<Ok> read();
            fail("Should not be able to read the OK packet");
        } catch (MysqlxError ex) {
            // check that the exception contains the error info from the server
            assertEquals("oops", ex.getMessage());
            assertEquals("12S34", ex.getSQLState());
            assertEquals(5432, ex.getErrorCode());
        }
    }
}
