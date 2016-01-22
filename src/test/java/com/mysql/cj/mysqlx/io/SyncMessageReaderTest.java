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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;

import com.google.protobuf.GeneratedMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Parser;
import com.mysql.cj.core.exceptions.WrongArgumentException;
import com.mysql.cj.core.io.FullReadInputStream;
import com.mysql.cj.mysqlx.MysqlxError;
import com.mysql.cj.mysqlx.protobuf.Mysqlx.Error;
import com.mysql.cj.mysqlx.protobuf.Mysqlx.Ok;
import com.mysql.cj.mysqlx.protobuf.Mysqlx.ServerMessages;

/**
 * Tests for {@link SyncMessageReader}.
 */
public class SyncMessageReaderTest {
    private SyncMessageReader reader;

    private static final byte[] okMsgPacket = serializeMessage(Ok.newBuilder().build(), ServerMessages.Type.OK_VALUE);
    private static final byte[] errMsgPacket = serializeMessage(
            Error.newBuilder().setMsg("oops").setCode(5432).setSqlState("12S34").setSeverity(Error.Severity.FATAL).build(), ServerMessages.Type.ERROR_VALUE);

    @Before
    public void setUp() {
    }

    /**
     * Serialize a message for testing.
     */
    private static byte[] serializeMessage(GeneratedMessage msg, int type) {
        int packetLen = msg.getSerializedSize() + 1;
        byte[] packet = ByteBuffer.allocate(packetLen + 4).order(ByteOrder.LITTLE_ENDIAN).putInt(packetLen).put((byte) type).put(msg.toByteArray()).array();
        return packet;
    }

    @Test
    public void testNextMessageClass() {
        this.reader = new SyncMessageReader(new FullReadInputStream(new ByteArrayInputStream(okMsgPacket)));
        assertEquals(Ok.class, this.reader.getNextMessageClass());
    }

    @Test
    public void testReadKnownMessageType() {
        this.reader = new SyncMessageReader(new FullReadInputStream(new ByteArrayInputStream(okMsgPacket)));
        Ok msg = this.reader.read(Ok.class);
        assertTrue(msg.isInitialized());
    }

    @Test
    public void testReadWrongMessageType() {
        this.reader = new SyncMessageReader(new FullReadInputStream(new ByteArrayInputStream(okMsgPacket)));
        // will throw a WrongArgumentException if failed
        try {
            Error msg = this.reader.read(Error.class);
            fail("Should not be able to read an error message when one is not present");
            assertTrue(msg.isInitialized()); // to squelch compiler warnings
        } catch (WrongArgumentException ex) {
            assertEquals("Unexpected message class. Expected '" + Error.class.getSimpleName() + "' but actually received '" + Ok.class.getSimpleName() + "'",
                    ex.getMessage());
        }
    }

    @Test
    public void testUnexpectedError() {
        this.reader = new SyncMessageReader(new FullReadInputStream(new ByteArrayInputStream(errMsgPacket)));
        try {
            // attempt to read an Ok packet
            this.reader.read(Ok.class);
            fail("Should not be able to read the OK packet");
        } catch (MysqlxError ex) {
            // check that the exception contains the error info from the server
            assertEquals("ERROR 5432 (12S34) oops", ex.getMessage());
            assertEquals("12S34", ex.getSQLState());
            assertEquals(5432, ex.getErrorCode());
        }
    }

    /**
     * This is a 'mini'-stress test that encompasses the check of <i>clearHeader()</i> being called correctly.
     */
    @Test
    public void testSeveralMessages() throws IOException {
        // construct the test message stream
        // message stream is: Error, Error, Error, Ok, Error, Ok, Error
        // if the header is not cleared properly, the second Error would be read incorrectly
        ByteArrayOutputStream x = new ByteArrayOutputStream();
        x.write(errMsgPacket);
        x.write(errMsgPacket);
        x.write(errMsgPacket);
        x.write(okMsgPacket);
        x.write(errMsgPacket);
        x.write(okMsgPacket);
        x.write(errMsgPacket);

        this.reader = new SyncMessageReader(new FullReadInputStream(new ByteArrayInputStream(x.toByteArray())));
        // read first three errors "unexpectedly" in a loop
        for (int i = 0; i < 3; ++i) {
            try {
                this.reader.read(Ok.class);
            } catch (MysqlxError err) {
                assertEquals(5432, err.getErrorCode());
            }
        }
        // read remaining messages normally
        this.reader.read(Ok.class);
        try {
            this.reader.read(Error.class);
        } catch (MysqlxError err) {
            // expected
        }
        this.reader.read(Ok.class);
        try {
            this.reader.read(Error.class);
        } catch (MysqlxError err) {
            // expected
        }
    }

    /**
     * Verification test to help prevent bugs in the typecode/class/parser mapping tables. We check that all classes that are mapped have a parser.
     * 
     * @todo Test in the other direction also
     */
    @Test
    public void testMappingTables() throws InvalidProtocolBufferException {
        for (Map.Entry<Class<? extends GeneratedMessage>, Integer> entry : MessageConstants.MESSAGE_CLASS_TO_TYPE.entrySet()) {
            /* int type = */entry.getValue();
            Class<? extends GeneratedMessage> messageClass = entry.getKey();
            Parser<? extends GeneratedMessage> parser = MessageConstants.MESSAGE_CLASS_TO_PARSER.get(messageClass);
            assertNotNull(parser);
            GeneratedMessage partiallyParsed = parser.parsePartialFrom(new byte[] {});
            assertEquals("Parsed class should equal the class that mapped to it via type tag", messageClass, partiallyParsed.getClass());
        }
    }
}
