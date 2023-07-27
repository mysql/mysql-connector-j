/*
 * Copyright (c) 2015, 2023, Oracle and/or its affiliates.
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

package com.mysql.cj.protocol.a;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * Tests for simple/direct packet sender.
 */
public class SimplePacketSenderTest extends PacketSenderTestBase {

    private ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    private SimplePacketSender sender = new SimplePacketSender(new BufferedOutputStream(this.outputStream));

    @AfterEach
    public void cleanupByteArrayOutputStream() {
        this.outputStream.reset();
    }

    @Test
    public void basicPacketSanityTest() throws IOException {
        final int packetLen = 20;

        byte[] packet = new byte[packetLen];
        fillPacketSequentially(packet);

        final byte packetSequence = 40;
        this.sender.send(packet, packetLen, packetSequence);

        // check encoded packet
        byte[] sentPacket = this.outputStream.toByteArray();
        assertEquals(packetLen + NativeConstants.HEADER_LENGTH, sentPacket.length);
        assertEquals(packetLen, NativeUtils.decodeMysqlThreeByteInteger(sentPacket));
        assertEquals(packetSequence, sentPacket[NativeConstants.HEADER_LENGTH - 1]);
        checkSequentiallyFilledPacket(sentPacket, NativeConstants.HEADER_LENGTH, packetLen);
    }

    @Test
    public void splitPacketTest() throws IOException {
        final int leftoverPacketLen = 4000;
        // test 2, 3, 4 multipackets
        for (int multiPackets = 2; multiPackets <= 4; ++multiPackets) {
            final int packetLen = (multiPackets - 1) * NativeConstants.MAX_PACKET_SIZE + leftoverPacketLen;

            byte[] packet = new byte[packetLen];

            // add markers to check the beginning and end of split packets
            // first byte is 10 + packet number
            // last byte is 20 + packet number
            packet[0] = 10 + 1;
            packet[packetLen - 1] = (byte) (20 + multiPackets);
            for (int i = 1; i < multiPackets; ++i) {
                // last byte of full packet
                packet[NativeConstants.MAX_PACKET_SIZE * i - 1] = (byte) (20 + i);
                // first byte of next packet
                packet[NativeConstants.MAX_PACKET_SIZE * i] = (byte) (10 + i + 1);
            }

            byte packetSequence = 17;
            this.outputStream.reset();
            this.sender.send(packet, packetLen, packetSequence);

            // check encoded packet
            int offset = 0;
            byte[] sentPacket = this.outputStream.toByteArray();
            // size of ALL packets written to output stream
            int sizeOfAllPackets = NativeConstants.HEADER_LENGTH * multiPackets + // header for each full packet + one leftover
                    NativeConstants.MAX_PACKET_SIZE * (multiPackets - 1) + // FULL packet payloads
                    leftoverPacketLen;
            assertEquals(sizeOfAllPackets, sentPacket.length);

            // check that i=`multiPackets' packets are sent plus one empty packet
            for (int i = 1; i <= multiPackets; ++i) {
                int splitPacketLen = NativeConstants.MAX_PACKET_SIZE;
                if (i == multiPackets) { // last packet is empty
                    splitPacketLen = leftoverPacketLen;
                }
                int packetLenInHeader = NativeUtils.decodeMysqlThreeByteInteger(sentPacket, offset);
                assertEquals(splitPacketLen, packetLenInHeader);
                assertEquals(packetSequence, sentPacket[offset + NativeConstants.HEADER_LENGTH - 1]);
                // check start/end bytes
                assertEquals(10 + i, sentPacket[offset + NativeConstants.HEADER_LENGTH]);
                assertEquals(20 + i, sentPacket[offset + NativeConstants.HEADER_LENGTH + packetLenInHeader - 1]);
                packetSequence++;
                offset += NativeConstants.MAX_PACKET_SIZE + NativeConstants.HEADER_LENGTH;
            }
        }
    }

    /**
     * Test the case where the packet size is a multiple of the max packet size. We need to send an extra empty packet to signal that the payload is complete.
     *
     * @throws IOException
     */
    @Test
    public void packetSizeMultipleOfMaxTest() throws IOException {
        // check 1, 2, 3 multiples of MAX_PACKET_SIZE
        for (int multiple = 1; multiple <= 3; ++multiple) {
            final int packetLen = multiple * NativeConstants.MAX_PACKET_SIZE;

            byte[] packet = new byte[packetLen];

            byte packetSequence = 40;
            this.outputStream.reset(); // reset as we're using it several times in this test
            this.sender.send(packet, packetLen, packetSequence);

            // check encoded packet
            int offset = 0;
            byte[] sentPacket = this.outputStream.toByteArray();
            // size of ALL packets written to output stream
            int sizeOfAllPackets = NativeConstants.HEADER_LENGTH * (multiple + 1) + // header for each full packet + one empty
                    NativeConstants.MAX_PACKET_SIZE * multiple; // FULL packet payloads
            assertEquals(sizeOfAllPackets, sentPacket.length);
            // check that `multiple' packets are sent plus one empty packet
            for (int i = 0; i < multiple + 1; ++i) {
                int splitPacketLen = NativeConstants.MAX_PACKET_SIZE;
                if (i == multiple) { // last packet is empty
                    splitPacketLen = 0;
                }
                int packetLenInHeader = NativeUtils.decodeMysqlThreeByteInteger(sentPacket, offset);
                assertEquals(splitPacketLen, packetLenInHeader);
                assertEquals(packetSequence, sentPacket[offset + NativeConstants.HEADER_LENGTH - 1]);
                packetSequence++;
                offset += NativeConstants.MAX_PACKET_SIZE + NativeConstants.HEADER_LENGTH;
            }
        }
    }

}
