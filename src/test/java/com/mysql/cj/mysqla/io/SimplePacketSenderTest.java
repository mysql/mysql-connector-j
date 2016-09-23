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

package com.mysql.cj.mysqla.io;

import static org.junit.Assert.assertEquals;

import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

import org.junit.After;
import org.junit.Test;

import com.mysql.cj.mysqla.MysqlaConstants;
import com.mysql.cj.mysqla.MysqlaUtils;

/**
 * Tests for simple/direct packet sender.
 */
public class SimplePacketSenderTest extends PacketSenderTestBase {
    private ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    private SimplePacketSender sender = new SimplePacketSender(new BufferedOutputStream(this.outputStream));

    @After
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
        assertEquals(packetLen + MysqlaConstants.HEADER_LENGTH, sentPacket.length);
        assertEquals(packetLen, MysqlaUtils.decodeMysqlThreeByteInteger(sentPacket));
        assertEquals(packetSequence, sentPacket[MysqlaConstants.HEADER_LENGTH - 1]);
        checkSequentiallyFilledPacket(sentPacket, MysqlaConstants.HEADER_LENGTH, packetLen);
    }

    @Test
    public void splitPacketTest() throws IOException {
        final int leftoverPacketLen = 4000;
        // test 2, 3, 4 multipackets
        for (int multiPackets = 2; multiPackets <= 4; ++multiPackets) {
            final int packetLen = ((multiPackets - 1) * MysqlaConstants.MAX_PACKET_SIZE) + leftoverPacketLen;

            byte[] packet = new byte[packetLen];

            // add markers to check the beginning and end of split packets
            // first byte is 10 + packet number
            // last byte is 20 + packet number
            packet[0] = 10 + 1;
            packet[packetLen - 1] = (byte) (20 + multiPackets);
            for (int i = 1; i < multiPackets; ++i) {
                // last byte of full packet
                packet[(MysqlaConstants.MAX_PACKET_SIZE * i) - 1] = (byte) (20 + i);
                // first byte of next packet
                packet[MysqlaConstants.MAX_PACKET_SIZE * i] = (byte) (10 + i + 1);
            }

            byte packetSequence = 17;
            this.outputStream.reset();
            this.sender.send(packet, packetLen, packetSequence);

            // check encoded packet
            int offset = 0;
            byte[] sentPacket = this.outputStream.toByteArray();
            // size of ALL packets written to output stream
            int sizeOfAllPackets = (MysqlaConstants.HEADER_LENGTH * multiPackets) + // header for each full packet + one leftover
                    (MysqlaConstants.MAX_PACKET_SIZE * (multiPackets - 1)) + // FULL packet payloads
                    leftoverPacketLen;
            assertEquals(sizeOfAllPackets, sentPacket.length);

            // check that i=`multiPackets' packets are sent plus one empty packet
            for (int i = 1; i <= multiPackets; ++i) {
                int splitPacketLen = MysqlaConstants.MAX_PACKET_SIZE;
                if (i == multiPackets) { // last packet is empty
                    splitPacketLen = leftoverPacketLen;
                }
                int packetLenInHeader = MysqlaUtils.decodeMysqlThreeByteInteger(sentPacket, offset);
                assertEquals(splitPacketLen, packetLenInHeader);
                assertEquals(packetSequence, sentPacket[offset + MysqlaConstants.HEADER_LENGTH - 1]);
                // check start/end bytes
                assertEquals(10 + i, sentPacket[offset + MysqlaConstants.HEADER_LENGTH]);
                assertEquals(20 + i, sentPacket[offset + MysqlaConstants.HEADER_LENGTH + packetLenInHeader - 1]);
                packetSequence++;
                offset += MysqlaConstants.MAX_PACKET_SIZE + MysqlaConstants.HEADER_LENGTH;
            }
        }
    }

    /**
     * Test the case where the packet size is a multiple of the max packet size. We need to send an extra empty packet to signal that the payload is complete.
     */
    @Test
    public void packetSizeMultipleOfMaxTest() throws IOException {
        // check 1, 2, 3 multiples of MAX_PACKET_SIZE
        for (int multiple = 1; multiple <= 3; ++multiple) {
            final int packetLen = multiple * MysqlaConstants.MAX_PACKET_SIZE;

            byte[] packet = new byte[packetLen];

            byte packetSequence = 40;
            this.outputStream.reset(); // reset as we're using it several times in this test
            this.sender.send(packet, packetLen, packetSequence);

            // check encoded packet
            int offset = 0;
            byte[] sentPacket = this.outputStream.toByteArray();
            // size of ALL packets written to output stream
            int sizeOfAllPackets = (MysqlaConstants.HEADER_LENGTH * (multiple + 1)) + // header for each full packet + one empty
                    (MysqlaConstants.MAX_PACKET_SIZE * multiple); // FULL packet payloads
            assertEquals(sizeOfAllPackets, sentPacket.length);
            // check that `multiple' packets are sent plus one empty packet
            for (int i = 0; i < multiple + 1; ++i) {
                int splitPacketLen = MysqlaConstants.MAX_PACKET_SIZE;
                if (i == multiple) { // last packet is empty
                    splitPacketLen = 0;
                }
                int packetLenInHeader = MysqlaUtils.decodeMysqlThreeByteInteger(sentPacket, offset);
                assertEquals(splitPacketLen, packetLenInHeader);
                assertEquals(packetSequence, sentPacket[offset + MysqlaConstants.HEADER_LENGTH - 1]);
                packetSequence++;
                offset += MysqlaConstants.MAX_PACKET_SIZE + MysqlaConstants.HEADER_LENGTH;
            }
        }
    }
}
