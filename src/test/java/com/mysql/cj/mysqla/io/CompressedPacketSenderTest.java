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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.zip.InflaterOutputStream;

import org.junit.After;
import org.junit.Test;

import com.mysql.cj.api.mysqla.io.PacketSender;
import com.mysql.cj.mysqla.MysqlaConstants;
import com.mysql.cj.mysqla.MysqlaUtils;

public class CompressedPacketSenderTest extends PacketSenderTestBase {
    private ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    private PacketSender sender = new CompressedPacketSender(new BufferedOutputStream(this.outputStream));

    /**
     * Test utility to transform a buffer containing compressed packets into a sequence of payloads.
     */
    static class CompressedPackets {
        byte[] packetData;
        private ByteArrayOutputStream decompressedStream;

        byte[] payload;
        int compressedPayloadLen;
        int compressedSequenceId;
        int uncompressedPayloadLen;
        int offset = 0; // offset after all currently read data

        public CompressedPackets(byte[] packetData) {
            this.packetData = packetData;
            this.decompressedStream = new ByteArrayOutputStream();
        }

        public boolean nextPayload() throws IOException {
            if (this.offset == this.packetData.length) {
                return false;
            }
            // read compressed packet header
            this.compressedPayloadLen = MysqlaUtils.decodeMysqlThreeByteInteger(this.packetData, this.offset);
            this.compressedSequenceId = this.packetData[this.offset + 3];
            this.uncompressedPayloadLen = MysqlaUtils.decodeMysqlThreeByteInteger(this.packetData, this.offset + 4);
            this.offset += CompressedPacketSender.COMP_HEADER_LENGTH;
            if (this.uncompressedPayloadLen == 0) {
                // uncompressed packet
                this.payload = java.util.Arrays.copyOfRange(this.packetData, this.offset, this.offset + this.compressedPayloadLen);
            } else {
                // uncompress payload
                InflaterOutputStream inflater = new InflaterOutputStream(this.decompressedStream);
                inflater.write(this.packetData, this.offset, this.compressedPayloadLen);
                inflater.finish();
                inflater.flush();
                this.payload = this.decompressedStream.toByteArray();
                this.decompressedStream.reset();
            }
            this.offset += this.compressedPayloadLen;
            return true;
        }
    }

    @After
    public void cleanupByteArrayOutputStream() {
        this.outputStream.reset();
    }

    @Test
    public void basicCompressedPacketTest() throws IOException {
        final int packetLen = 3000; // needs to be big enough to compress

        byte[] packet = new byte[packetLen];
        fillPacketSequentially(packet);

        final byte packetSequence = 22;
        this.sender.send(packet, packetLen, packetSequence);

        // check encoded packet
        CompressedPackets packets = new CompressedPackets(this.outputStream.toByteArray());

        final int compressedPacketLen = 316; // expected value generated from this test case - compression is deterministic
        assertEquals(compressedPacketLen, packets.packetData.length);
        assertEquals(compressedPacketLen - CompressedPacketSender.COMP_HEADER_LENGTH, MysqlaUtils.decodeMysqlThreeByteInteger(packets.packetData));
        assertEquals(packetSequence, packets.packetData[3]); // compressed sequence is independent
        assertEquals(packetLen + MysqlaConstants.HEADER_LENGTH, MysqlaUtils.decodeMysqlThreeByteInteger(packets.packetData, 4));

        // decompress payload and check
        assertTrue(packets.nextPayload());
        assertEquals(packetLen, MysqlaUtils.decodeMysqlThreeByteInteger(packets.payload));
        assertEquals(packetSequence, packets.payload[3]);
        checkSequentiallyFilledPacket(packets.payload, 4, packetLen);
        assertFalse(packets.nextPayload());
    }

    /**
     * Test the situation where a single packet is split into two and the second part doesn't exceed the capacity of the second compressed packet.
     */
    @Test
    public void basicTwoPartSplitPacketTest() throws IOException {
        final int packetLen = MysqlaConstants.MAX_PACKET_SIZE + 20000;
        byte[] packet = new byte[packetLen];
        // mark key positions in packet to check split packet
        packet[0] = 41;
        packet[MysqlaConstants.MAX_PACKET_SIZE - 1] = 42;
        packet[MysqlaConstants.MAX_PACKET_SIZE] = 43;
        packet[packetLen - 1] = 44;

        final byte packetSequence = 45;
        this.sender.send(packet, packetLen, packetSequence);

        // check encoded packet
        CompressedPackets packets = new CompressedPackets(this.outputStream.toByteArray());

        // first packet
        assertTrue(packets.nextPayload());
        assertEquals(packetSequence, packets.compressedSequenceId);
        assertEquals(MysqlaConstants.MAX_PACKET_SIZE, packets.uncompressedPayloadLen);
        assertEquals(packets.uncompressedPayloadLen, packets.payload.length);
        assertEquals(41, packets.payload[MysqlaConstants.HEADER_LENGTH]);
        int firstPacketRawPacketLen = MysqlaUtils.decodeMysqlThreeByteInteger(packets.payload);
        assertEquals(MysqlaConstants.MAX_PACKET_SIZE, firstPacketRawPacketLen);
        int firstPacketUncompressedPayloadLen = packets.uncompressedPayloadLen;

        // second packet
        assertTrue(packets.nextPayload());
        assertEquals(packetSequence + 1, packets.compressedSequenceId);
        assertEquals(packetLen - firstPacketUncompressedPayloadLen + (2 * MysqlaConstants.HEADER_LENGTH), packets.uncompressedPayloadLen);
        assertEquals(packets.uncompressedPayloadLen, packets.payload.length);
        assertEquals(43, packets.payload[MysqlaConstants.HEADER_LENGTH + MysqlaConstants.HEADER_LENGTH]);
        assertEquals(42, packets.payload[MysqlaConstants.HEADER_LENGTH - 1]);
        assertEquals(44, packets.payload[packets.uncompressedPayloadLen - 1]);
        int secondPacketUncompressedPayloadLen = packets.uncompressedPayloadLen;

        assertEquals(packetLen, firstPacketUncompressedPayloadLen + secondPacketUncompressedPayloadLen - (2 * MysqlaConstants.HEADER_LENGTH));

        // done
        assertFalse(packets.nextPayload());
    }

    /**
     * Test the situation where a single packet is split into two and the second part exceeds the capacity of the second compressed packet requiring a third
     * compressed packet.
     */
    @Test
    public void twoPacketToThreeCompressedPacketNoBoundary() throws IOException {
        final int packetLen = (MysqlaConstants.MAX_PACKET_SIZE * 2) - 1;

        byte[] packet = new byte[packetLen];

        this.sender.send(packet, packetLen, (byte) 0);

        // check encoded packet
        CompressedPackets packets = new CompressedPackets(this.outputStream.toByteArray());

        assertTrue(packets.nextPayload());

        assertTrue(packets.nextPayload());

        // last packet is uncompressed
        // payload is 7 bytes: 4 (from first packet) + 3 (from second packet) bytes
        assertEquals(7, MysqlaUtils.decodeMysqlThreeByteInteger(packets.packetData, packets.offset));
        assertEquals(2, packets.packetData[packets.offset + 3]); // sequence
        assertEquals(0, MysqlaUtils.decodeMysqlThreeByteInteger(packets.packetData, packets.offset + 4)); // uncompressed

        assertEquals(CompressedPacketSender.COMP_HEADER_LENGTH + 7, packets.packetData.length - packets.offset);
    }

    /**
     * This tests that the splitting of MySQL packets includes an additional empty packet to signal the end of the multi-packet sequence.
     */
    @Test
    public void twoPacketToThreeWithEmptyUncompressedPacket() throws IOException {
        // it takes three mysql packets to represent a large packet that spans the exact capacity of two packets
        final int packetLen = MysqlaConstants.MAX_PACKET_SIZE * 2;

        byte[] packet = new byte[packetLen];
        // seed data to check packets after splitting & compression
        packet[packetLen - 4] = 22;
        packet[packetLen - 3] = 23;
        packet[packetLen - 2] = 24;
        packet[packetLen - 1] = 25;

        this.sender.send(packet, packetLen, (byte) 0);

        // check encoded packet
        CompressedPackets packets = new CompressedPackets(this.outputStream.toByteArray());

        assertTrue(packets.nextPayload());
        assertTrue(packets.nextPayload());
        assertTrue(packets.nextPayload());

        // third packet includes remaining 8 bytes of data and the blank header for the third mysql packet

        // last packet is uncompressed
        // payload is: 4 (bumped from first packet) + 4 (bumped from second packet) bytes + empty header (4)
        assertEquals(12, packets.compressedPayloadLen);
        assertEquals(0, packets.uncompressedPayloadLen); // uncompressed indicator
        // last four bytes of original packet should be second four bytes here
        assertEquals(22, packets.payload[4]);
        assertEquals(23, packets.payload[5]);
        assertEquals(24, packets.payload[6]);
        assertEquals(25, packets.payload[7]);

        // third MySQL packet is an empty header
        assertEquals(2, packets.payload[11]); // sequence
        assertEquals(0, MysqlaUtils.decodeMysqlThreeByteInteger(packets.payload, 8)); // payload len
    }

    @Test
    public void smallPacketsArentCompressed() throws IOException {
        final int packetLen = CompressedPacketSender.MIN_COMPRESS_LEN - 1; // needs to be big enough to compress

        byte[] packet = new byte[packetLen];
        fillPacketSequentially(packet);

        final byte packetSequence = 33;
        this.sender.send(packet, packetLen, packetSequence);

        // check encoded packet
        byte[] sentPacket = this.outputStream.toByteArray();

        assertEquals(packetLen + MysqlaConstants.HEADER_LENGTH + CompressedPacketSender.COMP_HEADER_LENGTH, sentPacket.length);
        // header field for compressed payload length should equal uncompressed len + header
        assertEquals(packetLen + MysqlaConstants.HEADER_LENGTH, MysqlaUtils.decodeMysqlThreeByteInteger(sentPacket));
        assertEquals(packetSequence, sentPacket[3]);
        // header field for uncompressed payload length should be 0
        assertEquals(0, MysqlaUtils.decodeMysqlThreeByteInteger(sentPacket, 4));

        assertEquals(packetLen, MysqlaUtils.decodeMysqlThreeByteInteger(sentPacket, CompressedPacketSender.COMP_HEADER_LENGTH));
        assertEquals(packetSequence, sentPacket[CompressedPacketSender.COMP_HEADER_LENGTH + 3]);
        checkSequentiallyFilledPacket(sentPacket, CompressedPacketSender.COMP_HEADER_LENGTH + MysqlaConstants.HEADER_LENGTH, packetLen);
    }

    @Test
    public void incompressiblePacketsArentCompressed() throws IOException {
        final int packetLen = CompressedPacketSender.MIN_COMPRESS_LEN * 2; // needs to be big enough to compress

        byte[] packet = new byte[packetLen];
        // this sequential data is not easily compressible by the DEFLATE algorithm
        fillPacketSequentially(packet);

        final byte packetSequence = 33;
        this.sender.send(packet, packetLen, packetSequence);

        // check encoded packet
        byte[] sentPacket = this.outputStream.toByteArray();

        assertEquals(packetLen + MysqlaConstants.HEADER_LENGTH + CompressedPacketSender.COMP_HEADER_LENGTH, sentPacket.length);
        // header field for compressed payload length should equal uncompressed len + header
        assertEquals(packetLen + MysqlaConstants.HEADER_LENGTH, MysqlaUtils.decodeMysqlThreeByteInteger(sentPacket));
        assertEquals(packetSequence, sentPacket[3]);
        // header field for uncompressed payload length should be 0
        assertEquals(0, MysqlaUtils.decodeMysqlThreeByteInteger(sentPacket, 4));

        assertEquals(packetLen, MysqlaUtils.decodeMysqlThreeByteInteger(sentPacket, CompressedPacketSender.COMP_HEADER_LENGTH));
        assertEquals(packetSequence, sentPacket[CompressedPacketSender.COMP_HEADER_LENGTH + 3]);
        checkSequentiallyFilledPacket(sentPacket, CompressedPacketSender.COMP_HEADER_LENGTH + MysqlaConstants.HEADER_LENGTH, packetLen);
    }
}
