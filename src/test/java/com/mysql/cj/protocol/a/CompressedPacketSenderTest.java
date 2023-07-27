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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.zip.InflaterOutputStream;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import com.mysql.cj.protocol.MessageSender;

public class CompressedPacketSenderTest extends PacketSenderTestBase {

    private ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    private MessageSender<NativePacketPayload> sender = new CompressedPacketSender(new BufferedOutputStream(this.outputStream));

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
            this.compressedPayloadLen = NativeUtils.decodeMysqlThreeByteInteger(this.packetData, this.offset);
            this.compressedSequenceId = this.packetData[this.offset + 3];
            this.uncompressedPayloadLen = NativeUtils.decodeMysqlThreeByteInteger(this.packetData, this.offset + 4);
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

    @AfterEach
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
        assertEquals(compressedPacketLen - CompressedPacketSender.COMP_HEADER_LENGTH, NativeUtils.decodeMysqlThreeByteInteger(packets.packetData));
        assertEquals(packetSequence, packets.packetData[3]); // compressed sequence is independent
        assertEquals(packetLen + NativeConstants.HEADER_LENGTH, NativeUtils.decodeMysqlThreeByteInteger(packets.packetData, 4));

        // decompress payload and check
        assertTrue(packets.nextPayload());
        assertEquals(packetLen, NativeUtils.decodeMysqlThreeByteInteger(packets.payload));
        assertEquals(packetSequence, packets.payload[3]);
        checkSequentiallyFilledPacket(packets.payload, 4, packetLen);
        assertFalse(packets.nextPayload());
    }

    /**
     * Test the situation where a single packet is split into two and the second part doesn't exceed the capacity of the second compressed packet.
     *
     * @throws IOException
     */
    @Test
    public void basicTwoPartSplitPacketTest() throws IOException {
        final int packetLen = NativeConstants.MAX_PACKET_SIZE + 20000;
        byte[] packet = new byte[packetLen];
        // mark key positions in packet to check split packet
        packet[0] = 41;
        packet[NativeConstants.MAX_PACKET_SIZE - 1] = 42;
        packet[NativeConstants.MAX_PACKET_SIZE] = 43;
        packet[packetLen - 1] = 44;

        final byte packetSequence = 45;
        this.sender.send(packet, packetLen, packetSequence);

        // check encoded packet
        CompressedPackets packets = new CompressedPackets(this.outputStream.toByteArray());

        // first packet
        assertTrue(packets.nextPayload());
        assertEquals(packetSequence, packets.compressedSequenceId);
        assertEquals(NativeConstants.MAX_PACKET_SIZE, packets.uncompressedPayloadLen);
        assertEquals(packets.uncompressedPayloadLen, packets.payload.length);
        assertEquals(41, packets.payload[NativeConstants.HEADER_LENGTH]);
        int firstPacketRawPacketLen = NativeUtils.decodeMysqlThreeByteInteger(packets.payload);
        assertEquals(NativeConstants.MAX_PACKET_SIZE, firstPacketRawPacketLen);
        int firstPacketUncompressedPayloadLen = packets.uncompressedPayloadLen;

        // second packet
        assertTrue(packets.nextPayload());
        assertEquals(packetSequence + 1, packets.compressedSequenceId);
        assertEquals(packetLen - firstPacketUncompressedPayloadLen + 2 * NativeConstants.HEADER_LENGTH, packets.uncompressedPayloadLen);
        assertEquals(packets.uncompressedPayloadLen, packets.payload.length);
        assertEquals(43, packets.payload[NativeConstants.HEADER_LENGTH + NativeConstants.HEADER_LENGTH]);
        assertEquals(42, packets.payload[NativeConstants.HEADER_LENGTH - 1]);
        assertEquals(44, packets.payload[packets.uncompressedPayloadLen - 1]);
        int secondPacketUncompressedPayloadLen = packets.uncompressedPayloadLen;

        assertEquals(packetLen, firstPacketUncompressedPayloadLen + secondPacketUncompressedPayloadLen - 2 * NativeConstants.HEADER_LENGTH);

        // done
        assertFalse(packets.nextPayload());
    }

    /**
     * Test the situation where a single packet is split into two and the second part exceeds the capacity of the second compressed packet requiring a third
     * compressed packet.
     *
     * @throws IOException
     */
    @Test
    public void twoPacketToThreeCompressedPacketNoBoundary() throws IOException {
        final int packetLen = NativeConstants.MAX_PACKET_SIZE * 2 - 1;

        byte[] packet = new byte[packetLen];

        this.sender.send(packet, packetLen, (byte) 0);

        // check encoded packet
        CompressedPackets packets = new CompressedPackets(this.outputStream.toByteArray());

        assertTrue(packets.nextPayload());

        assertTrue(packets.nextPayload());

        // last packet is uncompressed
        // payload is 7 bytes: 4 (from first packet) + 3 (from second packet) bytes
        assertEquals(7, NativeUtils.decodeMysqlThreeByteInteger(packets.packetData, packets.offset));
        assertEquals(2, packets.packetData[packets.offset + 3]); // sequence
        assertEquals(0, NativeUtils.decodeMysqlThreeByteInteger(packets.packetData, packets.offset + 4)); // uncompressed

        assertEquals(CompressedPacketSender.COMP_HEADER_LENGTH + 7, packets.packetData.length - packets.offset);
    }

    /**
     * This tests that the splitting of MySQL packets includes an additional empty packet to signal the end of the multi-packet sequence.
     *
     * @throws IOException
     */
    @Test
    public void twoPacketToThreeWithEmptyUncompressedPacket() throws IOException {
        // it takes three mysql packets to represent a large packet that spans the exact capacity of two packets
        final int packetLen = NativeConstants.MAX_PACKET_SIZE * 2;

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
        assertEquals(0, NativeUtils.decodeMysqlThreeByteInteger(packets.payload, 8)); // payload len
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

        assertEquals(packetLen + NativeConstants.HEADER_LENGTH + CompressedPacketSender.COMP_HEADER_LENGTH, sentPacket.length);
        // header field for compressed payload length should equal uncompressed len + header
        assertEquals(packetLen + NativeConstants.HEADER_LENGTH, NativeUtils.decodeMysqlThreeByteInteger(sentPacket));
        assertEquals(packetSequence, sentPacket[3]);
        // header field for uncompressed payload length should be 0
        assertEquals(0, NativeUtils.decodeMysqlThreeByteInteger(sentPacket, 4));

        assertEquals(packetLen, NativeUtils.decodeMysqlThreeByteInteger(sentPacket, CompressedPacketSender.COMP_HEADER_LENGTH));
        assertEquals(packetSequence, sentPacket[CompressedPacketSender.COMP_HEADER_LENGTH + 3]);
        checkSequentiallyFilledPacket(sentPacket, CompressedPacketSender.COMP_HEADER_LENGTH + NativeConstants.HEADER_LENGTH, packetLen);
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

        assertEquals(packetLen + NativeConstants.HEADER_LENGTH + CompressedPacketSender.COMP_HEADER_LENGTH, sentPacket.length);
        // header field for compressed payload length should equal uncompressed len + header
        assertEquals(packetLen + NativeConstants.HEADER_LENGTH, NativeUtils.decodeMysqlThreeByteInteger(sentPacket));
        assertEquals(packetSequence, sentPacket[3]);
        // header field for uncompressed payload length should be 0
        assertEquals(0, NativeUtils.decodeMysqlThreeByteInteger(sentPacket, 4));

        assertEquals(packetLen, NativeUtils.decodeMysqlThreeByteInteger(sentPacket, CompressedPacketSender.COMP_HEADER_LENGTH));
        assertEquals(packetSequence, sentPacket[CompressedPacketSender.COMP_HEADER_LENGTH + 3]);
        checkSequentiallyFilledPacket(sentPacket, CompressedPacketSender.COMP_HEADER_LENGTH + NativeConstants.HEADER_LENGTH, packetLen);
    }

}
