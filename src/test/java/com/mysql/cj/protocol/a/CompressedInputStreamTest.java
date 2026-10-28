/*
 * Copyright (c) 2026, Oracle and/or its affiliates.
 *
 * This program is free software; you can redistribute it and/or modify it under the terms of the GNU General Public License, version 2.0, as published by
 * the Free Software Foundation.
 *
 * This program is designed to work with certain software that is licensed under separate terms, as designated in a particular file or component or in
 * included license documentation. The authors of MySQL hereby grant you an additional permission to link the program and your derivative works with the
 * separately licensed software that they have either included with the program or referenced in the documentation.
 *
 * Without limiting anything contained in the foregoing, this file, which is part of MySQL Connector/J, is also subject to the Universal FOSS Exception,
 * version 1.0, a copy of which can be found at http://oss.oracle.com/licenses/universal-foss-exception.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License, version 2.0, for more details.
 *
 * You should have received a copy of the GNU General Public License along with this program; if not, write to the Free Software Foundation, Inc.,
 * 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA
 */

package com.mysql.cj.protocol.a;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Random;
import java.util.zip.Deflater;

import org.junit.jupiter.api.Test;

import com.mysql.cj.conf.DefaultPropertySet;
import com.mysql.cj.conf.PropertyKey;
import com.mysql.cj.conf.RuntimeProperty;
import com.mysql.cj.log.NullLogger;

public class CompressedInputStreamTest {

    private static byte[] packet(int len, long seed) {
        Random random = new Random(seed);
        byte[] packet = new byte[len];
        for (int i = 0; i < len; i++) {
            packet[i] = i % 64 < 48 ? (byte) "SELECT id, status FROM bet WHERE id = ?".charAt(i % 39) : (byte) random.nextInt();
        }
        return packet;
    }

    private static CompressedInputStream stream(byte[] wire) {
        return stream(wire, false);
    }

    private static CompressedInputStream stream(byte[] wire, boolean traceProtocol) {
        RuntimeProperty<Boolean> trace = new DefaultPropertySet().getBooleanProperty(PropertyKey.traceProtocol);
        trace.setValue(traceProtocol);
        return new CompressedInputStream(new ByteArrayInputStream(wire), trace, new NullLogger(null));
    }

    private static byte[] concat(byte[]... parts) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        for (byte[] part : parts) {
            out.write(part, 0, part.length);
        }
        return out.toByteArray();
    }

    private static byte[] read(CompressedInputStream in, int length, int chunk) throws IOException {
        byte[] data = new byte[length];
        int offset = 0;
        while (offset < length) {
            int count = in.read(data, offset, Math.min(chunk, length - offset));
            if (count == -1) {
                return Arrays.copyOf(data, offset);
            }
            offset += count;
        }
        return data;
    }

    private static byte[] compressedPacket(byte[] payload, int announcedLength) {
        Deflater deflater = new Deflater();
        deflater.setInput(payload);
        deflater.finish();
        byte[] deflated = new byte[payload.length + 64];
        int deflatedLength = deflater.deflate(deflated);
        deflater.end();

        byte[] packet = new byte[CompressedPacketSender.COMP_HEADER_LENGTH + deflatedLength];
        NativeUtils.encodeMysqlThreeByteInteger(deflatedLength, packet, 0);
        NativeUtils.encodeMysqlThreeByteInteger(announcedLength, packet, 4);
        System.arraycopy(deflated, 0, packet, CompressedPacketSender.COMP_HEADER_LENGTH, deflatedLength);
        return packet;
    }

    @Test
    public void packetsOfAllSizesRoundTripThroughOneStream() throws IOException {
        int[] lengths = { 0, 1, CompressedPacketSender.MIN_COMPRESS_LEN - 1, CompressedPacketSender.MIN_COMPRESS_LEN, 3000, 70000, 600,
                NativeConstants.MAX_PACKET_SIZE - 4, NativeConstants.MAX_PACKET_SIZE + 20000, 600 };
        byte[] incompressible = new byte[2000];
        new Random(0).nextBytes(incompressible);

        ByteArrayOutputStream compressed = new ByteArrayOutputStream();
        CompressedPacketSender compressedSender = new CompressedPacketSender(new BufferedOutputStream(compressed));
        ByteArrayOutputStream plain = new ByteArrayOutputStream();
        SimplePacketSender plainSender = new SimplePacketSender(new BufferedOutputStream(plain));
        for (int i = 0; i <= lengths.length; i++) {
            byte[] packet = i < lengths.length ? packet(lengths[i], i) : incompressible;
            compressedSender.send(packet, packet.length, (byte) 0);
            plainSender.send(packet, packet.length, (byte) 0);
        }

        byte[] expected = plain.toByteArray();
        for (int chunk : new int[] { 7, 4096, 100000 }) {
            CompressedInputStream in = stream(compressed.toByteArray());
            assertArrayEquals(expected, read(in, expected.length, chunk), "chunk " + chunk);
            assertEquals(-1, in.read());
        }
    }

    @Test
    public void packetThatInflatesToTheAnnouncedLengthIsRead() throws IOException {
        byte[] payload = packet(3000, 1);
        assertArrayEquals(payload, read(stream(compressedPacket(payload, payload.length)), payload.length, 8192));
    }

    @Test
    public void packetThatInflatesToFewerBytesIsRejected() throws IOException {
        byte[] payload = packet(3000, 2);
        assertEquals(-1, stream(compressedPacket(payload, payload.length + 1)).read(new byte[8192]));
    }

    @Test
    public void packetThatInflatesToMoreBytesIsRejected() throws IOException {
        byte[] payload = packet(3000, 3);
        assertEquals(-1, stream(compressedPacket(payload, payload.length - 1)).read(new byte[8192]));
    }

    @Test
    public void packetCompressedFromOneByteIsRead() throws IOException {
        assertEquals(0x5A, stream(compressedPacket(new byte[] { 0x5A }, 1)).read());
    }

    @Test
    public void readsThatLeaveOneByteOfAPacketKeepTheBytesInOrder() throws IOException {
        byte[] first = packet(600, 4);
        byte[] second = packet(600, 5);
        byte[] wire = concat(compressedPacket(first, first.length), compressedPacket(second, second.length));
        assertArrayEquals(concat(first, second), read(stream(wire), 1200, 599));
    }

    @Test
    public void availableCountsTheUnreadBytesOfThePacketAndOfTheServerStream() throws IOException {
        byte[] next = compressedPacket(packet(600, 6), 600);
        CompressedInputStream in = stream(concat(compressedPacket(packet(3000, 7), 3000), next));
        read(in, 1000, 1000);
        assertEquals(2000 + next.length, in.available());
    }

    @Test
    public void largePacketIsReadWithProtocolTracing() throws IOException {
        byte[] payload = packet(3000, 8);
        assertArrayEquals(payload, read(stream(compressedPacket(payload, payload.length), true), payload.length, 8192));
    }

}
