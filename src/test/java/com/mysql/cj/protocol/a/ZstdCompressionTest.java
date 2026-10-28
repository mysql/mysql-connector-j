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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Random;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.github.luben.zstd.Zstd;
import com.mysql.cj.conf.DefaultPropertySet;
import com.mysql.cj.conf.PropertyKey;
import com.mysql.cj.log.NullLogger;

public class ZstdCompressionTest {

    @BeforeEach
    public void requireZstd() {
        assumeTrue(ZstdCompression.isAvailable(), "zstd-jni is not in the class path");
    }

    private static byte[] packet(int len, long seed) {
        Random random = new Random(seed);
        byte[] packet = new byte[len];
        for (int i = 0; i < len; i++) {
            packet[i] = i % 64 < 48 ? (byte) "SELECT id, status FROM bet WHERE id = ?".charAt(i % 39) : (byte) random.nextInt();
        }
        return packet;
    }

    private static byte[] send(CompressedPacketSender sender, byte[] packet) throws IOException {
        return send(sender, new ByteArrayOutputStream(), packet);
    }

    private static byte[] send(CompressedPacketSender sender, ByteArrayOutputStream wire, byte[] packet) throws IOException {
        wire.reset();
        sender.send(packet, packet.length, (byte) 0);
        return wire.toByteArray();
    }

    private static byte[] send(byte[] packet, int zstdLevel) throws IOException {
        ByteArrayOutputStream wire = new ByteArrayOutputStream();
        CompressedPacketSender sender = new CompressedPacketSender(new BufferedOutputStream(wire), zstdLevel);
        byte[] sent = send(sender, wire, packet);
        sender.stop();
        return sent;
    }

    private static byte[] receive(byte[] wire, boolean zstd) throws IOException {
        CompressedInputStream in = new CompressedInputStream(new ByteArrayInputStream(wire),
                new DefaultPropertySet().getBooleanProperty(PropertyKey.traceProtocol), new NullLogger(null), zstd);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        byte[] chunk = new byte[100000];
        int count;
        while ((count = in.read(chunk, 0, chunk.length)) != -1) {
            out.write(chunk, 0, count);
        }
        in.close();
        return out.toByteArray();
    }

    private static int uncompressedLength(byte[] wire) {
        return (wire[4] & 0xff) + ((wire[5] & 0xff) << 8) + ((wire[6] & 0xff) << 16);
    }

    private static boolean isZstdFrame(byte[] wire) {
        int h = CompressedPacketSender.COMP_HEADER_LENGTH;
        return uncompressedLength(wire) != 0 && (wire[h] & 0xff) == 0x28 && (wire[h + 1] & 0xff) == 0xb5 && (wire[h + 2] & 0xff) == 0x2f
                && (wire[h + 3] & 0xff) == 0xfd;
    }

    private static byte[] zstdPacket(byte[] payload, int announcedLength) {
        byte[] compressed = Zstd.compress(payload, 3);
        byte[] packet = new byte[CompressedPacketSender.COMP_HEADER_LENGTH + compressed.length];
        NativeUtils.encodeMysqlThreeByteInteger(compressed.length, packet, 0);
        NativeUtils.encodeMysqlThreeByteInteger(announcedLength, packet, 4);
        System.arraycopy(compressed, 0, packet, CompressedPacketSender.COMP_HEADER_LENGTH, compressed.length);
        return packet;
    }

    /**
     * Packets sent with zstd read back as the same packet stream as with zlib, including packets split over several compressed packets.
     *
     * @throws Exception
     */
    @Test
    public void packetsRoundTrip() throws Exception {
        for (int len : new int[] { 0, 1, 49, 50, 1000, 65536, NativeConstants.MAX_PACKET_SIZE - 4, NativeConstants.MAX_PACKET_SIZE,
                NativeConstants.MAX_PACKET_SIZE + 1000, 2 * NativeConstants.MAX_PACKET_SIZE }) {
            byte[] packet = packet(len, len);
            byte[] wire = send(packet, 3);
            assertArrayEquals(receive(send(packet, 0), false), receive(wire, true), "packet of " + len + " bytes");
            if (len >= 1000) {
                assertEquals(true, isZstdFrame(wire), "packet of " + len + " bytes");
            }
        }
    }

    /**
     * A packet that zstd cannot shrink is sent uncompressed, and the next packet is compressed into a new frame.
     *
     * @throws Exception
     */
    @Test
    public void incompressiblePacketsAreSentUncompressed() throws Exception {
        ByteArrayOutputStream wire = new ByteArrayOutputStream();
        CompressedPacketSender sender = new CompressedPacketSender(new BufferedOutputStream(wire), 3);
        byte[] random = new byte[4096];
        new Random(1).nextBytes(random);
        byte[] compressible = packet(4096, 3);
        for (int i = 0; i < 3; i++) {
            byte[] sent = send(sender, wire, random);
            assertEquals(0, uncompressedLength(sent));
            assertArrayEquals(receive(send(random, 0), false), receive(sent, true));

            sent = send(sender, wire, compressible);
            assertEquals(true, isZstdFrame(sent));
            assertArrayEquals(receive(send(compressible, 0), false), receive(sent, true));
        }
        sender.stop();
    }

    /**
     * A zstd packet that uncompresses to fewer or more bytes than its header announces is not passed on. The stream reports the failure as its end, which
     * the protocol turns into a communications error.
     *
     * @throws Exception
     */
    @Test
    public void packetsWithAWrongLengthAreRejected() throws Exception {
        byte[] payload = packet(1000, 2);
        assertArrayEquals(payload, receive(zstdPacket(payload, payload.length), true));
        assertEquals(0, receive(zstdPacket(payload, payload.length + 1), true).length);
        assertEquals(0, receive(zstdPacket(payload, payload.length - 1), true).length);
    }

    /**
     * The sender compresses at the given level.
     *
     * @throws Exception
     */
    @Test
    public void levelIsApplied() throws Exception {
        byte[] packet = packet(65536, 4);
        assertNotEquals(send(packet, 1).length, send(packet, 19).length);
    }

    /**
     * zstd is not available without zstd-jni, nor with zstd-jni but without its native library.
     *
     * @throws Exception
     */
    @Test
    public void unavailableWithoutZstdJniOrItsNativeLibrary() throws Exception {
        URL driver = ZstdCompression.class.getProtectionDomain().getCodeSource().getLocation();
        URL zstdJni = Zstd.class.getProtectionDomain().getCodeSource().getLocation();
        try (URLClassLoader withoutZstdJni = new URLClassLoader(new URL[] { driver }, null);
                URLClassLoader withoutNativeLibrary = new URLClassLoader(new URL[] { driver, zstdJni }, null) {

                    @Override
                    public URL findResource(String name) {
                        return name.contains("zstd-jni") ? null : super.findResource(name);
                    }

                }) {
            assertFalse(isAvailable(withoutZstdJni));
            assertFalse(isAvailable(withoutNativeLibrary));
        }
    }

    private static boolean isAvailable(ClassLoader classLoader) throws Exception {
        return (Boolean) classLoader.loadClass(ZstdCompression.class.getName()).getMethod("isAvailable").invoke(null);
    }

}
