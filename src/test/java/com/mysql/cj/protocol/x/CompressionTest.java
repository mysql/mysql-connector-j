/*
 * Copyright (c) 2020, 2023, Oracle and/or its affiliates.
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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.zip.DeflaterOutputStream;

import org.junit.jupiter.api.Test;

import com.google.protobuf.ByteString;
import com.mysql.cj.x.protobuf.Mysqlx;
import com.mysql.cj.x.protobuf.Mysqlx.ClientMessages;
import com.mysql.cj.x.protobuf.MysqlxConnection.Compression;

public class CompressionTest {

    static final byte[] data1 = ("[[ABCDEFGHIJKLMNOPQRSTUVWXYZ]][[ABCDEFGHIJKLMNOPQRSTUVWXYZ]][[ABCDEFGHIJKLMNOPQRSTUVWXYZ]][[ABCDEFGHIJKLMNOPQRSTUVWXYZ]]"
            + "[[ABCDEFGHIJKLMNOPQRSTUVWXYZ]][[ABCDEFGHIJKLMNOPQRSTUVWXYZ]][[ABCDEFGHIJKLMNOPQRSTUVWXYZ]][[ABCDEFGHIJKLMNOPQRSTUVWXYZ]]"
            + "[[ABCDEFGHIJKLMNOPQRSTUVWXYZ]][[ABCDEFGHIJKLMNOPQRSTUVWXYZ]][[ABCDEFGHIJKLMNOPQRSTUVWXYZ]][[ABCDEFGHIJKLMNOPQRSTUVWXYZ]]"
            + "[[ABCDEFGHIJKLMNOPQRSTUVWXYZ]][[ABCDEFGHIJKLMNOPQRSTUVWXYZ]][[ABCDEFGHIJKLMNOPQRSTUVWXYZ]][[ABCDEFGHIJKLMNOPQRSTUVWXYZ]]"
            + "[[ABCDEFGHIJKLMNOPQRSTUVWXYZ]][[ABCDEFGHIJKLMNOPQRSTUVWXYZ]][[ABCDEFGHIJKLMNOPQRSTUVWXYZ]][[ABCDEFGHIJKLMNOPQRSTUVWXYZ]]").getBytes(); // > 250

    static final byte[] data2 = ("[[ABCDEFGHIJKLMNOPQRSTUVWXYZ]][[ABCDEFGHIJKLMNOPQRSTUVWXYZ]][[ABCDEFGHIJKLMNOPQRSTUVWXYZ]][[ABCDEFGHIJKLMNOPQRSTUVWXYZ]]"
            + "[[ABCDEFGHIJKLMNOPQRSTUVWXYZ]][[ABCDEFGHIJKLMNOPQRSTUVWXYZ]][[ABCDEFGHIJKLMNOPQRSTUVWXYZ]][[ABCDEFGHIJKLMNOPQRSTUVWXYZ]]").getBytes(); // < 250

    static final byte[] uncompressedFrame1;
    static {
        uncompressedFrame1 = new byte[XMessageHeader.HEADER_LENGTH + data1.length];
        ByteBuffer frame = ByteBuffer.wrap(uncompressedFrame1).order(ByteOrder.LITTLE_ENDIAN);
        frame.putInt(XMessageHeader.MESSAGE_TYPE_LENGTH + data1.length); // 4 Bytes: frame length.
        frame.put((byte) 1); // 1 Byte: message type (anything other than 19).
        frame.put(data1);
    }

    static final byte[] uncompressedFrame2;
    static {
        uncompressedFrame2 = new byte[XMessageHeader.HEADER_LENGTH + data2.length];
        ByteBuffer frame = ByteBuffer.wrap(uncompressedFrame2).order(ByteOrder.LITTLE_ENDIAN);
        frame.putInt(XMessageHeader.MESSAGE_TYPE_LENGTH + data2.length); // 4 Bytes: frame length.
        frame.put((byte) 2); // 1 Byte: message type (anything other than 19).
        frame.put(data2);
    }

    static final byte[] downlinkCompressedSingleFrame;
    static final int downlinkCompressedSingleFrame1Length;
    static final int downlinkCompressedSingleFrame2Length;
    static {
        // 1st message.
        ByteArrayOutputStream compressedOut = new ByteArrayOutputStream();
        DeflaterOutputStream deflaterOut = new DeflaterOutputStream(compressedOut, true);
        try {
            deflaterOut.write(uncompressedFrame1);
            deflaterOut.flush();
            // deflaterOut.close(); Deflater can't be closed so that a continuous stream of compressed bytes is possible.
        } catch (Exception e) {
            fail(e.getMessage());
        }
        Compression compressionMessage1 = Compression.newBuilder().setUncompressedSize(uncompressedFrame1.length)
                .setServerMessages(Mysqlx.ServerMessages.Type.forNumber(1)).setPayload(ByteString.copyFrom(compressedOut.toByteArray())).build();
        byte[] compressedData1 = compressionMessage1.toByteArray();
        int compressedLength1 = compressionMessage1.getSerializedSize();
        // 2nd message.
        compressedOut.reset();
        try {
            deflaterOut.write(uncompressedFrame2);
            deflaterOut.flush();
            // deflaterOut.close(); Deflater can't be closed so that a continuous stream of compressed bytes is possible.
        } catch (Exception e) {
            fail(e.getMessage());
        }
        Compression compressionMessage2 = Compression.newBuilder().setUncompressedSize(uncompressedFrame2.length)
                .setServerMessages(Mysqlx.ServerMessages.Type.forNumber(2)).setPayload(ByteString.copyFrom(compressedOut.toByteArray())).build();
        byte[] compressedData2 = compressionMessage2.toByteArray();
        int compressedLength2 = compressionMessage2.getSerializedSize();

        downlinkCompressedSingleFrame = new byte[XMessageHeader.HEADER_LENGTH + compressedLength1 + XMessageHeader.HEADER_LENGTH + compressedLength2];
        ByteBuffer frame = ByteBuffer.wrap(downlinkCompressedSingleFrame).order(ByteOrder.LITTLE_ENDIAN);
        // 1st message.
        downlinkCompressedSingleFrame1Length = XMessageHeader.MESSAGE_TYPE_LENGTH + compressedLength1;
        frame.putInt(downlinkCompressedSingleFrame1Length); // 4 Bytes: frame length.
        frame.put((byte) 19); // 1 Byte: message type (19 = server compression).
        frame.put(compressedData1, 0, compressedLength1);
        // 2nd message.
        downlinkCompressedSingleFrame2Length = XMessageHeader.MESSAGE_TYPE_LENGTH + compressedLength2;
        frame.putInt(downlinkCompressedSingleFrame2Length); // 4 Bytes: frame length.
        frame.put((byte) 19); // 1 Byte: message type (19 = server compression).
        frame.put(compressedData2, 0, compressedLength2);
    }

    static final byte[] downlinkCompressedMultipleFrame;
    static {
        ByteArrayOutputStream compressedOut = new ByteArrayOutputStream();
        DeflaterOutputStream deflaterOut = new DeflaterOutputStream(compressedOut, true);
        try {
            // 1st message.
            deflaterOut.write(uncompressedFrame1);
            deflaterOut.flush();
            // 2nd message.
            deflaterOut.write(uncompressedFrame2);
            deflaterOut.flush();
            // deflaterOut.close(); Deflater can't be closed so that a continuous stream of compressed bytes is possible.
        } catch (Exception e) {
            fail(e.getMessage());
        }
        Compression compressionMessage = Compression.newBuilder().setUncompressedSize(uncompressedFrame1.length + uncompressedFrame2.length)
                .setPayload(ByteString.copyFrom(compressedOut.toByteArray())).build();
        byte[] compressedData = compressionMessage.toByteArray();
        int compressedLength = compressionMessage.getSerializedSize();
        downlinkCompressedMultipleFrame = new byte[XMessageHeader.HEADER_LENGTH + compressedLength];
        ByteBuffer frame = ByteBuffer.wrap(downlinkCompressedMultipleFrame).order(ByteOrder.LITTLE_ENDIAN);
        frame.putInt(XMessageHeader.MESSAGE_TYPE_LENGTH + compressedLength); // 4 Bytes: frame length.
        frame.put((byte) 19); // 1 Byte: message type (19 = server compression).
        frame.put(compressedData, 0, compressedLength);
    }

    static final byte[] uplinkCompressedFrame;
    static {
        ByteArrayOutputStream compressedOut = new ByteArrayOutputStream();
        DeflaterOutputStream deflaterOut = new DeflaterOutputStream(compressedOut, true);
        try {
            deflaterOut.write(uncompressedFrame1);
            deflaterOut.flush();
            // deflaterOut.close(); Deflater can't be closed so that a continuous stream of compressed bytes is possible.
        } catch (Exception e) {
            fail(e.getMessage());
        }
        Compression compressionMessage = Compression.newBuilder().setUncompressedSize(uncompressedFrame1.length)
                .setClientMessages(Mysqlx.ClientMessages.Type.forNumber(1)).setPayload(ByteString.copyFrom(compressedOut.toByteArray())).build();
        byte[] compressedData = compressionMessage.toByteArray();
        int compressedLength = compressionMessage.getSerializedSize();
        uplinkCompressedFrame = new byte[XMessageHeader.HEADER_LENGTH + compressedLength];
        ByteBuffer frame = ByteBuffer.wrap(uplinkCompressedFrame).order(ByteOrder.LITTLE_ENDIAN);
        // 1st message.
        frame.putInt(XMessageHeader.MESSAGE_TYPE_LENGTH + compressedLength); // 4 Bytes: frame length.
        frame.put((byte) ClientMessages.Type.COMPRESSION_VALUE); // 1 Byte: message type (46 = client compression).
        frame.put(compressedData, 0, compressedLength);
    }

    /**
     * Tests that the {@link CompressionSplittedInputStream} reads from original underlying {@link InputStream} when data is not compressed.
     *
     * @throws Exception
     */
    @Test
    public void downlinkCompressionSplittingUncompressed() throws Exception {
        Field field = CompressionSplittedInputStream.class.getDeclaredField("compressorIn");
        field.setAccessible(true);

        ByteArrayOutputStream dataOut = new ByteArrayOutputStream();
        dataOut.write(uncompressedFrame1);
        dataOut.write(uncompressedFrame2);
        ByteArrayInputStream dataIn = new ByteArrayInputStream(dataOut.toByteArray());
        byte[] uncompressedRead = null;
        int remaining;

        try (InputStream compressorIn = new CompressionSplittedInputStream(dataIn,
                new CompressorStreamsFactory(CompressionAlgorithm.getDefaultInstances().get("deflate_stream")))) {
            assertEquals(uncompressedFrame1.length + uncompressedFrame2.length, compressorIn.available());

            // 1st message.
            uncompressedRead = new byte[uncompressedFrame1.length];
            for (int i = 0; i < uncompressedFrame1.length / 3; i++) {
                assertEquals(3, compressorIn.read(uncompressedRead, i * 3, 3)); // Read 3 bytes at a time.

                assertEquals(uncompressedFrame1.length - (i + 1) * 3 + uncompressedFrame2.length, compressorIn.available());
                assertNull(field.get(compressorIn));
            }

            remaining = uncompressedFrame1.length % 3;
            assertEquals(remaining, compressorIn.read(uncompressedRead, uncompressedFrame1.length - remaining, remaining)); // Read remaining bytes.

            assertEquals(uncompressedFrame2.length, compressorIn.available());
            assertNull(field.get(compressorIn));
            assertArrayEquals(uncompressedFrame1, uncompressedRead);

            // 2nd message.
            uncompressedRead = new byte[uncompressedFrame2.length];
            for (int i = 0; i < uncompressedFrame2.length / 3; i++) {
                assertEquals(3, compressorIn.read(uncompressedRead, i * 3, 3)); // Read 3 bytes at a time.

                assertEquals(uncompressedFrame2.length - (i + 1) * 3, compressorIn.available());
                assertNull(field.get(compressorIn));
            }

            remaining = uncompressedFrame2.length % 3;
            assertEquals(remaining, compressorIn.read(uncompressedRead, uncompressedFrame2.length - remaining, remaining)); // Read remaining bytes.

            assertEquals(0, compressorIn.available());
            assertNull(field.get(compressorIn));
            assertArrayEquals(uncompressedFrame2, uncompressedRead);
        }
    }

    /**
     * Tests that the {@link CompressionSplittedInputStream} reads single compressed messages and inflates them properly.
     *
     * @throws Exception
     */
    @Test
    public void downlinkCompressionSingleCompressed() throws Exception {
        Field field = CompressionSplittedInputStream.class.getDeclaredField("compressorIn");
        field.setAccessible(true);

        ByteArrayInputStream dataIn = new ByteArrayInputStream(downlinkCompressedSingleFrame);
        byte[] uncompressedRead = null;
        int remaining;

        try (InputStream compressorIn = new CompressionSplittedInputStream(dataIn,
                new CompressorStreamsFactory(CompressionAlgorithm.getDefaultInstances().get("deflate_stream")))) {
            assertEquals(downlinkCompressedSingleFrame.length, compressorIn.available());

            // 1st message.
            uncompressedRead = new byte[uncompressedFrame1.length];
            for (int i = 0; i < uncompressedFrame1.length / 3; i++) {
                assertEquals(3, compressorIn.read(uncompressedRead, i * 3, 3)); // Read 3 bytes at a time.

                assertEquals(uncompressedFrame1.length - (i + 1) * 3, compressorIn.available());
                assertEquals(ConfinedInputStream.class, field.get(compressorIn).getClass());
            }

            remaining = uncompressedFrame1.length % 3;
            assertEquals(remaining, compressorIn.read(uncompressedRead, uncompressedFrame1.length - remaining, remaining)); // Read remaining bytes.

            assertEquals(XMessageHeader.MESSAGE_SIZE_LENGTH + downlinkCompressedSingleFrame2Length, compressorIn.available());
            assertNull(field.get(compressorIn));
            assertArrayEquals(uncompressedFrame1, uncompressedRead);

            // 2nd message.
            uncompressedRead = new byte[uncompressedFrame2.length];
            for (int i = 0; i < uncompressedFrame2.length / 3; i++) {
                assertEquals(3, compressorIn.read(uncompressedRead, i * 3, 3)); // Read 3 bytes at a time.

                assertEquals(uncompressedFrame2.length - (i + 1) * 3, compressorIn.available());
                assertEquals(ConfinedInputStream.class, field.get(compressorIn).getClass());
            }

            remaining = uncompressedFrame2.length % 3;
            assertEquals(remaining, compressorIn.read(uncompressedRead, uncompressedFrame2.length - remaining, remaining)); // Read remaining bytes.

            assertEquals(0, compressorIn.available());
            assertNull(field.get(compressorIn));
            assertArrayEquals(uncompressedFrame2, uncompressedRead);
        }
    }

    /**
     * Tests that the {@link CompressionSplittedInputStream} reads multiple compressed messages and inflates them properly.
     *
     * @throws Exception
     */
    @Test
    public void downlinkCompressionMultipleCompressed() throws Exception {
        Field field = CompressionSplittedInputStream.class.getDeclaredField("compressorIn");
        field.setAccessible(true);

        ByteArrayInputStream dataIn = new ByteArrayInputStream(downlinkCompressedMultipleFrame);
        byte[] uncompressedRead = null;
        int remaining;

        try (InputStream compressorIn = new CompressionSplittedInputStream(dataIn,
                new CompressorStreamsFactory(CompressionAlgorithm.getDefaultInstances().get("deflate_stream")))) {
            assertEquals(downlinkCompressedMultipleFrame.length, compressorIn.available());

            // 1st message.
            uncompressedRead = new byte[uncompressedFrame1.length];
            for (int i = 0; i < uncompressedFrame1.length / 3; i++) {
                assertEquals(3, compressorIn.read(uncompressedRead, i * 3, 3)); // Read 3 bytes at a time.

                assertEquals(uncompressedFrame1.length - (i + 1) * 3 + uncompressedFrame2.length, compressorIn.available());
                assertEquals(ConfinedInputStream.class, field.get(compressorIn).getClass());
            }

            remaining = uncompressedFrame1.length % 3;
            assertEquals(remaining, compressorIn.read(uncompressedRead, uncompressedFrame1.length - remaining, remaining)); // Read remaining bytes.

            assertEquals(uncompressedFrame2.length, compressorIn.available());
            assertEquals(ConfinedInputStream.class, field.get(compressorIn).getClass());
            assertArrayEquals(uncompressedFrame1, uncompressedRead);

            // 2nd message.
            uncompressedRead = new byte[uncompressedFrame2.length];
            for (int i = 0; i < uncompressedFrame2.length / 3; i++) {
                assertEquals(3, compressorIn.read(uncompressedRead, i * 3, 3)); // Read 3 bytes at a time.

                assertEquals(uncompressedFrame2.length - (i + 1) * 3, compressorIn.available());
                assertEquals(ConfinedInputStream.class, field.get(compressorIn).getClass());
            }

            remaining = uncompressedFrame2.length % 3;
            assertEquals(remaining, compressorIn.read(uncompressedRead, uncompressedFrame2.length - remaining, remaining)); // Read remaining bytes.

            assertEquals(0, compressorIn.available());
            assertNull(field.get(compressorIn));
            assertArrayEquals(uncompressedFrame2, uncompressedRead);
        }
    }

    /**
     * Tests that the {@link CompressionSplittedOutputStream} writes into an underlying OutputStream properly deflated data.
     *
     * @throws Exception
     */
    @Test
    public void uplinkCompressionSplitting() throws Exception {
        Field field = CompressionSplittedOutputStream.class.getDeclaredField("compressionEnabled");
        field.setAccessible(true);

        ByteArrayOutputStream dataOut = new ByteArrayOutputStream();
        byte[] dataWritten = null;
        int remaining;

        try (OutputStream compressorOut = new CompressionSplittedOutputStream(dataOut,
                new CompressorStreamsFactory(CompressionAlgorithm.getDefaultInstances().get("deflate_stream")))) {
            // 1st message gets compressed (length > 250 bytes).
            for (int i = 0; i < uncompressedFrame1.length / 3; i++) {
                compressorOut.write(uncompressedFrame1, i * 3, 3); // Write 3 bytes at a time.

                assertEquals(i != 0, (boolean) field.get(compressorOut));
            }

            remaining = uncompressedFrame1.length % 3;
            compressorOut.write(uncompressedFrame1, uncompressedFrame1.length - remaining, remaining); // Write remaining bytes.

            assertFalse((boolean) field.get(compressorOut));

            dataWritten = dataOut.toByteArray();
            assertArrayEquals(uplinkCompressedFrame, dataWritten);

            // 2st message remains uncompressed (length < 250 bytes).
            dataOut.reset();
            for (int i = 0; i < uncompressedFrame2.length / 3; i++) {
                compressorOut.write(uncompressedFrame2, i * 3, 3); // Write 3 bytes at a time.

                assertFalse((boolean) field.get(compressorOut));
            }

            remaining = uncompressedFrame2.length % 3;
            compressorOut.write(uncompressedFrame2, uncompressedFrame2.length - remaining, remaining); // Write remaining bytes.

            assertFalse((boolean) field.get(compressorOut));

            dataWritten = dataOut.toByteArray();
            assertArrayEquals(uncompressedFrame2, dataWritten);
        }
    }

}
