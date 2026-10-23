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

import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

import com.mysql.cj.exceptions.CJCommunicationsException;
import com.mysql.cj.protocol.a.NativeConstants.IntegerDataType;

/**
 * Tests for text and binary result set readers.
 */
public class ResultsetReaderTest {

    private static final long MAX_COLUMN_COUNT = 65535L;

    @Test
    public void textReaderRejectsColumnCountAboveMaximum() {
        TextResultsetReader reader = new TextResultsetReader(null);
        assertThrows(CJCommunicationsException.class, () -> reader.read(-1, false, columnCountPacket(MAX_COLUMN_COUNT + 1), null, null));
    }

    @Test
    public void textReaderRejectsResourceExhaustingColumnCount() {
        TextResultsetReader reader = new TextResultsetReader(null);
        assertThrows(CJCommunicationsException.class, () -> reader.read(-1, false, columnCountPacket(Integer.MAX_VALUE), null, null));
    }

    @Test
    public void textReaderRejectsOverflowingColumnCount() {
        TextResultsetReader reader = new TextResultsetReader(null);
        assertThrows(CJCommunicationsException.class, () -> reader.read(-1, false, columnCountPacket((long) Integer.MAX_VALUE + 1), null, null));
    }

    @Test
    public void textReaderRejectsUnsignedColumnCountOutsideLongRange() {
        TextResultsetReader reader = new TextResultsetReader(null);
        NativePacketPayload resultPacket = new NativePacketPayload(new byte[] { (byte) 0xfe, 0, 0, 0, 0, 0, 0, 0, (byte) 0x80 });
        assertThrows(CJCommunicationsException.class, () -> reader.read(-1, false, resultPacket, null, null));
    }

    @Test
    public void textReaderRejectsOverlongZeroColumnCount() {
        TextResultsetReader reader = new TextResultsetReader(null);
        NativePacketPayload resultPacket = new NativePacketPayload(new byte[] { (byte) 0xfc, 0, 0 });
        assertThrows(CJCommunicationsException.class, () -> reader.read(-1, false, resultPacket, null, null));
    }

    @Test
    public void textReaderAcceptsCanonicalOkPacket() {
        TextResultsetReader reader = new TextResultsetReader(null);
        NativePacketPayload resultPacket = new NativePacketPayload(new byte[] { 0, 0, 0, 2, 0, 0, 0 });

        // A null protocol is a sentinel: reaching it proves that the packet was classified as an OK packet.
        assertThrows(NullPointerException.class, () -> reader.read(-1, false, resultPacket, null, null));
    }

    @Test
    public void textReaderRejectsTruncatedOkPacket() {
        TextResultsetReader reader = new TextResultsetReader(null);
        NativePacketPayload resultPacket = new NativePacketPayload(new byte[] { 0 });
        assertThrows(CJCommunicationsException.class, () -> reader.read(-1, false, resultPacket, null, null));
    }

    @Test
    public void binaryReaderRejectsColumnCountAboveMaximum() {
        BinaryResultsetReader reader = new BinaryResultsetReader(null);
        assertThrows(CJCommunicationsException.class, () -> reader.read(-1, false, columnCountPacket(MAX_COLUMN_COUNT + 1), null, null));
    }

    @Test
    public void binaryReaderRejectsResourceExhaustingColumnCount() {
        BinaryResultsetReader reader = new BinaryResultsetReader(null);
        assertThrows(CJCommunicationsException.class, () -> reader.read(-1, false, columnCountPacket(Integer.MAX_VALUE), null, null));
    }

    @Test
    public void binaryReaderRejectsOverflowingColumnCount() {
        BinaryResultsetReader reader = new BinaryResultsetReader(null);
        assertThrows(CJCommunicationsException.class, () -> reader.read(-1, false, columnCountPacket((long) Integer.MAX_VALUE + 1), null, null));
    }

    @Test
    public void binaryReaderRejectsUnsignedColumnCountOutsideLongRange() {
        BinaryResultsetReader reader = new BinaryResultsetReader(null);
        NativePacketPayload resultPacket = new NativePacketPayload(new byte[] { (byte) 0xfe, 0, 0, 0, 0, 0, 0, 0, (byte) 0x80 });
        assertThrows(CJCommunicationsException.class, () -> reader.read(-1, false, resultPacket, null, null));
    }

    @Test
    public void binaryReaderRejectsOverlongZeroColumnCount() {
        BinaryResultsetReader reader = new BinaryResultsetReader(null);
        NativePacketPayload resultPacket = new NativePacketPayload(new byte[] { (byte) 0xfc, 0, 0 });
        assertThrows(CJCommunicationsException.class, () -> reader.read(-1, false, resultPacket, null, null));
    }

    @Test
    public void binaryReaderAcceptsCanonicalOkPacket() {
        BinaryResultsetReader reader = new BinaryResultsetReader(null);
        NativePacketPayload resultPacket = new NativePacketPayload(new byte[] { 0, 0, 0, 2, 0, 0, 0 });

        // A null protocol is a sentinel: reaching it proves that the packet was classified as an OK packet.
        assertThrows(NullPointerException.class, () -> reader.read(-1, false, resultPacket, null, null));
    }

    @Test
    public void binaryReaderRejectsTruncatedOkPacket() {
        BinaryResultsetReader reader = new BinaryResultsetReader(null);
        NativePacketPayload resultPacket = new NativePacketPayload(new byte[] { 0 });
        assertThrows(CJCommunicationsException.class, () -> reader.read(-1, false, resultPacket, null, null));
    }

    @Test
    public void textReaderAcceptsMaximumColumnCount() {
        TextResultsetReader reader = new TextResultsetReader(null);

        // A null protocol is a sentinel: reaching it proves that the column count passed validation.
        assertThrows(NullPointerException.class, () -> reader.read(-1, false, columnCountPacket(MAX_COLUMN_COUNT), null, null));
    }

    @Test
    public void binaryReaderAcceptsMaximumColumnCount() {
        BinaryResultsetReader reader = new BinaryResultsetReader(null);

        // A null protocol is a sentinel: reaching it proves that the column count passed validation.
        assertThrows(NullPointerException.class, () -> reader.read(-1, false, columnCountPacket(MAX_COLUMN_COUNT), null, null));
    }

    private NativePacketPayload columnCountPacket(long columnCount) {
        NativePacketPayload packet = new NativePacketPayload(9);
        packet.writeInteger(IntegerDataType.INT_LENENC, columnCount);
        packet.setPosition(0);
        return packet;
    }

}
