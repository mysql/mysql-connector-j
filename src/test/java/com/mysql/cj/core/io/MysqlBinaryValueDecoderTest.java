/*
  Copyright (c) 2015, Oracle and/or its affiliates. All rights reserved.

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

package com.mysql.cj.core.io;

import static org.junit.Assert.assertEquals;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import org.junit.Test;

import com.mysql.cj.api.io.ValueFactory;

/**
 * Tests for {@link MysqlBinaryValueDecoder}.
 */
public class MysqlBinaryValueDecoderTest {
    private MysqlBinaryValueDecoder valueDecoder = new MysqlBinaryValueDecoder();

    @Test
    public void testSampleValues() {
        ValueFactory<String> vf = new StringValueFactory();
        String decoded;

        byte[] intTrivial = new byte[] { 1, 0, 0, 0 };
        decoded = this.valueDecoder.decodeInt4(intTrivial, 0, 4, vf);
        assertEquals("1", decoded);

        byte[] intOffset1 = new byte[] { 0x7F, 0x12, 0x34, 0x56, 0x78 };
        decoded = this.valueDecoder.decodeInt4(intOffset1, 1, 4, vf);
        assertEquals("2018915346", decoded);
    }

    @Test
    public void testInt4Limits() {
        ValueFactory<String> vf = new StringValueFactory();
        String decoded;

        byte[] signedInt4Min = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN).putInt(Integer.MIN_VALUE).array();
        decoded = this.valueDecoder.decodeInt4(signedInt4Min, 0, 4, vf);
        assertEquals(String.valueOf(Integer.MIN_VALUE), decoded);

        byte[] signedInt4Max = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN).putInt(Integer.MAX_VALUE).array();
        decoded = this.valueDecoder.decodeInt4(signedInt4Max, 0, 4, vf);
        assertEquals(String.valueOf(Integer.MAX_VALUE), decoded);

        byte[] unsignedInt4Max = ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN).putLong(((long) Integer.MAX_VALUE) * 2 + 1).array();
        decoded = this.valueDecoder.decodeUInt4(unsignedInt4Max, 0, 4, vf);
        assertEquals("4294967295", decoded);
    }

    @Test
    public void testInt8Limits() {
        ValueFactory<String> vf = new StringValueFactory();
        String decoded;

        byte[] signedInt8Min = ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN).putLong(Long.MIN_VALUE).array();
        decoded = this.valueDecoder.decodeInt8(signedInt8Min, 0, 8, vf);
        assertEquals(String.valueOf(Long.MIN_VALUE), decoded);

        byte[] signedInt8Max = ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN).putLong(Long.MAX_VALUE).array();
        decoded = this.valueDecoder.decodeInt8(signedInt8Max, 0, 8, vf);
        assertEquals(String.valueOf(Long.MAX_VALUE), decoded);
        // again with UInt decoder to make sure it's handled correctly
        decoded = this.valueDecoder.decodeUInt8(signedInt8Max, 0, 8, vf);
        assertEquals(String.valueOf(Long.MAX_VALUE), decoded);

        // big-endian version of 2^64-1 (aka Long.MAX_VALUE * 2 + 1)
        byte[] be = BigInteger.valueOf(Long.MAX_VALUE).multiply(new BigInteger("2")).add(new BigInteger("1")).toByteArray();
        // uppermost byte is sign byte
        byte[] unsignedInt8Max = new byte[] { be[8], be[7], be[6], be[5], be[4], be[3], be[2], be[1] };
        assertEquals(8, unsignedInt8Max.length);
        decoded = this.valueDecoder.decodeUInt8(unsignedInt8Max, 0, 8, vf);
        assertEquals("18446744073709551615", decoded);
    }
}
