/*
 * Copyright (c) 2014, 2024, Oracle and/or its affiliates.
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

package com.mysql.cj.util;

/**
 * This decoder implements standard Base64 decoding except it allows and silently ignores non-base64 input characters (spaces, line breaks etc)
 *
 * Note: Java 6+ provide standard decoders
 */
public class Base64Decoder {

    /*
     * -1 means non-base64 character
     * -2 means padding
     */
    private static byte[] decoderMap = new byte[] { -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
            -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, 62, -1, -1, -1, 63, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61, -1, -1, -1, -2, -1, -1,
            -1, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, -1, -1, -1, -1, -1, -1, 26, 27, 28, 29, 30, 31,
            32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, -1, -1, -1, -1, -1 };

    public static class IntWrapper {

        public int value;

        public IntWrapper(int value) {
            this.value = value;
        }

    }

    private static byte getNextValidByte(byte[] in, IntWrapper pos, int maxPos) {
        while (pos.value <= maxPos) {
            if (in[pos.value] >= 0 && decoderMap[in[pos.value]] >= 0) {
                return in[pos.value++];
            }
            pos.value++;
        }
        // padding if reached max position
        return '=';
    }

    public static byte[] decode(byte[] in, int pos, int length) {
        IntWrapper offset = new Base64Decoder.IntWrapper(pos);
        byte[] sestet = new byte[4];

        int outLen = length * 3 / 4; // over-estimated if non-base64 characters present
        byte[] octet = new byte[outLen];
        int octetId = 0;

        int maxPos = offset.value + length - 1;
        while (offset.value <= maxPos) {
            sestet[0] = decoderMap[getNextValidByte(in, offset, maxPos)];
            sestet[1] = decoderMap[getNextValidByte(in, offset, maxPos)];
            sestet[2] = decoderMap[getNextValidByte(in, offset, maxPos)];
            sestet[3] = decoderMap[getNextValidByte(in, offset, maxPos)];

            if (sestet[1] != -2) {
                octet[octetId++] = (byte) (sestet[0] << 2 | sestet[1] >>> 4);
            }
            if (sestet[2] != -2) {
                octet[octetId++] = (byte) ((sestet[1] & 0xf) << 4 | sestet[2] >>> 2);
            }
            if (sestet[3] != -2) {
                octet[octetId++] = (byte) ((sestet[2] & 3) << 6 | sestet[3]);
            }
        }
        // return real-length value
        byte[] out = new byte[octetId];
        System.arraycopy(octet, 0, out, 0, octetId);
        return out;
    }

}
