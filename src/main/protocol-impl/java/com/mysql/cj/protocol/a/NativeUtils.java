/*
 * Copyright (c) 2015, 2020, Oracle and/or its affiliates.
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

import com.mysql.cj.MysqlType;

/**
 * Utilities to manipulate MySQL protocol-specific formats.
 */
public class NativeUtils {
    private NativeUtils() {
    }

    public static byte[] encodeMysqlThreeByteInteger(int i) {
        byte[] b = new byte[3];
        b[0] = (byte) (i & 0xff);
        b[1] = (byte) (i >>> 8);
        b[2] = (byte) (i >>> 16);
        return b;
    }

    public static void encodeMysqlThreeByteInteger(int i, byte[] b, int offset) {
        b[offset++] = (byte) (i & 0xff);
        b[offset++] = (byte) (i >>> 8);
        b[offset] = (byte) (i >>> 16);
    }

    public static int decodeMysqlThreeByteInteger(byte[] b) {
        return decodeMysqlThreeByteInteger(b, 0);
    }

    public static int decodeMysqlThreeByteInteger(byte[] b, int offset) {
        return (b[offset + 0] & 0xff) + ((b[offset + 1] & 0xff) << 8) + ((b[offset + 2] & 0xff) << 16);
    }

    /**
     * Get the length of a binary-encoded value of the given type.
     * 
     * @param type
     *            type
     * 
     * @return the length (&gt;0), 0 for a length-prefixed type, or -1 for unknown
     */
    public static int getBinaryEncodedLength(int type) {
        switch (type) {
            case MysqlType.FIELD_TYPE_TINY:
                return 1;
            case MysqlType.FIELD_TYPE_SHORT:
            case MysqlType.FIELD_TYPE_YEAR:
                return 2;
            case MysqlType.FIELD_TYPE_LONG:
            case MysqlType.FIELD_TYPE_INT24:
            case MysqlType.FIELD_TYPE_FLOAT:
                return 4;
            case MysqlType.FIELD_TYPE_LONGLONG:
            case MysqlType.FIELD_TYPE_DOUBLE:
                return 8;
            case MysqlType.FIELD_TYPE_TIME:
            case MysqlType.FIELD_TYPE_DATE:
            case MysqlType.FIELD_TYPE_DATETIME:
            case MysqlType.FIELD_TYPE_TIMESTAMP:
            case MysqlType.FIELD_TYPE_TINY_BLOB:
            case MysqlType.FIELD_TYPE_MEDIUM_BLOB:
            case MysqlType.FIELD_TYPE_LONG_BLOB:
            case MysqlType.FIELD_TYPE_BLOB:
            case MysqlType.FIELD_TYPE_VAR_STRING:
            case MysqlType.FIELD_TYPE_VARCHAR:
            case MysqlType.FIELD_TYPE_STRING:
            case MysqlType.FIELD_TYPE_DECIMAL:
            case MysqlType.FIELD_TYPE_NEWDECIMAL:
            case MysqlType.FIELD_TYPE_GEOMETRY:
            case MysqlType.FIELD_TYPE_BIT:
            case MysqlType.FIELD_TYPE_JSON:
            case MysqlType.FIELD_TYPE_NULL:
                return 0;
        }
        return -1; // unknown type
    }

}
