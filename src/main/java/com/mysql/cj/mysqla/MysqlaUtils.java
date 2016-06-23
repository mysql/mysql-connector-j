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

package com.mysql.cj.mysqla;

import com.mysql.cj.api.mysqla.io.PacketPayload;
import com.mysql.cj.core.Messages;
import com.mysql.cj.core.util.StringUtils;

/**
 * Utilities to manipulate MySQL protocol-specific formats.
 */
public class MysqlaUtils {
    private MysqlaUtils() {
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
     * Interpret a BIT value as a long.
     */
    public static long bitToLong(byte[] bytes, int offset, int length) {
        long valueAsLong = 0;
        for (int i = 0; i < length; i++) {
            valueAsLong = valueAsLong << 8 | bytes[offset + i] & 0xff;
        }
        return valueAsLong;
    }

    public static String extractSqlFromPacket(String possibleSqlQuery, PacketPayload queryPacket, int endOfQueryPacketPosition, int maxQuerySizeToLog) {
        String extractedSql = null;

        if (possibleSqlQuery != null) {
            if (possibleSqlQuery.length() > maxQuerySizeToLog) {
                StringBuilder truncatedQueryBuf = new StringBuilder(possibleSqlQuery.substring(0, maxQuerySizeToLog));
                truncatedQueryBuf.append(Messages.getString("MysqlIO.25"));
                extractedSql = truncatedQueryBuf.toString();
            } else {
                extractedSql = possibleSqlQuery;
            }
        }

        if (extractedSql == null) {
            // This is probably from a client-side prepared statement

            int extractPosition = endOfQueryPacketPosition;

            boolean truncated = false;

            if (endOfQueryPacketPosition > maxQuerySizeToLog) {
                extractPosition = maxQuerySizeToLog;
                truncated = true;
            }

            extractedSql = StringUtils.toString(queryPacket.getByteBuffer(), 1, (extractPosition - 1));

            if (truncated) {
                extractedSql += Messages.getString("MysqlIO.25");
            }
        }

        return extractedSql;

    }

    /**
     * Get the length of a binary-encoded value of the given type.
     * 
     * @return the length (&gt;0), 0 for a length-prefixed type, or -1 for unknown
     */
    public static int getBinaryEncodedLength(int type) {
        switch (type) {
            case MysqlaConstants.FIELD_TYPE_TINY:
                return 1;
            case MysqlaConstants.FIELD_TYPE_SHORT:
            case MysqlaConstants.FIELD_TYPE_YEAR:
                return 2;
            case MysqlaConstants.FIELD_TYPE_LONG:
            case MysqlaConstants.FIELD_TYPE_INT24:
            case MysqlaConstants.FIELD_TYPE_FLOAT:
                return 4;
            case MysqlaConstants.FIELD_TYPE_LONGLONG:
            case MysqlaConstants.FIELD_TYPE_DOUBLE:
                return 8;
            case MysqlaConstants.FIELD_TYPE_TIME:
            case MysqlaConstants.FIELD_TYPE_DATE:
            case MysqlaConstants.FIELD_TYPE_DATETIME:
            case MysqlaConstants.FIELD_TYPE_TIMESTAMP:
            case MysqlaConstants.FIELD_TYPE_TINY_BLOB:
            case MysqlaConstants.FIELD_TYPE_MEDIUM_BLOB:
            case MysqlaConstants.FIELD_TYPE_LONG_BLOB:
            case MysqlaConstants.FIELD_TYPE_BLOB:
            case MysqlaConstants.FIELD_TYPE_VAR_STRING:
            case MysqlaConstants.FIELD_TYPE_VARCHAR:
            case MysqlaConstants.FIELD_TYPE_STRING:
            case MysqlaConstants.FIELD_TYPE_DECIMAL:
            case MysqlaConstants.FIELD_TYPE_NEWDECIMAL:
            case MysqlaConstants.FIELD_TYPE_GEOMETRY:
            case MysqlaConstants.FIELD_TYPE_BIT:
            case MysqlaConstants.FIELD_TYPE_JSON:
                return 0;
        }
        return -1; // unknown type
    }
}
