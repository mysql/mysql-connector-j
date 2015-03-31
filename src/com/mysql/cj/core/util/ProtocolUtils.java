/*
  Copyright (c) 2015, Oracle and/or its affiliates. All rights reserved.

  The MySQL Connector/J is licensed under the terms of the GPLv2
  <http://www.gnu.org/licenses/old-licenses/gpl-2.0.html>, like most MySQL Connectors.
  There are special exceptions to the terms and conditions of the GPLv2 as it is applied to
  this software, see the FLOSS License Exception
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

package com.mysql.cj.core.util;

import com.mysql.cj.core.Messages;
import com.mysql.cj.core.io.Buffer;

/**
 * Utilities to manipulate MySQL protocol-specific formats.
 */
public class ProtocolUtils {
    private ProtocolUtils() {
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

    public static String extractSqlFromPacket(String possibleSqlQuery, Buffer queryPacket, int endOfQueryPacketPosition, int maxQuerySizeToLog) {

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
}
