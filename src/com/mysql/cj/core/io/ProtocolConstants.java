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

package com.mysql.cj.core.io;

/**
 * Constant values specific to the MySQL protocol.
 */
public class ProtocolConstants {
    private ProtocolConstants() {
    }

    /** Maximum size of MySQL packet payload. */
    public static final int MAX_PACKET_SIZE = 256 * 256 * 256 - 1;
    /** Size of MySQL packet header (payload size + packet sequence ID). */
    public static final int HEADER_LENGTH = 4;
    public static final int SEED_LENGTH = 20;

    /* Type ids of response packets. */
    public static final short TYPE_ID_ERROR = 0xFF;
    public static final short TYPE_ID_EOF = 0xFE;
    public static final short TYPE_ID_LOCAL_INFILE = 0xFB;
    public static final short TYPE_ID_OK = 0;

    /* MySQL binary protocol value lengths. */
    public static final int BIN_LEN_INT1 = 1;
    public static final int BIN_LEN_INT2 = 2;
    public static final int BIN_LEN_INT4 = 4;
    public static final int BIN_LEN_INT8 = 8;
    public static final int BIN_LEN_FLOAT = 4;
    public static final int BIN_LEN_DOUBLE = 8;
    public static final int BIN_LEN_DATE = 4;
    public static final int BIN_LEN_TIMESTAMP = 11;
    public static final int BIN_LEN_TIMESTAMP_NO_US = 7;
    public static final int BIN_LEN_TIME = 8;
    public static final int BIN_LEN_TIME_NO_US = 12;
}
