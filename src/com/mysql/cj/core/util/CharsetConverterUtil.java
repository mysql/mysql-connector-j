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

import com.mysql.cj.api.CharsetConverter;
import com.mysql.cj.api.MysqlConnection;

/**
 * Encapsulate the conn+encoding pair that is used throughout the driver to decode result data.
 */
public class CharsetConverterUtil {
    private String encoding;
    private MysqlConnection conn;

    public CharsetConverterUtil() {
    }

    public CharsetConverterUtil(String encoding, MysqlConnection conn) {
        this.encoding = encoding;
        this.conn = conn;
    }

    /**
     * Interpret the bytestring as a string in the encoding of this converter.
     */
    public String createString(byte[] bytes, int offset, int length) {
        String stringVal = null;

        if (this.conn != null) {
            try {
                if (this.encoding == null) {
                    stringVal = StringUtils.toString(bytes);
                } else {
                    CharsetConverter converter = this.conn.getCharsetConverter(this.encoding);

                    if (converter != null) {
                        stringVal = converter.toString(bytes, offset, length);
                    } else {
                        stringVal = StringUtils.toString(bytes, offset, length, this.encoding);
                    }
                }
            } catch (java.io.UnsupportedEncodingException ex) {
                throw new RuntimeException("Unsupported encoding: " + this.encoding, ex);
            }
        } else {
            stringVal = StringUtils.toAsciiString(bytes, offset, length);
        }
        return stringVal;
    }
}
