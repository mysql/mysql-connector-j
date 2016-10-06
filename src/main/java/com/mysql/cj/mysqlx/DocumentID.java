/*
  Copyright (c) 2016, Oracle and/or its affiliates. All rights reserved.

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

package com.mysql.cj.mysqlx;

import java.util.UUID;

public class DocumentID {

    /**
     * X DevAPI Document ID has format based on RFC 4122 UUID format (version 1, variant 1),
     * with a modification to match the requirement of a stable id prefix.
     * <p>
     * The original UUID specification has the following format, as described in
     * <a href="http://dev.mysql.com/doc/refman/5.7/en/miscellaneous-functions.html#function_uuid">function_uuid</a>
     * <p>
     * aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee
     * <p>
     * where aaaaaaaa is the lower part of the timestamp and eeeeeeeeeeee is the MAC
     * address of the host. X DevAPI Document ID has inverted components order, so the format becomes:
     * <p>
     * eeeeeeeeeeee-dddd-cccc-bbbb-aaaaaaaa
     * <p>
     * Example:
     * <p>
     * RFC 4122 UUID: 5c99cdfe-48cb-11e6-94f3-4a383b7fcc8
     * <p>
     * X DevAPI Document ID: 4a383b7fcc8-94f3-11e6-48cb-5c99cdfe
     * 
     * @return
     */
    public static String generate() {
        UUID uuid = UUID.randomUUID();
        return (getDigits(uuid.getLeastSignificantBits(), 12) + //
                getDigits(uuid.getLeastSignificantBits() >> 48, 4) + //
                getDigits(uuid.getMostSignificantBits(), 4) + //
                getDigits(uuid.getMostSignificantBits() >> 16, 4) + //
                getDigits(uuid.getMostSignificantBits() >> 32, 8));
    }

    private static String getDigits(long val, int digits) {
        long hi = 1L << (digits * 4);
        return Long.toHexString(hi | (val & (hi - 1))).substring(1);
    }

}
