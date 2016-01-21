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

package com.mysql.cj.core.util;

/**
 * A lazy string that can take a byte buffer and encoding and interpret it as a string if/when requested. The string is cached and saved for any further
 * requests. "NULL" values can be represented by a 0-len string or a <i>null</i> passed to {@link LazyString(String)}.
 */
public class LazyString {
    private String string; // the string, if one has been created
    private byte[] buffer;
    private int offset;
    private int length;
    private String encoding;

    public LazyString(String string) {
        // convenience for wrapping
        this.string = string;
    }

    public LazyString(byte[] buffer, int offset, int length, String encoding) {
        this.buffer = buffer;
        this.offset = offset;
        this.length = length;
        this.encoding = encoding;
    }

    private String createAndCacheString() {
        if (this.length > 0) {
            this.string = StringUtils.toString(this.buffer, this.offset, this.length, this.encoding);
        }
        // this can be NULL for 0-len strings
        return this.string;
    }

    @Override
    public String toString() {
        if (this.string != null) {
            return this.string;
        }
        return createAndCacheString();
    }

    public int length() {
        if (this.string != null) {
            return this.string.length();
        }
        return this.length;
    }
}
