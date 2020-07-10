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

package com.mysql.cj.util;

import java.util.function.Supplier;

/**
 * A lazy string that can take a byte buffer and encoding and interpret it as a string if/when requested. The string is cached and saved for any further
 * requests. "NULL" values can be represented by a 0-len string or a <i>null</i> passed to LazyString(String).
 */
public class LazyString implements Supplier<String> {
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

    public LazyString(byte[] buffer, int offset, int length) {
        this.buffer = buffer;
        this.offset = offset;
        this.length = length;
    }

    private String createAndCacheString() {
        if (this.length > 0) {
            this.string = this.encoding == null ? StringUtils.toString(this.buffer, this.offset, this.length)
                    : StringUtils.toString(this.buffer, this.offset, this.length, this.encoding);
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

    @Override
    public String get() {
        return toString();
    }
}
