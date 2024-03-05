/*
 * Copyright (c) 2015, 2024, Oracle and/or its affiliates.
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

package com.mysql.cj.protocol;

import java.io.EOFException;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;

import com.mysql.cj.Messages;

/**
 * InputStream wrapper that provides methods to aggregate reads of a given size. c.f. readFully(byte[],int,int).
 */
public class FullReadInputStream extends FilterInputStream {

    public FullReadInputStream(InputStream underlyingStream) {
        super(underlyingStream);
    }

    public InputStream getUnderlyingStream() {
        return this.in;
    }

    public int readFully(byte[] b) throws IOException {
        return readFully(b, 0, b.length);
    }

    public int readFully(byte[] b, int off, int len) throws IOException {
        if (len < 0) {
            throw new IndexOutOfBoundsException();
        }

        int n = 0;

        while (n < len) {
            int count = read(b, off + n, len - n);

            if (count < 0) {
                throw new EOFException(Messages.getString("MysqlIO.EOF", new Object[] { Integer.valueOf(len), Integer.valueOf(n) }));
            }

            n += count;
        }

        return n;
    }

    public long skipFully(long len) throws IOException {
        if (len < 0) {
            throw new IOException(Messages.getString("MysqlIO.105"));
        }

        long n = 0;

        while (n < len) {
            long count = skip(len - n);

            if (count < 0) {
                throw new EOFException(Messages.getString("MysqlIO.EOF", new Object[] { Long.valueOf(len), Long.valueOf(n) }));
            }

            n += count;
        }

        return n;
    }

    public int skipLengthEncodedInteger() throws IOException {
        int sw = read() & 0xff;

        switch (sw) {
            case 252:
                return (int) skipFully(2) + 1;

            case 253:
                return (int) skipFully(3) + 1;

            case 254:
                return (int) skipFully(8) + 1;

            default:
                return 1;
        }
    }

}
