/*
 * Copyright (c) 2020, 2023, Oracle and/or its affiliates.
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

package com.mysql.cj.protocol.x;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * An {@link InputStream} wrapper that limits the number of bytes that can be read form the underlying {@link InputStream}.
 */
public class ConfinedInputStream extends FilterInputStream {

    private int limit = 0;
    private int consumed = 0;

    private boolean closed = false;

    protected ConfinedInputStream(InputStream in) {
        this(in, 0);
    }

    protected ConfinedInputStream(InputStream in, int lim) {
        super(in);
        this.limit = lim;
        this.consumed = 0;
    }

    /**
     * Returns the number of bytes not yet consumed. Note that this method doen't care about the exact number of bytes that may or may not be available in the
     * underlying {@link InputStream}
     *
     * @return the number of bytes available.
     * @see FilterInputStream#available()
     */
    @Override
    public int available() throws IOException {
        ensureOpen();
        return this.limit - this.consumed;
    }

    /**
     * Closes this stream and throws away any bytes not consumed from the underlying {@link InputStream}.
     *
     * @see FilterInputStream#close()
     */
    @Override
    public void close() throws IOException {
        // Just make sure no more bytes are left on the underlying InputStream.
        // It's up to the InputStream higher in the chain to close the underlying InputStream.
        if (!this.closed) {
            dumpLeftovers();
            this.closed = true;
        }
    }

    /**
     * @see FilterInputStream#read()
     */
    @Override
    public int read() throws IOException {
        ensureOpen();
        int read = super.read();
        if (read >= 0) {
            this.consumed++;
        }
        return read;
    }

    /**
     * Forwards the read to {@link #read(byte[], int, int)}.
     *
     * @see FilterInputStream#read(byte[])
     */
    @Override
    public int read(byte[] b) throws IOException {
        ensureOpen();
        return read(b, 0, b.length);
    }

    /**
     * Reads bytes from the underlying {@link InputStream} up to the number of bytes defined in this {@link ConfinedInputStream} limit.
     *
     * @see FilterInputStream#read(byte[], int, int)
     */
    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        ensureOpen();
        if (this.consumed >= this.limit) {
            return -1;
        }
        int toRead = Math.min(len, available());
        int read = super.read(b, off, toRead);
        if (read > 0) {
            this.consumed += read;
        }
        return read;
    }

    /**
     * Resets this {@link ConfinedInputStream} limit so that it can be reused over the same underlying {@link InputStream}.
     *
     * @param len
     *            the new length to set.
     * @return
     *         the number of bytes not consumed before reseting the limit.
     */
    public int resetLimit(int len) {
        int remaining = 0;
        try {
            remaining = available();
        } catch (IOException e) {
            // Won't happen.
        }
        this.limit = len;
        this.consumed = 0;
        return remaining;
    }

    /**
     * Skips the number bytes not yet consumed from the underlying {@link InputStream}.
     *
     * @return the number of bytes skipped.
     * @throws IOException
     *             if any of the underlying I/O operations fail.
     */
    protected long dumpLeftovers() throws IOException {
        long skipped = skip(available());
        this.consumed += skipped;
        return skipped;
    }

    /**
     * Ensures that this {@link InputStream} wasn't closed yet.
     *
     * @throws IOException
     *             if this {@link InputStream} was closed.
     */
    private void ensureOpen() throws IOException {
        if (this.closed) {
            throw new IOException("Stream closed");
        }
    }

}
