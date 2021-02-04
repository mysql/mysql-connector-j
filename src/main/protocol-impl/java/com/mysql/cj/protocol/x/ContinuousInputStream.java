/*
 * Copyright (c) 2021, Oracle and/or its affiliates.
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
import java.util.LinkedList;
import java.util.Queue;

/**
 * An {@link InputStream} wrapper that reads from a queue of underlying {@link InputStream}s, giving the impression that all data is coming from a single,
 * continuous, source.
 */
public class ContinuousInputStream extends FilterInputStream {
    private Queue<InputStream> inputStreams = new LinkedList<>();

    private boolean closed = false;

    protected ContinuousInputStream(InputStream in) {
        super(in);
    }

    /**
     * Returns the number of bytes available in the active underlying {@link InputStream}.
     * 
     * @return the number of bytes available.
     * @see FilterInputStream#available()
     */
    @Override
    public int available() throws IOException {
        ensureOpen();
        int available = super.available();
        if (available == 0 && nextInLine()) {
            return available();
        }
        return available;
    }

    /**
     * Closes this stream and all underlying {@link InputStream}s.
     * 
     * @see FilterInputStream#close()
     */
    @Override
    public void close() throws IOException {
        if (!this.closed) {
            this.closed = true;
            super.close();
            for (InputStream is : this.inputStreams) {
                is.close();
            }
        }
    }

    /**
     * Reads one byte from the underlying {@link InputStream}. When EOF is reached, then reads from the next {@link InputStream} in the queue.
     * 
     * @see FilterInputStream#read()
     */
    @Override
    public int read() throws IOException {
        ensureOpen();
        int read = super.read();
        if (read >= 0) {
            return read;
        }
        if (nextInLine()) {
            return read();
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
     * Reads bytes from the underlying {@link InputStream}. When EOF is reached, then reads from the next {@link InputStream} in the queue.
     * 
     * @see FilterInputStream#read(byte[], int, int)
     */
    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        ensureOpen();
        int toRead = Math.min(len, available());
        int read = super.read(b, off, toRead);
        if (read > 0) {
            return read;
        }
        if (nextInLine()) {
            return read(b, off, len);
        }
        return read;
    }

    /**
     * Adds another {@link InputStream} to the {@link InputStream}s queue.
     * 
     * @param newIn
     *            the {@link InputStream} to add.
     * @return
     *         <code>true</code> if the element was added to the {@link InputStream}s queue.
     */
    protected boolean addInputStream(InputStream newIn) {
        return this.inputStreams.offer(newIn);
    }

    /**
     * Closes the currently active {@link InputStream} and replaces it by the the head of the {@link InputStream}s queue.
     * 
     * @return
     *         <code>true</code> if the currently active {@link InputStream} was replaced by a new one.
     * @throws IOException
     *             if errors occur while closing the currently active {@link InputStream}.
     */
    private boolean nextInLine() throws IOException {
        InputStream nextInputStream = this.inputStreams.poll();
        if (nextInputStream != null) {
            super.close();
            this.in = nextInputStream;
            return true;
        }
        return false;
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
