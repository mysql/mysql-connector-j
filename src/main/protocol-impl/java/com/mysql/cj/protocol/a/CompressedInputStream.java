/*
 * Copyright (c) 2002, 2026, Oracle and/or its affiliates.
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

package com.mysql.cj.protocol.a;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.zip.DataFormatException;
import java.util.zip.Inflater;

import com.mysql.cj.conf.RuntimeProperty;
import com.mysql.cj.log.Log;
import com.mysql.cj.util.StringUtils;

/**
 * Used to de-compress packets from the MySQL server when protocol-level compression is turned on.
 */
public class CompressedInputStream extends InputStream {

    /** Buffers larger than this are not kept once the data in them has been consumed. */
    private static final int MAX_RETAINED_BUFFER_SIZE = 64 * 1024;

    /** The packet data after it has been un-compressed */
    private byte[] buffer;

    /** Number of bytes of packet data in {@link #buffer} */
    private int bufferLength = 0;

    /** The compressed bytes of the packet being read */
    private byte[] compressedBuffer;

    /** The stream we are reading from the server */
    private InputStream in;

    /** The ZIP inflater used to un-compress packets */
    private Inflater inflater;

    /** Connection property reference */
    private RuntimeProperty<Boolean> traceProtocol;

    /** Connection logger */
    private Log log;

    /**
     * The buffer to read packet headers into
     */
    private byte[] packetHeaderBuffer = new byte[7];

    /** The position we are reading from */
    private int pos = 0;

    /**
     * Creates a new CompressedInputStream that reads the given stream from the
     * server.
     *
     * @param streamFromServer
     *            original server InputStream
     * @param traceProtocol
     *            "traceProtocol" property
     * @param log
     *            logger
     */
    public CompressedInputStream(InputStream streamFromServer, RuntimeProperty<Boolean> traceProtocol, Log log) {
        this.traceProtocol = traceProtocol;
        this.log = log;
        this.in = streamFromServer;
        this.inflater = new Inflater();
    }

    @Override
    public int available() throws IOException {
        if (this.buffer == null) {
            return this.in.available();
        }

        return this.bufferLength - this.pos + this.in.available();
    }

    @Override
    public void close() throws IOException {
        this.in.close();
        this.buffer = null;
        this.compressedBuffer = null;
        this.inflater.end();
        this.inflater = null;
        this.traceProtocol = null;
        this.log = null;
    }

    /**
     * Retrieves and un-compressed (if necessary) the next packet from the
     * server.
     *
     * @throws IOException
     *             if an I/O error occurs
     */
    private void getNextPacketFromServer() throws IOException {
        int lengthRead = readFully(this.packetHeaderBuffer, 0, 7);

        if (lengthRead < 7) {
            throw new IOException("Unexpected end of input stream");
        }

        int compressedPacketLength = (this.packetHeaderBuffer[0] & 0xff) + ((this.packetHeaderBuffer[1] & 0xff) << 8)
                + ((this.packetHeaderBuffer[2] & 0xff) << 16);

        int uncompressedLength = (this.packetHeaderBuffer[4] & 0xff) + ((this.packetHeaderBuffer[5] & 0xff) << 8) + ((this.packetHeaderBuffer[6] & 0xff) << 16);

        boolean doTrace = this.traceProtocol.getValue();

        if (doTrace) {
            this.log.logTrace("Reading compressed packet of length " + compressedPacketLength + " uncompressed to " + uncompressedLength);
        }

        boolean compressed = uncompressedLength > 0;
        if (!compressed) {
            if (doTrace) {
                this.log.logTrace("Packet didn't meet compression threshold, not uncompressing...");
            }

            //
            // Read data, note this this code is reached when using compressed packets that have not been compressed, as well
            //
            uncompressedLength = compressedPacketLength;
        }

        int remaining = this.buffer == null ? 0 : this.bufferLength - this.pos;
        byte[] target = carryOverUnreadBytes(remaining, remaining + uncompressedLength, doTrace);

        if (compressed) {
            this.compressedBuffer = ensureCapacity(this.compressedBuffer, compressedPacketLength);
            readFully(this.compressedBuffer, 0, compressedPacketLength);
            inflate(compressedPacketLength, target, remaining, uncompressedLength);
        } else {
            readFully(target, remaining, uncompressedLength);
        }

        if (doTrace) {
            if (uncompressedLength > 1024) {
                this.log.logTrace("Uncompressed packet: \n" + StringUtils.dumpAsHex(Arrays.copyOfRange(target, remaining, remaining + 256), 256));
                int end = remaining + uncompressedLength;
                this.log.logTrace("Uncompressed packet: \n" + StringUtils.dumpAsHex(Arrays.copyOfRange(target, end - 256, end), 256));
                this.log.logTrace("Large packet dump truncated. Showing first and last 256 bytes.");
            } else {
                this.log.logTrace("Uncompressed packet: \n"
                        + StringUtils.dumpAsHex(Arrays.copyOfRange(target, remaining, remaining + uncompressedLength), uncompressedLength));
            }
        }

        this.pos = 0;
        this.buffer = target;
        this.bufferLength = remaining + uncompressedLength;

        return;
    }

    /**
     * Returns an array of at least {@code capacity} bytes that starts with the unread bytes of {@link #buffer}.
     *
     * @param remaining
     *            number of unread bytes in {@link #buffer}
     * @param capacity
     *            the number of bytes needed
     * @param doTrace
     *            whether to trace the protocol
     * @return the array to read the next packet into, after the unread bytes
     */
    private byte[] carryOverUnreadBytes(int remaining, int capacity, boolean doTrace) {
        byte[] target = ensureCapacity(this.buffer, capacity);
        if (remaining > 0) {
            if (doTrace) {
                this.log.logTrace("Combining remaining packet with new: ");
            }
            System.arraycopy(this.buffer, this.pos, target, 0, remaining);
        }
        return target;
    }

    /**
     * Inflates the compressed packet in {@link #compressedBuffer} into {@code target}.
     *
     * @param compressedLength
     *            number of compressed bytes
     * @param target
     *            the array to inflate into
     * @param offset
     *            where to start writing in {@code target}
     * @param uncompressedLength
     *            the uncompressed length announced by the packet header
     * @throws IOException
     *             if the packet cannot be inflated or does not inflate to exactly {@code uncompressedLength} bytes
     */
    private void inflate(int compressedLength, byte[] target, int offset, int uncompressedLength) throws IOException {
        this.inflater.reset();
        this.inflater.setInput(this.compressedBuffer, 0, compressedLength);

        int inflated;
        try {
            inflated = this.inflater.inflate(target, offset, uncompressedLength);
        } catch (DataFormatException dfe) {
            throw new IOException("Error while uncompressing packet from server.", dfe);
        }

        if (inflated != uncompressedLength || !this.inflater.finished()) {
            throw new IOException("Compressed packet from server does not uncompress to its announced length of " + uncompressedLength + " bytes.");
        }
    }

    /**
     * Returns {@code array} if it can hold {@code capacity} bytes, unless it is larger than {@link #MAX_RETAINED_BUFFER_SIZE} and far larger than needed, and
     * a new array otherwise.
     *
     * @param array
     *            the current array, or null
     * @param capacity
     *            the number of bytes needed
     * @return an array of at least {@code capacity} bytes
     */
    private static byte[] ensureCapacity(byte[] array, int capacity) {
        if (array != null && array.length >= capacity && (array.length <= MAX_RETAINED_BUFFER_SIZE || capacity > MAX_RETAINED_BUFFER_SIZE / 2)) {
            return array;
        }
        return new byte[Math.max(capacity, Math.min(MAX_RETAINED_BUFFER_SIZE, 2 * capacity))];
    }

    /**
     * Determines if another packet needs to be read from the server to be able
     * to read numBytes from the stream.
     *
     * @param numBytes
     *            the number of bytes to be read
     *
     * @throws IOException
     *             if an I/O error occors.
     */
    private void getNextPacketIfRequired(int numBytes) throws IOException {
        if (this.buffer == null || this.pos + numBytes > this.bufferLength) {
            getNextPacketFromServer();
        }
    }

    @Override
    public int read() throws IOException {
        try {
            getNextPacketIfRequired(1);
        } catch (IOException ioEx) {
            return -1;
        }

        return this.buffer[this.pos++] & 0xff;
    }

    @Override
    public int read(byte[] b) throws IOException {
        return read(b, 0, b.length);
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        if (b == null) {
            throw new NullPointerException();
        } else if (off < 0 || off > b.length || len < 0 || off + len > b.length || off + len < 0) {
            throw new IndexOutOfBoundsException();
        }

        if (len <= 0) {
            return 0;
        }

        try {
            getNextPacketIfRequired(len);
        } catch (IOException ioEx) {
            return -1;
        }

        int remainingBufferLength = this.bufferLength - this.pos;
        int consummedBytesLength = Math.min(remainingBufferLength, len);

        System.arraycopy(this.buffer, this.pos, b, off, consummedBytesLength);
        this.pos += consummedBytesLength;

        return consummedBytesLength;
    }

    private final int readFully(byte[] b, int off, int len) throws IOException {
        if (len < 0) {
            throw new IndexOutOfBoundsException();
        }

        int n = 0;

        while (n < len) {
            int count = this.in.read(b, off + n, len - n);

            if (count < 0) {
                throw new EOFException();
            }

            n += count;
        }

        return n;
    }

    @Override
    public long skip(long n) throws IOException {
        long count = 0;

        for (long i = 0; i < n; i++) {
            int bytesRead = read();

            if (bytesRead == -1) {
                break;
            }

            count++;
        }

        return count;
    }

}
