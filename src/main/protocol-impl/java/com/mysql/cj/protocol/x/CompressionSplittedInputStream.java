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

import static com.mysql.cj.protocol.x.XMessageHeader.HEADER_LENGTH;

import java.io.ByteArrayInputStream;
import java.io.EOFException;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Parser;
import com.mysql.cj.Messages;
import com.mysql.cj.exceptions.CJCommunicationsException;
import com.mysql.cj.exceptions.ExceptionFactory;
import com.mysql.cj.exceptions.WrongArgumentException;
import com.mysql.cj.x.protobuf.Mysqlx.ServerMessages;
import com.mysql.cj.x.protobuf.MysqlxConnection.Compression;

/**
 * An {@link InputStream} wrapper that analyzes X Protocol frames and, if compressed, routes them to a secondary compressor-able {@link InputStream} that also
 * knows how to rebuild uncompressed X Protocol frames from compressed ones.
 */
public class CompressionSplittedInputStream extends FilterInputStream {

    private CompressorStreamsFactory compressorIoStreamsFactory;

    private byte[] frameHeader = new byte[HEADER_LENGTH];
    private int frameHeaderConsumed = 0;
    private int framePayloadLength = 0;
    private int framePayloadConsumed = 0;
    private XMessageHeader xMessageHeader;

    private InputStream compressorIn = null;

    private byte[] singleByte = new byte[1];

    private boolean closed = false;

    public CompressionSplittedInputStream(InputStream in, CompressorStreamsFactory streamsFactory) {
        super(in);
        this.compressorIoStreamsFactory = streamsFactory;
    }

    /**
     * Same as {@link InputStream#available()}, except that the exact number of bytes that can be read from the underlying {@link InputStream} may not be
     * accurate until it is known if the next bytes contain compressed data or not.
     *
     * @return an approximate number of available bytes to read.
     *
     * @see FilterInputStream#available()
     */
    @Override
    public int available() throws IOException {
        ensureOpen();
        if (this.compressorIn != null) {
            return this.compressorIn.available();
        }
        return (this.frameHeaderConsumed > 0 ? HEADER_LENGTH - this.frameHeaderConsumed : 0) + this.in.available();
    }

    /**
     * Closes this stream.
     *
     * @see FilterInputStream#close()
     */
    @Override
    public void close() throws IOException {
        if (!this.closed) {
            super.close();
            this.in = null;
            if (this.compressorIn != null) {
                this.compressorIn.close();
            }
            this.compressorIn = null;
            this.closed = true;
        }
    }

    /**
     * Forwards the read to {@link #read(byte[], int, int)}.
     *
     * @see FilterInputStream#read()
     */
    @Override
    public int read() throws IOException {
        ensureOpen();
        int read = read(this.singleByte, 0, 1);
        if (read >= 0) {
            return this.singleByte[0] & 0xff;
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
     * Reads bytes from the underlying {@link InputStream} either from the one that gets data directly from the original source {@link InputStream} or from
     * a compressor able {@link InputStream}, if reading of a compressed X Protocol frame is in progress.
     *
     * @see FilterInputStream#read(byte[], int, int)
     */
    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        ensureOpen();
        if (len <= 0) {
            return 0;
        }

        peekNextFrame();

        try {
            if (isCompressedDataAvailable()) {
                int bytesRead = readFully(this.compressorIn, b, off, len);
                if (isCompressedDataReadComplete()) {
                    this.compressorIn.close();
                    this.compressorIn = null;
                }
                return bytesRead;
            }
        } catch (IOException e) {
            throw e;
        }

        int headerBytesRead = 0;
        if (!isFrameHeaderFullyConsumed()) { // Recycle the frame header bytes.
            int lenToConsume = Math.min(len, HEADER_LENGTH - this.frameHeaderConsumed);
            System.arraycopy(this.frameHeader, this.frameHeaderConsumed, b, off, lenToConsume);
            off += lenToConsume;
            len -= lenToConsume;
            this.frameHeaderConsumed += lenToConsume;
            headerBytesRead = lenToConsume;
        }

        // Read frame payload bytes.
        int payloadBytesRead = readFully(b, off, len);
        this.framePayloadConsumed += payloadBytesRead;

        return headerBytesRead + payloadBytesRead;
    }

    /**
     * Checks the header of the next X Protocol frame and, depending on its type, sets up this class to read from an alternative compressor able underlying
     * {@link InputStream}.
     *
     * @throws IOException
     *             if any of the underlying I/O operations fail.
     */
    private void peekNextFrame() throws IOException {
        if (isDataAvailable()) {
            return;
        }

        readFully(this.frameHeader, 0, HEADER_LENGTH);
        this.xMessageHeader = new XMessageHeader(this.frameHeader);
        this.framePayloadLength = this.xMessageHeader.getMessageSize();
        this.frameHeaderConsumed = 0;
        this.framePayloadConsumed = 0;

        if (isCompressedFrame()) {
            Compression compressedMessage = parseCompressedMessage();
            this.compressorIn = new ConfinedInputStream(
                    this.compressorIoStreamsFactory.getInputStreamInstance(new ByteArrayInputStream(compressedMessage.getPayload().toByteArray())),
                    (int) compressedMessage.getUncompressedSize());

            // Preemptively set as all bytes consumed since next reads will be redirected to the compressor InputStream.
            this.frameHeaderConsumed = HEADER_LENGTH;
            this.framePayloadConsumed = this.framePayloadLength;
        }
    }

    /**
     * Checks if current X Protocol frame is compressed.
     *
     * @return
     *         <code>true</code> if the type of current frame is {@link com.mysql.cj.x.protobuf.Mysqlx.ServerMessages.Type#COMPRESSION}, <code>false</code>
     *         otherwise.
     */
    private boolean isCompressedFrame() {
        return ServerMessages.Type.forNumber(this.xMessageHeader.getMessageType()) == ServerMessages.Type.COMPRESSION;
    }

    /**
     * Parses the next X Protocol message as a compressed one.
     *
     * @return
     *         The Protobuf {@link Compression} message.
     */
    @SuppressWarnings("unchecked")
    private Compression parseCompressedMessage() {
        Parser<Compression> parser = (Parser<Compression>) MessageConstants.MESSAGE_CLASS_TO_PARSER
                .get(MessageConstants.MESSAGE_TYPE_TO_CLASS.get(ServerMessages.Type.COMPRESSION_VALUE));
        byte[] packet = new byte[this.xMessageHeader.getMessageSize()];

        try {
            readFully(packet);
        } catch (IOException e) {
            throw ExceptionFactory.createException(CJCommunicationsException.class, Messages.getString("Protocol.Compression.Streams.0"), e);
        }

        try {
            return parser.parseFrom(packet);
        } catch (InvalidProtocolBufferException e) {
            throw ExceptionFactory.createException(WrongArgumentException.class, Messages.getString("Protocol.Compression.Streams.1"), e);
        }
    }

    /**
     * Checks if there is data available to be consumed.
     *
     * @return
     *         <code>true</code> if this frame's bytes weren't all consumed yet, <code>false</code> otherwise.
     *
     * @throws IOException
     *             if any of the underlying I/O operations fail.
     */
    private boolean isDataAvailable() throws IOException {
        return isCompressedDataAvailable() || this.frameHeaderConsumed > 0 && this.frameHeaderConsumed < HEADER_LENGTH
                || isFrameHeaderFullyConsumed() && this.framePayloadConsumed < this.framePayloadLength;
    }

    /**
     * Checks if there is data available in the compressed {@link InputStream} to be consumed.
     *
     * @return
     *         <code>true</code> if there is compressed data available, <code>false</code> otherwise.
     *
     * @throws IOException
     *             if any of the underlying I/O operations fail.
     */
    private boolean isCompressedDataAvailable() throws IOException {
        return this.compressorIn != null && this.compressorIn.available() > 0;
    }

    /**
     * Checks if all data from the compressed {@link InputStream} was fully consumed.
     *
     * @return
     *         <code>true</code> if all compressed data was consumed, <code>false</code> otherwise.
     *
     * @throws IOException
     *             if any of the underlying I/O operations fail.
     */
    private boolean isCompressedDataReadComplete() throws IOException {
        return this.compressorIn != null && this.compressorIn.available() == 0;
    }

    /**
     * Checks if the X Protocol frame header was fully consumed.
     *
     * @return
     *         <code>true</code> if the frame header was fully consumed, <code>false</code> otherwise.
     */
    boolean isFrameHeaderFullyConsumed() {
        return this.frameHeaderConsumed == HEADER_LENGTH;
    }

    /**
     * Reads the number of bytes required to fill the given buffer from the underlying {@link InputStream}, blocking if needed.
     *
     * @param b
     *            the buffer into which the data is read.
     * @return the total number of bytes read into the buffer, or <code>-1</code> if there is no more data because the end of the stream has been reached.
     * @exception IOException
     *                if any of the underlying I/O operations fail.
     */
    public int readFully(byte[] b) throws IOException {
        return readFully(b, 0, b.length);
    }

    /**
     * Reads the exact number of requested bytes from the underlying {@link InputStream}, blocking if needed.
     *
     * @param b
     *            the buffer into which the data is read.
     * @param off
     *            the start offset in the destination array <code>b</code>
     * @param len
     *            the maximum number of bytes read.
     * @return the total number of bytes read into the buffer, or <code>-1</code> if there is no more data because the end of the stream has been reached.
     * @exception IOException
     *                if any of the underlying I/O operations fail.
     */
    private final int readFully(byte[] b, int off, int len) throws IOException {
        return readFully(this.in, b, off, len);
    }

    /**
     * Reads the exact number of requested bytes from the given {@link InputStream}, blocking if needed.
     *
     * @param inStream
     *            input stream to read from
     * @param b
     *            the buffer into which the data is read.
     * @param off
     *            the start offset in the destination array <code>b</code>
     * @param len
     *            the maximum number of bytes read.
     * @return the total number of bytes read into the buffer, or <code>-1</code> if there is no more data because the end of the stream has been reached.
     *
     * @throws IOException
     *             if any of the underlying I/O operations fail.
     */
    private final int readFully(InputStream inStream, byte[] b, int off, int len) throws IOException {
        if (len < 0) {
            throw new IndexOutOfBoundsException();
        }

        int total = 0;
        while (total < len) {
            int count = inStream.read(b, off + total, len - total);
            if (count < 0) {
                throw new EOFException();
            }
            total += count;
        }
        return total;
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
