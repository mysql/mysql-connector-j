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

import java.io.ByteArrayOutputStream;
import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;

import com.google.protobuf.ByteString;
import com.mysql.cj.x.protobuf.Mysqlx.ClientMessages;
import com.mysql.cj.x.protobuf.MysqlxConnection.Compression;

/**
 * An {@link OutputStream} wrapper that analyzes X Protocol frames and routes them directly to the original underlying {@link OutputStream} or passes them
 * through to a compressor-able {@link OutputStream} to compresses the frame, rebuilds it and then sends a newly compressed X Protocol frame, depending on
 * defined data size threshold.
 */
public class CompressionSplittedOutputStream extends FilterOutputStream {

    private CompressorStreamsFactory compressorIoStreamsFactory;

    private byte[] frameHeader = new byte[HEADER_LENGTH];
    private int frameHeaderBuffered = 0;
    private int frameHeaderDumped = 0;
    private int framePayloadLength = 0;
    private int framePayloadDumped = 0;
    private XMessageHeader xMessageHeader = null;

    private boolean compressionEnabled = false;

    private ByteArrayOutputStream bufferOut = null;
    private OutputStream compressorOut = null;

    private byte[] singleByte = new byte[1];

    private boolean closed = false;

    public CompressionSplittedOutputStream(OutputStream out, CompressorStreamsFactory ioStreamsFactory) {
        super(out);
        this.compressorIoStreamsFactory = ioStreamsFactory;
    }

    /**
     * Closes this stream.
     *
     * @see FilterOutputStream#close()
     */
    @Override
    public void close() throws IOException {
        if (!this.closed) {
            super.close();
            this.out = null;
            this.bufferOut = null;
            if (this.compressorOut != null) {
                this.compressorOut.close();
            }
            this.compressorOut = null;
            this.closed = true;
        }
    }

    /**
     * Forwards the write to {@link #write(byte[], int, int)};
     *
     * @see FilterOutputStream#write(int)
     */
    @Override
    public void write(int b) throws IOException {
        ensureOpen();
        this.singleByte[0] = (byte) b;
        write(this.singleByte, 0, 1);
    }

    /**
     * Forwards the write to {@link #write(byte[], int, int)};
     *
     * @see FilterOutputStream#write(byte[])
     */
    @Override
    public void write(byte[] b) throws IOException {
        ensureOpen();
        write(b, 0, b.length);
    }

    /**
     * Analyzes the given bytes as an X Protocol frame and, depending on its size, writes it as-is in the underlying {@link OutputStream} or rebuilds it as a
     * compressed X Protocol packet.
     *
     * @see java.io.FilterOutputStream#write(int)
     */
    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        ensureOpen();
        if ((off | len | b.length - (len + off) | off + len) < 0) { // Borrowed from FilterOutputStream.
            throw new IndexOutOfBoundsException();
        }

        int bytesProcessed = peekFrameHeader(b, off, len);

        if (isFrameHeaderBuffered() && !isFrameHeaderWriteComplete()) {
            this.xMessageHeader = new XMessageHeader(this.frameHeader);
            this.framePayloadLength = this.xMessageHeader.getMessageSize();
            this.framePayloadDumped = 0;

            this.compressionEnabled = this.framePayloadLength >= 250; // Compression threshold. May be user configurable in the future.

            if (this.compressionEnabled) {
                this.bufferOut = new ByteArrayOutputStream();
                this.compressorOut = this.compressorIoStreamsFactory.getOutputStreamInstance(this.bufferOut);
                this.compressorOut.write(this.frameHeader, 0, HEADER_LENGTH);
            } else {
                this.out.write(this.frameHeader, 0, HEADER_LENGTH);
            }
            this.frameHeaderDumped = HEADER_LENGTH;
        }

        int bytesToDump = len - bytesProcessed;
        if (bytesToDump > 0) {
            if (this.compressionEnabled) {
                this.compressorOut.write(b, off + bytesProcessed, bytesToDump);
            } else {
                this.out.write(b, off + bytesProcessed, bytesToDump);
            }
        }
        this.framePayloadDumped += bytesToDump;

        finalizeWrite();
    }

    /**
     * Captures the first bytes of each X Protocol frame into a byte buffer.
     *
     * @param b
     *            the data.
     * @param off
     *            the start offset in the data.
     * @param len
     *            the number of bytes to write.
     * @return
     *         the number of bytes actually buffered.
     */
    private int peekFrameHeader(byte[] b, int off, int len) {
        if (isPayloadWriteReady()) {
            return 0;
        }

        int toCollect = 0;
        if (this.frameHeaderBuffered < HEADER_LENGTH) {
            toCollect = Math.min(len, HEADER_LENGTH - this.frameHeaderBuffered);
            System.arraycopy(b, off, this.frameHeader, this.frameHeaderBuffered, toCollect);
            this.frameHeaderBuffered += toCollect;
        }
        return toCollect;
    }

    /**
     * Checks if there is a complete frame header already buffered.
     *
     * @return
     *         <code>true</code> if the frame header buffer is full, <code>false</code> otherwise.
     */
    private boolean isFrameHeaderBuffered() {
        return this.frameHeaderBuffered == HEADER_LENGTH;
    }

    /**
     * Checks if the entire frame X Protocol frame header has been fully written.
     *
     * @return
     *         <code>true</code> if the frame header was written, <code>false</code> otherwise.
     */
    private boolean isFrameHeaderWriteComplete() {
        return this.frameHeaderDumped == HEADER_LENGTH;
    }

    /**
     * Checks if the X Protocol frame payload is ready to be written on the underlying {@link OutputStream}.
     *
     * @return
     *         <code>true</code> the payload can be written, <code>false</code> otherwise.
     */
    private boolean isPayloadWriteReady() {
        return isFrameHeaderWriteComplete() && this.framePayloadDumped < this.framePayloadLength;
    }

    /**
     * Checks if current X Protocol frame has been fully written.
     *
     * @return
     *         <code>true</code> if the frame currently in progress was fully written, <code>false</code> otherwise.
     */
    private boolean isWriteComplete() {
        return isFrameHeaderWriteComplete() && this.framePayloadDumped >= this.framePayloadLength;
    }

    /**
     * Finalizes the writing of the compressed {@link OutputStream}, if one is currently in use, by flushing it into a temporary buffer and reassembling the
     * original X Protocol frame into a compressed one. Finally, writes the entire compressed frame into the underlying {@link OutputStream}.
     *
     * @throws IOException
     *             if any of the underlying I/O operations fail.
     */
    private void finalizeWrite() throws IOException {
        if (isWriteComplete()) {
            if (this.compressionEnabled) {
                this.compressorOut.close();
                this.compressorOut = null;

                byte[] compressedData = this.bufferOut.toByteArray();
                Compression compressedMessage = Compression.newBuilder().setUncompressedSize(XMessageHeader.HEADER_LENGTH + this.framePayloadLength)
                        .setClientMessages(ClientMessages.Type.forNumber(this.xMessageHeader.getMessageType())).setPayload(ByteString.copyFrom(compressedData))
                        .build();

                ByteBuffer messageHeader = ByteBuffer.allocate(XMessageHeader.HEADER_LENGTH).order(ByteOrder.LITTLE_ENDIAN);
                messageHeader.putInt(compressedMessage.getSerializedSize() + XMessageHeader.MESSAGE_TYPE_LENGTH);
                messageHeader.put((byte) ClientMessages.Type.COMPRESSION_VALUE);

                this.out.write(messageHeader.array());
                compressedMessage.writeTo(this.out);
                this.out.flush();

                this.compressionEnabled = false;
            }

            Arrays.fill(this.frameHeader, (byte) 0);
            this.frameHeaderBuffered = 0;
            this.frameHeaderDumped = 0;
            this.framePayloadLength = 0;
            this.framePayloadDumped = 0;
            this.xMessageHeader = null;
        }
    }

    /**
     * Ensures that this {@link OutputStream} wasn't closed yet.
     *
     * @throws IOException
     *             if this {@link OutputStream} was closed.
     */
    private void ensureOpen() throws IOException {
        if (this.closed) {
            throw new IOException("Stream closed");
        }
    }

}
