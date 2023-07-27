/*
 * Copyright (c) 2015, 2023, Oracle and/or its affiliates.
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

package com.mysql.cj.protocol.a;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.util.zip.Deflater;

import com.mysql.cj.protocol.MessageSender;

/**
 * A {@link MessageSender} for the compressed protocol.
 *
 * TODO: add support for pre-allocated buffer for large packets (if there's a demonstrable perf improvement)
 */
public class CompressedPacketSender implements MessageSender<NativePacketPayload> {

    private BufferedOutputStream outputStream;
    private Deflater deflater = new Deflater();
    /** Buffer to compress data to. Used only across one send() invocation. */
    private byte compressedPacket[];
    /** Sequence id of compressed packet. Used only across one send() invocation. */
    private byte compressedSequenceId = 0;
    /** Length of current compressed packet. */
    private int compressedPayloadLen = 0;

    public static final int COMP_HEADER_LENGTH = 7;
    public static final int MIN_COMPRESS_LEN = 50;

    public CompressedPacketSender(BufferedOutputStream outputStream) {
        this.outputStream = outputStream;
    }

    /**
     * Shut down this packet sender and deallocate any resources.
     */
    public void stop() {
        this.deflater.end();
        this.deflater = null;
    }

    private void resetPacket() {
        this.compressedPayloadLen = 0;
        this.deflater.reset();
    }

    /**
     * Add and compress the header for the raw packet into the compressed packet.
     *
     * @param packetSequence
     *            sequence id
     * @param uncompressedPacketLen
     *            uncompressed packet length
     */
    private void addUncompressedHeader(byte packetSequence, int uncompressedPacketLen) {
        byte uncompressedHeader[] = new byte[NativeConstants.HEADER_LENGTH];
        NativeUtils.encodeMysqlThreeByteInteger(uncompressedPacketLen, uncompressedHeader, 0);
        uncompressedHeader[3] = packetSequence;
        this.deflater.setInput(uncompressedHeader);
        this.compressedPayloadLen += this.deflater.deflate(this.compressedPacket, this.compressedPayloadLen,
                this.compressedPacket.length - this.compressedPayloadLen);
    }

    /**
     * Add and compress the payload into the compressed packet.
     *
     * @param payload
     *            payload bytes
     * @param payloadOffset
     *            offset
     * @param payloadLen
     *            length
     */
    private void addPayload(byte[] payload, int payloadOffset, int payloadLen) {
        this.deflater.setInput(payload, payloadOffset, payloadLen);
        this.compressedPayloadLen += this.deflater.deflate(this.compressedPacket, this.compressedPayloadLen,
                this.compressedPacket.length - this.compressedPayloadLen);
    }

    /**
     * Complete compression of the current payload contents to the compressed packet.
     */
    private void completeCompression() {
        this.deflater.finish();
        this.compressedPayloadLen += this.deflater.deflate(this.compressedPacket, this.compressedPayloadLen,
                this.compressedPacket.length - this.compressedPayloadLen);
    }

    /**
     * Write the compressed packet header.
     *
     * @param compLen
     *            compressed data length
     * @param seq
     *            sequence id
     * @param uncompLen
     *            uncompressed data length
     * @throws IOException
     *             if write exception occurs
     */
    private void writeCompressedHeader(int compLen, byte seq, int uncompLen) throws IOException {
        this.outputStream.write(NativeUtils.encodeMysqlThreeByteInteger(compLen));
        this.outputStream.write(seq);
        this.outputStream.write(NativeUtils.encodeMysqlThreeByteInteger(uncompLen));
    }

    /**
     * Write an uncompressed packet header.
     *
     * @param packetLen
     *            packet length
     * @param packetSequence
     *            sequence id
     * @throws IOException
     *             if write exception occurs
     */
    private void writeUncompressedHeader(int packetLen, byte packetSequence) throws IOException {
        this.outputStream.write(NativeUtils.encodeMysqlThreeByteInteger(packetLen));
        this.outputStream.write(packetSequence);
    }

    /**
     * Send a compressed packet.
     *
     * @param uncompressedPayloadLen
     *            uncompressed data length
     * @throws IOException
     *             if write exception occurs
     */
    private void sendCompressedPacket(int uncompressedPayloadLen) throws IOException {
        writeCompressedHeader(this.compressedPayloadLen, this.compressedSequenceId++, uncompressedPayloadLen);

        // compressed payload
        this.outputStream.write(this.compressedPacket, 0, this.compressedPayloadLen);
    }

    /**
     * Packet sender implementation for the compressed MySQL protocol. For compressed transmission of multi-packets, split the packets up in the same way as the
     * uncompressed protocol. We fit up to MAX_PACKET_SIZE bytes of split uncompressed packet, including the header, into an compressed packet. The first packet
     * of the multi-packet is 4 bytes of header and MAX_PACKET_SIZE - 4 bytes of the payload. The next packet must send the remaining four bytes of the payload
     * followed by a new header and payload. If the second split packet is also around MAX_PACKET_SIZE in length, then only MAX_PACKET_SIZE - 4 (from the
     * previous packet) - 4 (for the new header) can be sent. This means the payload will be limited by 8 bytes and this will continue to increase by 4 at every
     * iteration.
     *
     * @param packet
     *            data bytes
     * @param packetLen
     *            packet length
     * @param packetSequence
     *            sequence id
     * @throws IOException
     *             if i/o exception occurs
     */
    @Override
    public void send(byte[] packet, int packetLen, byte packetSequence) throws IOException {
        this.compressedSequenceId = packetSequence;

        // short-circuit send small packets without compression and return
        if (packetLen < MIN_COMPRESS_LEN) {
            writeCompressedHeader(packetLen + NativeConstants.HEADER_LENGTH, this.compressedSequenceId, 0);
            writeUncompressedHeader(packetLen, packetSequence);
            this.outputStream.write(packet, 0, packetLen);
            this.outputStream.flush();
            return;
        }

        if (packetLen + NativeConstants.HEADER_LENGTH > NativeConstants.MAX_PACKET_SIZE) {
            this.compressedPacket = new byte[NativeConstants.MAX_PACKET_SIZE];
        } else {
            this.compressedPacket = new byte[NativeConstants.HEADER_LENGTH + packetLen];
        }

        PacketSplitter packetSplitter = new PacketSplitter(packetLen);

        int unsentPayloadLen = 0;
        int unsentOffset = 0;
        // loop over constructing and sending compressed packets
        while (true) {
            this.compressedPayloadLen = 0;

            if (packetSplitter.nextPacket()) {
                // rest of previous packet
                if (unsentPayloadLen > 0) {
                    addPayload(packet, unsentOffset, unsentPayloadLen);
                }

                // current packet
                int remaining = NativeConstants.MAX_PACKET_SIZE - unsentPayloadLen;
                // if remaining is 0 then we are sending a very huge packet such that are 4-byte header-size carryover from last packet accumulated to the size
                // of a whole packet itself. We don't handle this. Would require 4 million packet segments (64 gigs in one logical packet)
                int len = Math.min(remaining, NativeConstants.HEADER_LENGTH + packetSplitter.getPacketLen());
                int lenNoHdr = len - NativeConstants.HEADER_LENGTH;
                addUncompressedHeader(packetSequence, packetSplitter.getPacketLen());
                addPayload(packet, packetSplitter.getOffset(), lenNoHdr);

                completeCompression();
                // don't send payloads with incompressible data
                if (this.compressedPayloadLen >= len) {
                    // combine the unsent and current packet in an uncompressed packet
                    writeCompressedHeader(unsentPayloadLen + len, this.compressedSequenceId++, 0);
                    this.outputStream.write(packet, unsentOffset, unsentPayloadLen);
                    writeUncompressedHeader(lenNoHdr, packetSequence);
                    this.outputStream.write(packet, packetSplitter.getOffset(), lenNoHdr);
                } else {
                    sendCompressedPacket(len + unsentPayloadLen);
                }

                packetSequence++;
                unsentPayloadLen = packetSplitter.getPacketLen() - lenNoHdr;
                unsentOffset = packetSplitter.getOffset() + lenNoHdr;
                resetPacket();
            } else if (unsentPayloadLen > 0) {
                // no more packets, send remaining unsent data
                addPayload(packet, unsentOffset, unsentPayloadLen);
                completeCompression();
                if (this.compressedPayloadLen >= unsentPayloadLen) {
                    writeCompressedHeader(unsentPayloadLen, this.compressedSequenceId, 0);
                    this.outputStream.write(packet, unsentOffset, unsentPayloadLen);
                } else {
                    sendCompressedPacket(unsentPayloadLen);
                }
                resetPacket();
                break;
            } else {
                // nothing left to send (only happens on boundaries)
                break;
            }
        }

        this.outputStream.flush();

        // release reference to (possibly large) compressed packet buffer
        this.compressedPacket = null;
    }

    @Override
    public MessageSender<NativePacketPayload> undecorateAll() {
        return this;
    }

    @Override
    public MessageSender<NativePacketPayload> undecorate() {
        return this;
    }

}
