/*
 * Copyright (c) 2026, Oracle and/or its affiliates.
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

import java.nio.ByteBuffer;

import com.github.luben.zstd.EndDirective;
import com.github.luben.zstd.ZstdCompressCtx;

/**
 * Compresses the packets of one connection with zstd, each one into its own frame, reusing one compression context.
 */
class ZstdPacketCompressor {

    private static final ByteBuffer NO_INPUT = ByteBuffer.allocate(0);

    private final ZstdCompressCtx context;
    private final int level;
    private ByteBuffer target;
    private boolean overflow;

    ZstdPacketCompressor(int level) {
        this.level = level;
        this.context = new ZstdCompressCtx();
    }

    /**
     * Starts a frame that is written to the given buffer.
     *
     * @param buffer
     *            the buffer for the compressed packet
     */
    void start(byte[] buffer) {
        this.context.reset();
        this.context.setLevel(this.level);
        this.target = ByteBuffer.wrap(buffer);
        this.overflow = false;
    }

    /**
     * Compresses bytes into the current frame.
     *
     * @param data
     *            bytes to compress
     * @param offset
     *            offset
     * @param len
     *            length
     */
    void add(byte[] data, int offset, int len) {
        ByteBuffer source = ByteBuffer.wrap(data, offset, len);
        while (!this.overflow && source.hasRemaining()) {
            this.context.compressByteBufferStream(this.target, source, EndDirective.CONTINUE);
            this.overflow = !this.target.hasRemaining();
        }
    }

    /**
     * Ends the current frame.
     *
     * @return the length of the frame, or the length of the whole buffer if the frame does not fit in it.
     */
    int finish() {
        while (!this.overflow && !this.context.compressByteBufferStream(this.target, NO_INPUT, EndDirective.END)) {
            this.overflow = !this.target.hasRemaining();
        }
        if (this.overflow) {
            return this.target.capacity();
        }
        return this.target.position();
    }

    void close() {
        this.context.close();
    }

}
