/*
 * Copyright (c) 2020, Oracle and/or its affiliates.
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

import java.io.InputStream;
import java.io.OutputStream;

import com.mysql.cj.Messages;
import com.mysql.cj.util.Util;

/**
 * Factory class for producing inflating and deflating-able {@link InputStream} and {@link OutputStream} instances for a selected compression algorithm.
 */
public class CompressorStreamsFactory {
    private CompressionAlgorithm compressionAlgorithm;

    private InputStream compressorInputStreamInstance = null;
    private ReusableInputStream underlyingInputStream = null;

    private OutputStream compressorOutputStreamInstance = null;
    private ReusableOutputStream underlyingOutputStream = null;

    public CompressorStreamsFactory(CompressionAlgorithm algorithm) {
        this.compressionAlgorithm = algorithm;
    }

    public CompressionMode getCompressionMode() {
        return this.compressionAlgorithm.getCompressionMode();
    }

    public boolean areCompressedStreamsReusable() {
        return getCompressionMode() == CompressionMode.STREAM;
    }

    /**
     * Creates an instance of an {@link InputStream} that wraps around the given {@link InputStream} and knows how to inflate data using the algorithm given in
     * this class' constructor.
     * 
     * @param in
     *            the {@link InputStream} to use as source of the bytes to inflate.
     * @return
     *         the new inflater {@link InputStream} wrapper instance.
     */
    public InputStream getInputStreamInstance(InputStream in) {
        InputStream underlyingIn = in;

        if (areCompressedStreamsReusable()) {
            if (this.compressorInputStreamInstance != null) {
                this.underlyingInputStream.setInputStream(underlyingIn);
                return this.compressorInputStreamInstance;
            }
            this.underlyingInputStream = new ReusableInputStream(underlyingIn);
            underlyingIn = this.underlyingInputStream;
        }

        InputStream compressionIn = (InputStream) Util.getInstance(this.compressionAlgorithm.getInputStreamClass().getName(),
                new Class<?>[] { InputStream.class }, new Object[] { underlyingIn }, null, Messages.getString("Protocol.Compression.IoFactory.0",
                        new Object[] { this.compressionAlgorithm.getInputStreamClass().getName(), this.compressionAlgorithm }));

        if (areCompressedStreamsReusable()) {
            this.compressorInputStreamInstance = compressionIn;
        }
        return compressionIn;
    }

    /**
     * Creates an instance of an {@link OutputStream} that wraps around the given {@link OutputStream} and knows how to deflate data using the algorithm given
     * in this class' constructor.
     * 
     * @param out
     *            the {@link OutputStream} to use as target of the bytes to deflate.
     * @return
     *         the new deflater {@link OutputStream} wrapper instance.
     */
    public OutputStream getOutputStreamInstance(OutputStream out) {
        OutputStream underlyingOut = out;

        if (areCompressedStreamsReusable()) {
            if (this.compressorOutputStreamInstance != null) {
                this.underlyingOutputStream.setOutputStream(underlyingOut);
                return this.compressorOutputStreamInstance;
            }
            this.underlyingOutputStream = new ReusableOutputStream(underlyingOut);
            underlyingOut = this.underlyingOutputStream;
        }

        OutputStream compressionOut = (OutputStream) Util.getInstance(this.compressionAlgorithm.getOutputStreamClass().getName(),
                new Class<?>[] { OutputStream.class }, new Object[] { underlyingOut }, null, Messages.getString("Protocol.Compression.IoFactory.1",
                        new Object[] { this.compressionAlgorithm.getOutputStreamClass().getName(), this.compressionAlgorithm }));

        if (areCompressedStreamsReusable()) {
            compressionOut = new ContinuousOutputStream(compressionOut);
            this.compressorOutputStreamInstance = compressionOut;
        }
        return compressionOut;
    }
}
