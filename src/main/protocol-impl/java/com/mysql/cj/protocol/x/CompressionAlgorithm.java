/*
 * Copyright (c) 2020, Oracle and/or its affiliates. All rights reserved.
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
import java.util.HashMap;
import java.util.Map;
import java.util.zip.InflaterInputStream;

import com.mysql.cj.Messages;
import com.mysql.cj.exceptions.ExceptionFactory;
import com.mysql.cj.exceptions.WrongArgumentException;

/**
 * Contains all information about a compression algorithm, its name, compression mode and classes that implement the inflating and deflating streams of data.
 */
public class CompressionAlgorithm {
    private String algorithmName;
    private CompressionMode compressionMode;
    private Class<?> inputStreamClass;
    private Class<?> outputStreamClass;

    /**
     * Returns a list of the compression algorithms supported natively. Additional algorithms can be registered by user.
     * 
     * @return a list of the compression algorithms supported natively.
     */
    public static Map<String, CompressionAlgorithm> getDefaultInstances() {
        HashMap<String, CompressionAlgorithm> defaultInstances = new HashMap<>();
        defaultInstances.put("deflate_stream", new CompressionAlgorithm("deflate_stream", InflaterInputStream.class, SyncFlushDeflaterOutputStream.class));
        return defaultInstances;
    }

    public CompressionAlgorithm(String name, Class<?> inputStreamClass, Class<?> outputStreamClass) {
        this.algorithmName = name.toLowerCase();
        String[] nameMode = name.split("_");
        if (nameMode.length != 2) {
            throw ExceptionFactory.createException(WrongArgumentException.class, Messages.getString("Protocol.Compression.6", new Object[] { name }));
        }
        try {
            CompressionMode mode = CompressionMode.valueOf(nameMode[1].toUpperCase());
            this.compressionMode = mode;
        } catch (IllegalArgumentException e) {
            throw ExceptionFactory.createException(WrongArgumentException.class, Messages.getString("Protocol.Compression.7", new Object[] { nameMode[1] }));
        }
        this.inputStreamClass = inputStreamClass;
        this.outputStreamClass = outputStreamClass;
    }

    /**
     * Gets this algorithm's name.
     * 
     * @return the algorithm name.
     */
    public String getAlgorithmName() {
        return this.algorithmName;
    }

    /**
     * Gets this algorithm's compression mode.
     * 
     * @return the algorithm {@link CompressionMode}
     */
    public CompressionMode getCompressionMode() {
        return this.compressionMode;
    }

    /**
     * Gets this algorithm's {@link InputStream} implementation class that can be used to inflate data.
     * 
     * @return the {@link InputStream} that knows how to deflate data.
     */
    public Class<?> getInputStreamClass() {
        return this.inputStreamClass;
    }

    /**
     * Gets this algorithm's {@link OutputStream} implementation class that can be used to deflate data.
     * 
     * @return the {@link OutputStream} that knows how to deflate data.
     */
    public Class<?> getOutputStreamClass() {
        return this.outputStreamClass;
    }
}