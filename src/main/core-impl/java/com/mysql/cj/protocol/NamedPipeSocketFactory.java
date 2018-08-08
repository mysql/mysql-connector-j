/*
 * Copyright (c) 2002, 2018, Oracle and/or its affiliates. All rights reserved.
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

package com.mysql.cj.protocol;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.net.Socket;
import java.net.SocketException;

import com.mysql.cj.Messages;
import com.mysql.cj.Session;
import com.mysql.cj.conf.PropertyKey;
import com.mysql.cj.conf.PropertySet;

/**
 * A socket factory for named pipes (on Windows)
 */
public class NamedPipeSocketFactory implements SocketFactory {
    /**
     * A socket that encapsulates named pipes on Windows
     */
    class NamedPipeSocket extends Socket {
        private boolean isClosed = false;

        private RandomAccessFile namedPipeFile;

        NamedPipeSocket(String filePath) throws IOException {
            if ((filePath == null) || (filePath.length() == 0)) {
                throw new IOException(Messages.getString("NamedPipeSocketFactory.4"));
            }

            this.namedPipeFile = new RandomAccessFile(filePath, "rw");
        }

        /**
         * @see java.net.Socket#close()
         */
        @Override
        public synchronized void close() throws IOException {
            this.namedPipeFile.close();
            this.isClosed = true;
        }

        /**
         * @see java.net.Socket#getInputStream()
         */
        @Override
        public InputStream getInputStream() throws IOException {
            return new RandomAccessFileInputStream(this.namedPipeFile);
        }

        /**
         * @see java.net.Socket#getOutputStream()
         */
        @Override
        public OutputStream getOutputStream() throws IOException {
            return new RandomAccessFileOutputStream(this.namedPipeFile);
        }

        /**
         * @see java.net.Socket#isClosed()
         */
        @Override
        public boolean isClosed() {
            return this.isClosed;
        }

        @Override
        public void shutdownInput() throws IOException {
            // no-op
        }
    }

    /**
     * Enables OutputStream-type functionality for a RandomAccessFile
     */
    class RandomAccessFileInputStream extends InputStream {
        RandomAccessFile raFile;

        RandomAccessFileInputStream(RandomAccessFile file) {
            this.raFile = file;
        }

        /**
         * @see java.io.InputStream#available()
         */
        @Override
        public int available() throws IOException {
            return -1;
        }

        /**
         * @see java.io.InputStream#close()
         */
        @Override
        public void close() throws IOException {
            this.raFile.close();
        }

        /**
         * @see java.io.InputStream#read()
         */
        @Override
        public int read() throws IOException {
            return this.raFile.read();
        }

        /**
         * @see java.io.InputStream#read(byte[])
         */
        @Override
        public int read(byte[] b) throws IOException {
            return this.raFile.read(b);
        }

        /**
         * @see java.io.InputStream#read(byte[], int, int)
         */
        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            return this.raFile.read(b, off, len);
        }
    }

    /**
     * Enables OutputStream-type functionality for a RandomAccessFile
     */
    class RandomAccessFileOutputStream extends OutputStream {
        RandomAccessFile raFile;

        RandomAccessFileOutputStream(RandomAccessFile file) {
            this.raFile = file;
        }

        /**
         * @see java.io.OutputStream#close()
         */
        @Override
        public void close() throws IOException {
            this.raFile.close();
        }

        /**
         * @see java.io.OutputStream#write(byte[])
         */
        @Override
        public void write(byte[] b) throws IOException {
            this.raFile.write(b);
        }

        /**
         * @see java.io.OutputStream#write(byte[], int, int)
         */
        @Override
        public void write(byte[] b, int off, int len) throws IOException {
            this.raFile.write(b, off, len);
        }

        /**
         * @see java.io.OutputStream#write(int)
         */
        @Override
        public void write(int b) throws IOException {
        }
    }

    private Socket namedPipeSocket;

    /**
     * Constructor for NamedPipeSocketFactory.
     */
    public NamedPipeSocketFactory() {
        super();
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T extends Closeable> T performTlsHandshake(SocketConnection socketConnection, ServerSession serverSession) throws IOException {
        return (T) this.namedPipeSocket;
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T extends Closeable> T connect(String host, int portNumber /* ignored */, PropertySet props, int loginTimeout) throws IOException {
        String namedPipePath = props.getStringProperty(PropertyKey.PATH).getValue();

        if (namedPipePath == null) {
            namedPipePath = "\\\\.\\pipe\\MySQL";
        } else if (namedPipePath.length() == 0) {
            throw new SocketException(
                    Messages.getString("NamedPipeSocketFactory.2") + PropertyKey.PATH.getCcAlias() + Messages.getString("NamedPipeSocketFactory.3"));
        }

        this.namedPipeSocket = new NamedPipeSocket(namedPipePath);

        return (T) this.namedPipeSocket;
    }

    @Override
    public boolean isLocallyConnected(Session sess) {
        // Until I learn otherwise (or learn how to detect it), I assume that we are
        return true;
    }
}
