/*
 * Copyright (c) 2002, 2024, Oracle and/or its affiliates.
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

package com.mysql.cj.protocol;

import java.io.Closeable;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.net.Socket;
import java.net.SocketException;
import java.util.concurrent.TimeUnit;

import com.mysql.cj.Messages;
import com.mysql.cj.Session;
import com.mysql.cj.conf.PropertyKey;
import com.mysql.cj.conf.PropertySet;
import com.mysql.cj.conf.RuntimeProperty;
import com.mysql.cj.log.Log;

/**
 * A socket factory for named pipes (on Windows)
 */
public class NamedPipeSocketFactory implements SocketFactory {

    private static final int DEFAULT_TIMEOUT = 100;

    /**
     * A socket that encapsulates named pipes on Windows
     */
    class NamedPipeSocket extends Socket {

        private boolean isClosed = false;

        private RandomAccessFile namedPipeFile;

        NamedPipeSocket(String filePath, int timeout) throws IOException {
            if (filePath == null || filePath.length() == 0) {
                throw new IOException(Messages.getString("NamedPipeSocketFactory.4"));
            }

            int timeoutCountdown = timeout == 0 ? DEFAULT_TIMEOUT : timeout;
            long startTime = System.currentTimeMillis();
            for (;;) {
                try {
                    this.namedPipeFile = new RandomAccessFile(filePath, "rw");
                    break;
                } catch (FileNotFoundException e) {
                    if (timeout == 0) { // No timeout was set.
                        throw new IOException("Named pipe busy error (ERROR_PIPE_BUSY).\nConsider setting a value for "
                                + "'connectTimeout' or DriverManager.setLoginTimeout(int) to repeatedly try opening the named pipe before failing.", e);
                    }
                    if (System.currentTimeMillis() - startTime > timeoutCountdown) {
                        throw e;
                    }
                }
                try {
                    TimeUnit.MILLISECONDS.sleep(10);
                } catch (InterruptedException e) {
                    throw new IOException(e);
                }
            }
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

    @Override
    public <T extends Closeable> T performTlsHandshake(SocketConnection socketConnection, ServerSession serverSession) throws IOException {
        return performTlsHandshake(socketConnection, serverSession, null);
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T extends Closeable> T performTlsHandshake(SocketConnection socketConnection, ServerSession serverSession, Log log) throws IOException {
        return (T) this.namedPipeSocket;
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T extends Closeable> T connect(String host, int portNumber /* ignored */, PropertySet props, int loginTimeout) throws IOException {
        String namedPipePath = null;

        RuntimeProperty<String> path = props.getStringProperty(PropertyKey.PATH);
        if (path != null) {
            namedPipePath = path.getValue();
        }

        if (namedPipePath == null) {
            namedPipePath = "\\\\.\\pipe\\MySQL";
        } else if (namedPipePath.length() == 0) {
            throw new SocketException(
                    Messages.getString("NamedPipeSocketFactory.2") + PropertyKey.PATH.getCcAlias() + Messages.getString("NamedPipeSocketFactory.3"));
        }

        int connectTimeout = props.getIntegerProperty(PropertyKey.connectTimeout.getKeyName()).getValue();
        int timeout = connectTimeout > 0 && loginTimeout > 0 ? Math.min(connectTimeout, loginTimeout) : connectTimeout + loginTimeout;

        this.namedPipeSocket = new NamedPipeSocket(namedPipePath, timeout);

        return (T) this.namedPipeSocket;
    }

    @Override
    public boolean isLocallyConnected(Session sess) {
        // Until I learn otherwise (or learn how to detect it), I assume that we are
        return true;
    }

}
