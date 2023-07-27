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

package testsuite;

import static org.junit.jupiter.api.Assertions.assertFalse;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.nio.channels.SocketChannel;
import java.util.HashSet;
import java.util.Set;

import com.mysql.cj.conf.HostInfo;
import com.mysql.cj.conf.PropertySet;
import com.mysql.cj.protocol.StandardSocketFactory;

public class InjectedSocketFactory extends StandardSocketFactory {

    static final Set<String> IMMEDIATELY_DOWNED_HOSTS_PORT = new HashSet<>();

    private HostInfo hi;
    private PropertySet propSet;

    public static byte[] injectedBuffer = null;
    protected static int injectedBufferPos = 0;

    public static void flushAllStaticData() {
        IMMEDIATELY_DOWNED_HOSTS_PORT.clear();
        injectedBuffer = null;
        injectedBufferPos = 0;
    }

    public static void downHost(String hostPortPair) {
        IMMEDIATELY_DOWNED_HOSTS_PORT.add(hostPortPair);
    }

    public static void dontDownHost(String hostname) {
        IMMEDIATELY_DOWNED_HOSTS_PORT.remove(hostname);
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T extends Closeable> T connect(String host_name, int port_number, PropertySet pset, int loginTimeout) throws IOException {
        this.loginTimeoutCountdown = loginTimeout;
        this.hi = new HostInfo(null, host_name, port_number, null, null);
        this.propSet = pset;

        Socket socket = null;
        try {
            if (IMMEDIATELY_DOWNED_HOSTS_PORT.contains(this.hi.getHostPortPair())) {
                throw new SocketTimeoutException();
            }
            this.rawSocket = new SocketWrapper(super.connect(this.hi.getHost(), this.hi.getPort(), this.propSet, this.loginTimeoutCountdown), this.propSet,
                    this.hi);
            socket = this.rawSocket;
        } catch (IOException e) {
            throw e;
        }
        return (T) socket;
    }

    class SocketWrapper extends Socket {

        final Socket underlyingSocket;
        final PropertySet propSet;
        final HostInfo hi;

        SocketWrapper(Socket realSocket, PropertySet pset, HostInfo hi) {
            this.underlyingSocket = realSocket;
            this.propSet = pset;
            this.hi = hi;
        }

        @Override
        public void bind(SocketAddress bindpoint) throws IOException {
            this.underlyingSocket.bind(bindpoint);
        }

        @Override
        public synchronized void close() throws IOException {
            this.underlyingSocket.close();
        }

        @Override
        public SocketChannel getChannel() {
            return this.underlyingSocket.getChannel();
        }

        @Override
        public InetAddress getInetAddress() {
            return this.underlyingSocket.getInetAddress();
        }

        @Override
        public InputStream getInputStream() throws IOException {
            return new InjectedInputStream(this.underlyingSocket.getInputStream(), this.propSet, this.hi);
        }

        @Override
        public boolean getKeepAlive() throws SocketException {
            return this.underlyingSocket.getKeepAlive();
        }

        @Override
        public InetAddress getLocalAddress() {
            return this.underlyingSocket.getLocalAddress();
        }

        @Override
        public int getLocalPort() {
            return this.underlyingSocket.getLocalPort();
        }

        @Override
        public SocketAddress getLocalSocketAddress() {
            return this.underlyingSocket.getLocalSocketAddress();
        }

        @Override
        public boolean getOOBInline() throws SocketException {
            return this.underlyingSocket.getOOBInline();
        }

        @Override
        public OutputStream getOutputStream() throws IOException {
            return this.underlyingSocket.getOutputStream();
        }

        @Override
        public int getPort() {
            return this.underlyingSocket.getPort();
        }

        @Override
        public synchronized int getReceiveBufferSize() throws SocketException {
            return this.underlyingSocket.getReceiveBufferSize();
        }

        @Override
        public SocketAddress getRemoteSocketAddress() {
            return this.underlyingSocket.getRemoteSocketAddress();
        }

        @Override
        public boolean getReuseAddress() throws SocketException {
            return this.underlyingSocket.getReuseAddress();
        }

        @Override
        public synchronized int getSendBufferSize() throws SocketException {
            return this.underlyingSocket.getSendBufferSize();
        }

        @Override
        public int getSoLinger() throws SocketException {
            return this.underlyingSocket.getSoLinger();
        }

        @Override
        public synchronized int getSoTimeout() throws SocketException {
            return this.underlyingSocket.getSoTimeout();
        }

        @Override
        public boolean getTcpNoDelay() throws SocketException {
            return this.underlyingSocket.getTcpNoDelay();
        }

        @Override
        public int getTrafficClass() throws SocketException {
            return this.underlyingSocket.getTrafficClass();
        }

        @Override
        public boolean isBound() {
            return this.underlyingSocket.isBound();
        }

        @Override
        public boolean isClosed() {
            return this.underlyingSocket.isClosed();
        }

        @Override
        public boolean isConnected() {
            return this.underlyingSocket.isConnected();
        }

        @Override
        public boolean isInputShutdown() {
            return this.underlyingSocket.isInputShutdown();
        }

        @Override
        public boolean isOutputShutdown() {
            return this.underlyingSocket.isOutputShutdown();
        }

        @Override
        public void sendUrgentData(int data) throws IOException {
            this.underlyingSocket.sendUrgentData(data);
        }

        @Override
        public void setKeepAlive(boolean on) throws SocketException {
            this.underlyingSocket.setKeepAlive(on);
        }

        @Override
        public void setOOBInline(boolean on) throws SocketException {
            this.underlyingSocket.setOOBInline(on);
        }

        @Override
        public synchronized void setReceiveBufferSize(int size) throws SocketException {
            this.underlyingSocket.setReceiveBufferSize(size);
        }

        @Override
        public void setReuseAddress(boolean on) throws SocketException {
            this.underlyingSocket.setReuseAddress(on);
        }

        @Override
        public synchronized void setSendBufferSize(int size) throws SocketException {
            this.underlyingSocket.setSendBufferSize(size);
        }

        @Override
        public void setSoLinger(boolean on, int linger) throws SocketException {
            this.underlyingSocket.setSoLinger(on, linger);
        }

        @Override
        public synchronized void setSoTimeout(int timeout) throws SocketException {
            this.underlyingSocket.setSoTimeout(timeout);
        }

        @Override
        public void setTcpNoDelay(boolean on) throws SocketException {
            this.underlyingSocket.setTcpNoDelay(on);
        }

        @Override
        public void setTrafficClass(int tc) throws SocketException {
            this.underlyingSocket.setTrafficClass(tc);
        }

        @Override
        public void shutdownInput() throws IOException {
            this.underlyingSocket.shutdownInput();
        }

        @Override
        public void shutdownOutput() throws IOException {
            this.underlyingSocket.shutdownOutput();
        }

        @Override
        public String toString() {
            return this.underlyingSocket.toString();
        }

    }

    static class InjectedInputStream extends InputStream {

        final InputStream underlyingInputStream;
        final PropertySet propSet;
        final HostInfo hi;
        int loopCount = 0;

        InjectedInputStream(InputStream realInputStream, PropertySet pset, HostInfo hi) {
            this.underlyingInputStream = realInputStream;
            this.propSet = pset;
            this.hi = hi;
        }

        @Override
        public int available() throws IOException {
            return this.underlyingInputStream.available();
        }

        @Override
        public void close() throws IOException {
            this.underlyingInputStream.close();
        }

        @Override
        public synchronized void mark(int readlimit) {
            this.underlyingInputStream.mark(readlimit);
        }

        @Override
        public boolean markSupported() {
            return this.underlyingInputStream.markSupported();
        }

        @Override
        public synchronized void reset() throws IOException {
            this.underlyingInputStream.reset();
        }

        @Override
        public long skip(long n) throws IOException {
            return this.underlyingInputStream.skip(n);
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            if (injectedBuffer != null) {
                if (injectedBuffer.length - injectedBufferPos < len) {
                    len = injectedBuffer.length - injectedBufferPos;
                }

                System.arraycopy(injectedBuffer, injectedBufferPos, b, off, len);
                injectedBufferPos += len;

                if (injectedBuffer.length - injectedBufferPos == 0) {
                    injectedBuffer = null;
                    injectedBufferPos = 0;
                }
                return len;
            }

            try {
                int readCount = this.underlyingInputStream.read(b, off, len);
                this.loopCount = 0;
                return readCount;
            } catch (SocketTimeoutException e) {
                this.loopCount++;
                assertFalse(this.loopCount > 10, "Probable infinite loop at MySQLIO.clearInputStream().");
                return -1;
            }
        }

        @Override
        public int read(byte[] b) throws IOException {
            this.loopCount = 0;
            return this.underlyingInputStream.read(b);
        }

        @Override
        public int read() throws IOException {
            this.loopCount = 0;
            return this.underlyingInputStream.read();
        }

    }

}
