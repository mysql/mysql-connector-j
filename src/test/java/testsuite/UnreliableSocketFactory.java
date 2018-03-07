/*
 * Copyright (c) 2008, 2018, Oracle and/or its affiliates. All rights reserved.
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
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import com.mysql.cj.conf.PropertyDefinitions;
import com.mysql.cj.protocol.StandardSocketFactory;

/**
 * Configure "socketFactory" to use this class in your JDBC URL, and it will operate as normal, unless you map some host aliases to actual IP addresses, and
 * then have the test driver call hangOnConnect/Read/Write() which simulate the given failure condition for the host with the <b>alias</b> argument, and will
 * honor connect or socket timeout properties.
 * 
 * You can also cause a host to be immediately-downed by calling downHost() with an alias.
 * 
 * ATTENTION! This class is *NOT* thread safe.
 */
public class UnreliableSocketFactory extends StandardSocketFactory {
    public static final String STATUS_UNKNOWN = "?";
    public static final String STATUS_CONNECTED = "/";
    public static final String STATUS_FAILED = "\\";

    public static final long DEFAULT_TIMEOUT_MILLIS = 10 * 60 * 1000; // ugh

    private static final Map<String, String> MAPPED_HOSTS = new HashMap<>();
    static final Set<String> HUNG_READ_HOSTS = new HashSet<>();
    static final Set<String> HUNG_WRITE_HOSTS = new HashSet<>();
    static final Set<String> HUNG_CONNECT_HOSTS = new HashSet<>();
    static final Set<String> IMMEDIATELY_DOWNED_HOSTS = new HashSet<>();
    static final List<String> CONNECTION_ATTEMPTS = new LinkedList<>();

    private String hostname;
    private int portNumber;
    private Properties props;

    public static String getHostConnectedStatus(String host) {
        return STATUS_CONNECTED + host;
    }

    public static String getHostFailedStatus(String host) {
        return STATUS_FAILED + host;
    }

    public static String getHostUnknownStatus(String host) {
        return STATUS_FAILED + host;
    }

    public static void flushAllStaticData() {
        IMMEDIATELY_DOWNED_HOSTS.clear();
        HUNG_CONNECT_HOSTS.clear();
        HUNG_READ_HOSTS.clear();
        HUNG_WRITE_HOSTS.clear();
        flushConnectionAttempts();
    }

    public static void flushConnectionAttempts() {
        CONNECTION_ATTEMPTS.clear();
    }

    public static void mapHost(String alias, String orig) {
        MAPPED_HOSTS.put(alias, orig);
    }

    public static void hangOnRead(String hostname) {
        HUNG_READ_HOSTS.add(hostname);
    }

    public static void dontHangOnRead(String hostname) {
        HUNG_READ_HOSTS.remove(hostname);
    }

    public static void hangOnWrite(String hostname) {
        HUNG_WRITE_HOSTS.add(hostname);
    }

    public static void dontHangOnWrite(String hostname) {
        HUNG_WRITE_HOSTS.remove(hostname);
    }

    public static void hangOnConnect(String hostname) {
        HUNG_CONNECT_HOSTS.add(hostname);
    }

    public static void dontHangOnConnect(String hostname) {
        HUNG_CONNECT_HOSTS.remove(hostname);
    }

    public static void downHost(String hostname) {
        IMMEDIATELY_DOWNED_HOSTS.add(hostname);

    }

    public static void dontDownHost(String hostname) {
        IMMEDIATELY_DOWNED_HOSTS.remove(hostname);
    }

    public static String getHostFromLastConnection() {
        return getHostFromPastConnection(1);
    }

    public static String getHostFromPastConnection(int pos) {
        pos = Math.abs(pos);
        if (pos == 0 || CONNECTION_ATTEMPTS.isEmpty() || CONNECTION_ATTEMPTS.size() < pos) {
            return null;
        }
        return CONNECTION_ATTEMPTS.get(CONNECTION_ATTEMPTS.size() - pos);
    }

    public static List<String> getHostsFromAllConnections() {
        return getHostsFromLastConnections(CONNECTION_ATTEMPTS.size());
    }

    public static List<String> getHostsFromLastConnections(int count) {
        count = Math.abs(count);
        int lBound = Math.max(0, CONNECTION_ATTEMPTS.size() - count);
        return CONNECTION_ATTEMPTS.subList(lBound, CONNECTION_ATTEMPTS.size());
    }

    public static boolean isConnected() {
        String lastHost = getHostFromLastConnection();
        return lastHost == null ? false : lastHost.startsWith(STATUS_CONNECTED);
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T extends Closeable> T connect(String host_name, int port_number, Properties prop, int loginTimeout) throws IOException {
        this.loginTimeoutCountdown = loginTimeout;
        this.hostname = host_name;
        this.portNumber = port_number;
        this.props = prop;

        Socket socket = null;
        String result = STATUS_UNKNOWN;
        try {
            socket = getNewSocket();
            result = STATUS_CONNECTED;
        } catch (SocketException e) {
            result = STATUS_FAILED;
            throw e;
        } catch (IOException e) {
            result = STATUS_FAILED;
            throw e;
        } finally {
            CONNECTION_ATTEMPTS.add(result + host_name);
        }
        return (T) socket;
    }

    private Socket getNewSocket() throws SocketException, IOException {
        if (IMMEDIATELY_DOWNED_HOSTS.contains(this.hostname)) {

            sleepMillisForProperty(this.props, PropertyDefinitions.PNAME_connectTimeout);

            throw new SocketTimeoutException();
        }

        String hostnameToConnectTo = MAPPED_HOSTS.get(this.hostname);

        if (hostnameToConnectTo == null) {
            hostnameToConnectTo = this.hostname;
        }

        this.rawSocket = new HangingSocket(super.connect(hostnameToConnectTo, this.portNumber, this.props, this.loginTimeoutCountdown), this.props,
                this.hostname);
        return this.rawSocket;
    }

    static void sleepMillisForProperty(Properties props, String name) {
        try {
            Thread.sleep(Long.parseLong(props.getProperty(name, String.valueOf(DEFAULT_TIMEOUT_MILLIS))));
        } catch (NumberFormatException e) {
            throw new RuntimeException(e);
        } catch (InterruptedException e) {
            // ignore
        }
    }

    class HangingSocket extends Socket {

        final Socket underlyingSocket;
        final Properties props;
        final String aliasedHostname;

        HangingSocket(Socket realSocket, Properties props, String aliasedHostname) {
            this.underlyingSocket = realSocket;
            this.props = props;
            this.aliasedHostname = aliasedHostname;
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

            return new HangingInputStream(this.underlyingSocket.getInputStream(), this.props, this.aliasedHostname);
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
            return new HangingOutputStream(this.underlyingSocket.getOutputStream(), this.props, this.aliasedHostname);
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

    static class HangingInputStream extends InputStream {
        final InputStream underlyingInputStream;
        final Properties props;
        final String aliasedHostname;

        HangingInputStream(InputStream realInputStream, Properties props, String aliasedHostname) {
            this.underlyingInputStream = realInputStream;
            this.props = props;
            this.aliasedHostname = aliasedHostname;
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
            failIfRequired();

            return this.underlyingInputStream.skip(n);
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            failIfRequired();

            return this.underlyingInputStream.read(b, off, len);
        }

        @Override
        public int read(byte[] b) throws IOException {
            failIfRequired();

            return this.underlyingInputStream.read(b);
        }

        @Override
        public int read() throws IOException {
            failIfRequired();

            return this.underlyingInputStream.read();
        }

        private void failIfRequired() throws SocketTimeoutException {
            if (HUNG_READ_HOSTS.contains(this.aliasedHostname) || IMMEDIATELY_DOWNED_HOSTS.contains(this.aliasedHostname)) {
                sleepMillisForProperty(this.props, PropertyDefinitions.PNAME_socketTimeout);

                throw new SocketTimeoutException();
            }
        }
    }

    static class HangingOutputStream extends OutputStream {

        final Properties props;
        final String aliasedHostname;
        final OutputStream underlyingOutputStream;

        HangingOutputStream(OutputStream realOutputStream, Properties props, String aliasedHostname) {
            this.underlyingOutputStream = realOutputStream;
            this.props = props;
            this.aliasedHostname = aliasedHostname;
        }

        @Override
        public void close() throws IOException {
            failIfRequired();
            this.underlyingOutputStream.close();
        }

        @Override
        public void flush() throws IOException {
            this.underlyingOutputStream.flush();
        }

        @Override
        public void write(byte[] b, int off, int len) throws IOException {
            failIfRequired();
            this.underlyingOutputStream.write(b, off, len);
        }

        @Override
        public void write(byte[] b) throws IOException {
            failIfRequired();
            this.underlyingOutputStream.write(b);
        }

        @Override
        public void write(int b) throws IOException {
            failIfRequired();
            this.underlyingOutputStream.write(b);
        }

        private void failIfRequired() throws SocketTimeoutException {
            if (HUNG_WRITE_HOSTS.contains(this.aliasedHostname) || IMMEDIATELY_DOWNED_HOSTS.contains(this.aliasedHostname)) {
                sleepMillisForProperty(this.props, PropertyDefinitions.PNAME_socketTimeout);

                throw new SocketTimeoutException();
            }
        }

    }
}
