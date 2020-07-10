/*
 * Copyright (c) 2002, 2020, Oracle and/or its affiliates.
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
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketException;

import com.mysql.cj.Messages;
import com.mysql.cj.conf.PropertyKey;
import com.mysql.cj.conf.PropertySet;

/**
 * Socket factory for vanilla TCP/IP sockets (the standard)
 */
public class StandardSocketFactory implements SocketFactory {

    /** The hostname to connect to */
    protected String host = null;

    /** The port number to connect to */
    protected int port = 3306;

    /** The underlying TCP/IP socket to use */
    protected Socket rawSocket = null;

    /** The wrapper for underlying TCP/IP socket */
    protected Socket sslSocket = null;

    /** The remaining login time in milliseconds. Initial value set from defined DriverManager.setLoginTimeout() */
    protected int loginTimeoutCountdown = 0;

    /** Time when last Login Timeout check occurred */
    protected long loginTimeoutCheckTimestamp = System.currentTimeMillis();

    /** Backup original Socket timeout to be restored after handshake */
    protected int socketTimeoutBackup = 0;

    /**
     * Create the raw socket.
     *
     * @param props
     *            properties available to affect behaviour during socket creation.
     * @return socket
     */
    protected Socket createSocket(PropertySet props) {
        return new Socket();
    }

    /**
     * Configures socket properties based on properties from the connection
     * (tcpNoDelay, snd/rcv buf, traffic class, etc).
     * 
     * @param sock
     *            socket
     * @param pset
     *            properties
     * @throws SocketException
     *             if an error occurs
     * @throws IOException
     *             if an error occurs
     */
    private void configureSocket(Socket sock, PropertySet pset) throws SocketException, IOException {
        sock.setTcpNoDelay(pset.getBooleanProperty(PropertyKey.tcpNoDelay).getValue());
        sock.setKeepAlive(pset.getBooleanProperty(PropertyKey.tcpKeepAlive).getValue());

        int receiveBufferSize = pset.getIntegerProperty(PropertyKey.tcpRcvBuf).getValue();
        if (receiveBufferSize > 0) {
            sock.setReceiveBufferSize(receiveBufferSize);
        }

        int sendBufferSize = pset.getIntegerProperty(PropertyKey.tcpSndBuf).getValue();
        if (sendBufferSize > 0) {
            sock.setSendBufferSize(sendBufferSize);
        }

        int trafficClass = pset.getIntegerProperty(PropertyKey.tcpTrafficClass).getValue();
        if (trafficClass > 0) {
            sock.setTrafficClass(trafficClass);
        }
    }

    @SuppressWarnings("unchecked")
    public <T extends Closeable> T connect(String hostname, int portNumber, PropertySet pset, int loginTimeout) throws IOException {

        this.loginTimeoutCountdown = loginTimeout;

        if (pset != null) {
            this.host = hostname;

            this.port = portNumber;

            String localSocketHostname = pset.getStringProperty(PropertyKey.localSocketAddress).getValue();
            InetSocketAddress localSockAddr = null;
            if (localSocketHostname != null && localSocketHostname.length() > 0) {
                localSockAddr = new InetSocketAddress(InetAddress.getByName(localSocketHostname), 0);
            }

            int connectTimeout = pset.getIntegerProperty(PropertyKey.connectTimeout).getValue();

            if (this.host != null) {
                InetAddress[] possibleAddresses = InetAddress.getAllByName(this.host);

                if (possibleAddresses.length == 0) {
                    throw new SocketException("No addresses for host");
                }

                // save last exception to propagate to caller if connection fails
                SocketException lastException = null;

                // Need to loop through all possible addresses. Name lookup may return multiple addresses including IPv4 and IPv6 addresses. Some versions of
                // MySQL don't listen on the IPv6 address so we try all addresses.
                for (int i = 0; i < possibleAddresses.length; i++) {
                    try {
                        this.rawSocket = createSocket(pset);

                        configureSocket(this.rawSocket, pset);

                        InetSocketAddress sockAddr = new InetSocketAddress(possibleAddresses[i], this.port);
                        // bind to the local port if not using the ephemeral port
                        if (localSockAddr != null) {
                            this.rawSocket.bind(localSockAddr);
                        }

                        this.rawSocket.connect(sockAddr, getRealTimeout(connectTimeout));

                        break;
                    } catch (SocketException ex) {
                        lastException = ex;
                        resetLoginTimeCountdown();
                        this.rawSocket = null;
                    }
                }

                if (this.rawSocket == null && lastException != null) {
                    throw lastException;
                }

                resetLoginTimeCountdown();

                this.sslSocket = this.rawSocket;
                return (T) this.rawSocket;
            }
        }

        throw new SocketException("Unable to create socket");
    }

    public void beforeHandshake() throws IOException {
        resetLoginTimeCountdown();
        this.socketTimeoutBackup = this.rawSocket.getSoTimeout();
        this.rawSocket.setSoTimeout(getRealTimeout(this.socketTimeoutBackup));
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T extends Closeable> T performTlsHandshake(SocketConnection socketConnection, ServerSession serverSession) throws IOException {
        this.sslSocket = ExportControlled.performTlsHandshake(this.rawSocket, socketConnection,
                serverSession == null ? null : serverSession.getServerVersion());
        return (T) this.sslSocket;
    }

    public void afterHandshake() throws IOException {
        resetLoginTimeCountdown();
        this.rawSocket.setSoTimeout(this.socketTimeoutBackup);
    }

    /**
     * Decrements elapsed time since last reset from login timeout count down.
     * 
     * @throws SocketException
     *             If the login timeout is reached or exceeded.
     */
    protected void resetLoginTimeCountdown() throws SocketException {
        if (this.loginTimeoutCountdown > 0) {
            long now = System.currentTimeMillis();
            this.loginTimeoutCountdown -= now - this.loginTimeoutCheckTimestamp;
            if (this.loginTimeoutCountdown <= 0) {
                throw new SocketException(Messages.getString("Connection.LoginTimeout"));
            }
            this.loginTimeoutCheckTimestamp = now;
        }
    }

    /**
     * Validates the connection/socket timeout that must really be used.
     * 
     * @param expectedTimeout
     *            The timeout to validate.
     * @return The timeout to be used.
     */
    protected int getRealTimeout(int expectedTimeout) {
        if (this.loginTimeoutCountdown > 0 && (expectedTimeout == 0 || expectedTimeout > this.loginTimeoutCountdown)) {
            return this.loginTimeoutCountdown;
        }
        return expectedTimeout;
    }
}
