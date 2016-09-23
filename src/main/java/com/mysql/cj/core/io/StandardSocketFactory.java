/*
  Copyright (c) 2002, 2015, Oracle and/or its affiliates. All rights reserved.

  The MySQL Connector/J is licensed under the terms of the GPLv2
  <http://www.gnu.org/licenses/old-licenses/gpl-2.0.html>, like most MySQL Connectors.
  There are special exceptions to the terms and conditions of the GPLv2 as it is applied to
  this software, see the FOSS License Exception
  <http://www.mysql.com/about/legal/licensing/foss-exception.html>.

  This program is free software; you can redistribute it and/or modify it under the terms
  of the GNU General Public License as published by the Free Software Foundation; version 2
  of the License.

  This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
  without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
  See the GNU General Public License for more details.

  You should have received a copy of the GNU General Public License along with this
  program; if not, write to the Free Software Foundation, Inc., 51 Franklin St, Fifth
  Floor, Boston, MA 02110-1301  USA

 */

package com.mysql.cj.core.io;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketException;
import java.util.Properties;

import com.mysql.cj.api.io.SocketFactory;
import com.mysql.cj.core.Messages;
import com.mysql.cj.core.conf.PropertyDefinitions;

/**
 * Socket factory for vanilla TCP/IP sockets (the standard)
 */
public class StandardSocketFactory implements SocketFactory {

    public static final String TCP_KEEP_ALIVE_DEFAULT_VALUE = "true";

    public static final String TCP_RCV_BUF_DEFAULT_VALUE = "0";

    public static final String TCP_SND_BUF_DEFAULT_VALUE = "0";

    public static final String TCP_TRAFFIC_CLASS_DEFAULT_VALUE = "0";

    public static final String TCP_NO_DELAY_DEFAULT_VALUE = "true";

    /** The hostname to connect to */
    protected String host = null;

    /** The port number to connect to */
    protected int port = 3306;

    /** The underlying TCP/IP socket to use */
    protected Socket rawSocket = null;

    /** The remaining login time in milliseconds. Initial value set from defined DriverManager.setLoginTimeout() */
    protected int loginTimeoutCountdown = 0;

    /** Time when last Login Timeout check occurred */
    protected long loginTimeoutCheckTimestamp = System.currentTimeMillis();

    /** Backup original Socket timeout to be restored after handshake */
    protected int socketTimeoutBackup = 0;

    /**
     * Called by the driver after issuing the MySQL protocol handshake and
     * reading the results of the handshake.
     * 
     * @throws SocketException
     *             if a socket error occurs
     * @throws IOException
     *             if an I/O error occurs
     * 
     * @return The socket to use after the handshake
     */
    public Socket afterHandshake() throws SocketException, IOException {
        resetLoginTimeCountdown();
        this.rawSocket.setSoTimeout(this.socketTimeoutBackup);
        return this.rawSocket;
    }

    /**
     * Called by the driver before issuing the MySQL protocol handshake. Should
     * return the socket instance that should be used during the handshake.
     * 
     * @throws SocketException
     *             if a socket error occurs
     * @throws IOException
     *             if an I/O error occurs
     * 
     * @return the socket to use before the handshake
     */
    public Socket beforeHandshake() throws SocketException, IOException {
        resetLoginTimeCountdown();
        this.socketTimeoutBackup = this.rawSocket.getSoTimeout();
        this.rawSocket.setSoTimeout(getRealTimeout(this.socketTimeoutBackup));
        return this.rawSocket;
    }

    /**
     * Create the raw socket.
     *
     * @param props
     *            properties available to affect behavior during socket creation.
     */
    protected Socket createSocket(Properties props) {
        return new Socket();
    }

    /**
     * Configures socket properties based on properties from the connection
     * (tcpNoDelay, snd/rcv buf, traffic class, etc).
     * 
     * @param props
     * @throws SocketException
     * @throws IOException
     */
    private void configureSocket(Socket sock, Properties props) throws SocketException, IOException {
        sock.setTcpNoDelay(Boolean.valueOf(props.getProperty(PropertyDefinitions.PNAME_tcpNoDelay, TCP_NO_DELAY_DEFAULT_VALUE)).booleanValue());

        String keepAlive = props.getProperty(PropertyDefinitions.PNAME_tcpKeepAlive, TCP_KEEP_ALIVE_DEFAULT_VALUE);

        if (keepAlive != null && keepAlive.length() > 0) {
            sock.setKeepAlive(Boolean.valueOf(keepAlive).booleanValue());
        }

        int receiveBufferSize = Integer.parseInt(props.getProperty(PropertyDefinitions.PNAME_tcpRcvBuf, TCP_RCV_BUF_DEFAULT_VALUE));

        if (receiveBufferSize > 0) {
            sock.setReceiveBufferSize(receiveBufferSize);
        }

        int sendBufferSize = Integer.parseInt(props.getProperty(PropertyDefinitions.PNAME_tcpSndBuf, TCP_SND_BUF_DEFAULT_VALUE));

        if (sendBufferSize > 0) {
            sock.setSendBufferSize(sendBufferSize);
        }

        int trafficClass = Integer.parseInt(props.getProperty(PropertyDefinitions.PNAME_tcpTrafficClass, TCP_TRAFFIC_CLASS_DEFAULT_VALUE));

        if (trafficClass > 0) {
            sock.setTrafficClass(trafficClass);
        }
    }

    public Socket connect(String hostname, int portNumber, Properties props, int loginTimeout) throws SocketException, IOException {

        this.loginTimeoutCountdown = loginTimeout;

        if (props != null) {
            this.host = hostname;

            this.port = portNumber;

            String localSocketHostname = props.getProperty(PropertyDefinitions.PNAME_localSocketAddress);
            InetSocketAddress localSockAddr = null;
            if (localSocketHostname != null && localSocketHostname.length() > 0) {
                localSockAddr = new InetSocketAddress(InetAddress.getByName(localSocketHostname), 0);
            }

            String connectTimeoutStr = props.getProperty(PropertyDefinitions.PNAME_connectTimeout);

            int connectTimeout = 0;

            if (connectTimeoutStr != null) {
                try {
                    connectTimeout = Integer.parseInt(connectTimeoutStr);
                } catch (NumberFormatException nfe) {
                    throw new SocketException("Illegal value '" + connectTimeoutStr + "' for connectTimeout");
                }
            }

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
                        this.rawSocket = createSocket(props);

                        configureSocket(this.rawSocket, props);

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

                return this.rawSocket;
            }
        }

        throw new SocketException("Unable to create socket");
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
