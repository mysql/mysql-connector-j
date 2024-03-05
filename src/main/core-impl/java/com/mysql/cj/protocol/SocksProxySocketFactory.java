/*
 * Copyright (c) 2014, 2024, Oracle and/or its affiliates.
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
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Proxy;
import java.net.Socket;
import java.net.SocketException;

import com.mysql.cj.conf.PropertyKey;
import com.mysql.cj.conf.PropertySet;

/**
 * A socket factory used to create sockets connecting through a SOCKS proxy. The socket still supports all the same TCP features as the "standard" socket.
 */
public class SocksProxySocketFactory extends StandardSocketFactory {

    @Override
    protected Socket createSocket(PropertySet props) {
        String socksProxyHost = props.getStringProperty(PropertyKey.socksProxyHost).getValue();
        int socksProxyPort = props.getIntegerProperty(PropertyKey.socksProxyPort).getValue();
        return new Socket(new Proxy(Proxy.Type.SOCKS, new InetSocketAddress(socksProxyHost, socksProxyPort)));
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T extends Closeable> T connect(String hostname, int portNumber, PropertySet pset, int loginTimeout) throws IOException {
        if (!pset.getBooleanProperty(PropertyKey.socksProxyRemoteDns).getValue()) {
            // fall back to the parent connection procedure
            return super.connect(hostname, portNumber, pset, loginTimeout);
        }

        // proceed without local DNS resolution
        this.loginTimeoutCountdown = loginTimeout;

        if (pset != null && hostname != null) {
            this.host = hostname;
            this.port = portNumber;

            String localSocketHostname = pset.getStringProperty(PropertyKey.localSocketAddress).getValue();
            InetSocketAddress localSockAddr = localSocketHostname != null && localSocketHostname.length() > 0
                    ? new InetSocketAddress(InetAddress.getByName(localSocketHostname), 0)
                    : null;
            int connectTimeout = pset.getIntegerProperty(PropertyKey.connectTimeout).getValue();

            // save last exception to propagate to caller if connection fails
            try {
                this.rawSocket = createSocket(pset);
                configureSocket(this.rawSocket, pset);

                // bind to the local port if not using the ephemeral port
                if (localSockAddr != null) {
                    this.rawSocket.bind(localSockAddr);
                }

                this.rawSocket.connect(InetSocketAddress.createUnresolved(this.host, this.port), getRealTimeout(connectTimeout));

            } catch (SocketException ex) {
                this.rawSocket = null;
                throw ex;
            }

            resetLoginTimeCountdown();

            this.sslSocket = this.rawSocket;
            return (T) this.rawSocket;
        }

        throw new SocketException("Unable to create socket");
    }

}
