/*
   Copyright (C) 2002 MySQL AB
   
      This program is free software; you can redistribute it and/or modify
      it under the terms of the GNU General Public License as published by
      the Free Software Foundation; either version 2 of the License, or
      (at your option) any later version.
   
      This program is distributed in the hope that it will be useful,
      but WITHOUT ANY WARRANTY; without even the implied warranty of
      MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
      GNU General Public License for more details.
   
      You should have received a copy of the GNU General Public License
      along with this program; if not, write to the Free Software
      Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
      
 */
package com.mysql.jdbc;

import java.io.IOException;

import java.net.Socket;
import java.net.SocketException;

import java.util.Properties;


/**
 * Socket factory for vanilla TCP/IP sockets (the standard)
 */
public class StandardSocketFactory
    implements SocketFactory {

    //~ Instance/static variables .............................................

    protected Socket rawSocket = null;
    protected String host = null;
    protected int port = 3306;

    //~ Methods ...............................................................

    /**
     * @see com.mysql.jdbc.SocketFactory#createSocket(Properties)
     */
    public Socket connect(String host, Properties props)
                   throws SocketException, IOException {

        if (props != null) {
            this.host = host;

            String portStr = props.getProperty("PORT");

            if (portStr != null) {
                port = Integer.parseInt(portStr);
            }

            if (this.host != null) {
                rawSocket = new Socket(this.host, port);

                try {
                    rawSocket.setTcpNoDelay(true);
                } catch (Exception ex) {

                    /* Ignore */
                }

                return rawSocket;
            }
        }

        throw new SocketException("Unable to create socket");
    }

    /**
     * Called by the driver before issuing the MySQL protocol handshake.
     * Should return the socket instance that should be used during
     * the handshake.
     */
    public Socket beforeHandshake()
                           throws SocketException, IOException {

        return rawSocket;
    }

    /**
     * Called by the driver after issuing the MySQL protocol handshake and
     * reading the results of the handshake.
     */
    public Socket afterHandshake()
                          throws SocketException, IOException {

        return rawSocket;
    }
}