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


//import javax.net.SSLSocketFactory;


/**
 * Socket factory that creates SSL sockets.
 */
public class SSLSocketFactory
    extends StandardSocketFactory {

    //~ Methods ...............................................................

    /**
     * @see com.mysql.jdbc.SocketFactory#beforeHandshake()
     */
    public Socket beforeHandshake()
                           throws SocketException, IOException {

        javax.net.ssl.SSLSocketFactory sslFact = (javax.net.ssl.SSLSocketFactory) javax.net.ssl.SSLSocketFactory.getDefault();

        return sslFact.createSocket(rawSocket, host, port, true);
    }
}