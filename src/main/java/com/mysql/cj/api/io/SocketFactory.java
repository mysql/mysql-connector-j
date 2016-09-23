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

package com.mysql.cj.api.io;

import java.io.IOException;
import java.net.Socket;
import java.net.SocketException;
import java.util.Properties;

/**
 * Interface to allow pluggable socket creation in the driver
 */
public interface SocketFactory extends SocketMetadata {

    /**
     * Called by the driver after issuing the MySQL protocol handshake and
     * reading the results of the handshake.
     * 
     * @throws SocketException
     *             if a socket error occurs
     * @throws IOException
     *             if an I/O error occurs
     * 
     * @return the socket to use after the handshake
     */
    Socket afterHandshake() throws SocketException, IOException;

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
    Socket beforeHandshake() throws SocketException, IOException;

    /**
     * Creates a new socket using the given properties. Properties are parsed by
     * the driver from the URL. All properties other than sensitive ones (user
     * and password) are passed to this method. The driver will instantiate the
     * socket factory with the class name given in the property
     * &quot;socketFactory&quot;, where the standard is <code>com.mysql.cj.core.io.StandardSocketFactory</code> Implementing classes
     * are responsible for handling synchronization of this method (if needed).
     * 
     * @param host
     *            the hostname passed in the URL. It will be a single
     *            hostname, as the driver parses multi-hosts (for failover) and
     *            calls this method for each host connection attempt.
     * 
     * @param portNumber
     *            the port number to connect to (if required).
     * 
     * @param props
     *            properties passed to the driver via the URL and/or properties
     *            instance.
     * 
     * @return a socket connected to the given host
     * @throws SocketException
     *             if a socket error occurs
     * @throws IOException
     *             if an I/O error occurs
     */
    Socket connect(String host, int portNumber, Properties props, int loginTimeout) throws SocketException, IOException;
}
