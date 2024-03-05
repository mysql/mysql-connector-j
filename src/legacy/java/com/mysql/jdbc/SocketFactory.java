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

package com.mysql.jdbc;

import java.io.IOException;
import java.net.Socket;
import java.net.SocketException;
import java.util.Properties;

/**
 * Interface to allow pluggable socket creation in the driver
 *
 * @deprecated Use {@link com.mysql.cj.protocol.SocketFactory} instead.
 */
@Deprecated
public interface SocketFactory {

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
     * &quot;socketFactory&quot;, where the standard is <code>com.mysql.jdbc.StandardSocketFactory</code> Implementing classes
     * are responsible for handling synchronization of this method (if needed).
     *
     * @param host
     *            the hostname passed in the JDBC URL. It will be a single
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
    Socket connect(String host, int portNumber, Properties props) throws SocketException, IOException;

}
