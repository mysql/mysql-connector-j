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
import java.io.IOException;

import com.mysql.cj.conf.PropertySet;
import com.mysql.cj.log.Log;

/**
 * Interface to allow pluggable socket creation in the driver
 */
public interface SocketFactory extends SocketMetadata {

    /**
     * Creates a new socket or channel using the given properties. Properties are parsed by
     * the driver from the URL. All properties other than sensitive ones (user
     * and password) are passed to this method. The driver will instantiate the
     * socket factory with the class name given in the property
     * &quot;socketFactory&quot;, where the standard is <code>com.mysql.cj.protocol.StandardSocketFactory</code> Implementing classes
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
     * @param loginTimeout
     *            login timeout in milliseconds
     * @param <T>
     *            result type
     *
     * @return a socket connected to the given host
     * @throws IOException
     *             if an I/O error occurs
     */
    <T extends Closeable> T connect(String host, int portNumber, PropertySet props, int loginTimeout) throws IOException;

    /**
     * Called by the driver before issuing the MySQL protocol handshake.
     *
     * @throws IOException
     *             if an I/O error occurs
     */
    default void beforeHandshake() throws IOException {
    }

    /**
     * If required, called by the driver during MySQL protocol handshake to transform
     * original socket to SSL socket and perform TLS handshake.
     *
     * @param socketConnection
     *            current SocketConnection
     * @param serverSession
     *            current ServerSession
     * @param <T>
     *            result type
     * @return SSL socket
     * @throws IOException
     *             if an I/O error occurs
     */
    <T extends Closeable> T performTlsHandshake(SocketConnection socketConnection, ServerSession serverSession) throws IOException;

    /**
     * If required, called by the driver during MySQL protocol handshake to transform
     * original socket to SSL socket and perform TLS handshake.
     *
     * @param socketConnection
     *            current SocketConnection
     * @param serverSession
     *            current ServerSession
     * @param <T>
     *            result type
     * @param log
     *            logger
     * @return SSL socket
     * @throws IOException
     *             if an I/O error occurs
     */
    default <T extends Closeable> T performTlsHandshake(SocketConnection socketConnection, ServerSession serverSession, Log log) throws IOException {
        return performTlsHandshake(socketConnection, serverSession);
    }

    /**
     * Called by the driver after completing the MySQL protocol handshake and
     * reading the results of the authentication.
     *
     * @throws IOException
     *             if an I/O error occurs
     */
    default void afterHandshake() throws IOException {
    }

}
