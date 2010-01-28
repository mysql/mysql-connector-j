/*
 Copyright  2002-2004 MySQL AB, 2008 Sun Microsystems
 All rights reserved. Use is subject to license terms.

  The MySQL Connector/J is licensed under the terms of the GPL,
  like most MySQL Connectors. There are special exceptions to the
  terms and conditions of the GPL as it is applied to this software,
  see the FLOSS License Exception available on mysql.com.

  This program is free software; you can redistribute it and/or
  modify it under the terms of the GNU General Public License as
  published by the Free Software Foundation; version 2 of the
  License.

  This program is distributed in the hope that it will be useful,  
  but WITHOUT ANY WARRANTY; without even the implied warranty of
  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
  GNU General Public License for more details.

  You should have received a copy of the GNU General Public License
  along with this program; if not, write to the Free Software
  Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
  02110-1301 USA



 */
package com.mysql.jdbc;

import java.io.IOException;
import java.net.Socket;
import java.net.SocketException;
import java.util.Properties;

/**
 * Interface to allow pluggable socket creation in the driver
 * 
 * @author Mark Matthews
 */
public interface SocketFactory {
	// ~ Methods
	// ----------------------------------------------------------------

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
	 * &quot;socketFactory&quot;, where the standard is
	 * <code>com.mysql.jdbc.StandardSocketFactory</code> Implementing classes
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
	Socket connect(String host, int portNumber, Properties props)
			throws SocketException, IOException;
}
