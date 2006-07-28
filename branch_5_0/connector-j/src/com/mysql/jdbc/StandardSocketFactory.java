/*
 Copyright (C) 2002-2004 MySQL AB

 This program is free software; you can redistribute it and/or modify
 it under the terms of version 2 of the GNU General Public License as 
 published by the Free Software Foundation.

 There are special exceptions to the terms and conditions of the GPL 
 as it is applied to this software. View the full text of the 
 exception in file EXCEPTIONS-CONNECTOR-J in the directory of this 
 software distribution.

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

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import java.net.InetAddress;
import java.net.Socket;
import java.net.SocketException;

import java.util.Properties;

/**
 * Socket factory for vanilla TCP/IP sockets (the standard)
 * 
 * @author Mark Matthews
 */
public class StandardSocketFactory implements SocketFactory {
	// ~ Instance fields
	// --------------------------------------------------------

	/** The hostname to connect to */
	protected String host = null;

	/** The port number to connect to */
	protected int port = 3306;

	/** The underlying TCP/IP socket to use */
	protected Socket rawSocket = null;

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
	 * @return The socket to use after the handshake
	 */
	public Socket afterHandshake() throws SocketException, IOException {
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
		return this.rawSocket;
	}

	/**
	 * @see com.mysql.jdbc.SocketFactory#createSocket(Properties)
	 */
	public Socket connect(String hostname, int portNumber, Properties props)
			throws SocketException, IOException {

		if (props != null) {
			this.host = hostname;

			this.port = portNumber;

			boolean hasConnectTimeoutMethod = false;

			Method connectWithTimeoutMethod = null;

			try {
				// Have to do this with reflection, otherwise older JVMs croak
				Class socketAddressClass = Class
						.forName("java.net.SocketAddress");

				connectWithTimeoutMethod = Socket.class.getMethod("connect",
						new Class[] { socketAddressClass, Integer.TYPE });

				hasConnectTimeoutMethod = true;
			} catch (NoClassDefFoundError noClassDefFound) {
				hasConnectTimeoutMethod = false;
			} catch (NoSuchMethodException noSuchMethodEx) {
				hasConnectTimeoutMethod = false;
			} catch (Throwable catchAll) {
				hasConnectTimeoutMethod = false;
			}

			int connectTimeout = 0;

			String connectTimeoutStr = props.getProperty("connectTimeout");

			if (connectTimeoutStr != null) {
				try {
					connectTimeout = Integer.parseInt(connectTimeoutStr);
				} catch (NumberFormatException nfe) {
					throw new SocketException("Illegal value '"
							+ connectTimeoutStr + "' for connectTimeout");
				}
			}

			if (this.host != null) {
				if (!hasConnectTimeoutMethod || (connectTimeout == 0)) {
					InetAddress[] possibleAddresses = InetAddress
							.getAllByName(this.host);

					Exception caughtWhileConnecting = null;

					// Need to loop through all possible addresses, in case
					// someone has IPV6 configured (SuSE, for example...)

					for (int i = 0; i < possibleAddresses.length; i++) {
						try {
							rawSocket = new Socket(possibleAddresses[i], port);

							break;
						} catch (Exception ex) {
							caughtWhileConnecting = ex;
						}
					}

					if (rawSocket == null) {
						throw new SocketException(caughtWhileConnecting
								.toString());
					}
				} else {
					// must explicitly state this due to classloader issues
					// when running on older JVMs :(
					try {
						Class inetSocketAddressClass = Class
								.forName("java.net.InetSocketAddress");
						Constructor addrConstructor = inetSocketAddressClass
						.getConstructor(new Class[] { InetAddress.class,
								Integer.TYPE });

				InetAddress[] possibleAddresses = InetAddress
						.getAllByName(this.host);

				Exception caughtWhileConnecting = null;

				// Need to loop through all possible addresses, in case
				// someone has IPV6 configured (SuSE, for example...)

				for (int i = 0; i < possibleAddresses.length; i++) {
					
					try {
						Object sockAddr = addrConstructor
								.newInstance(new Object[] { possibleAddresses[i],
										new Integer(port) });
						
								rawSocket = new Socket();
								connectWithTimeoutMethod.invoke(rawSocket,
										new Object[] { sockAddr,
												new Integer(connectTimeout) });

								break;
							} catch (Exception ex) {
								rawSocket = null;

								caughtWhileConnecting = ex;
							}
						}

						if (rawSocket == null) {
							throw new SocketException(caughtWhileConnecting
									.toString());
						}

					} catch (Throwable t) {
						if (!(t instanceof SocketException)) {
							throw new SocketException(t.toString());
						}

						throw (SocketException) t;
					}
				}

				try {
					this.rawSocket.setTcpNoDelay(true);
				} catch (Exception ex) {
					/* Ignore */
					;
				}

				return this.rawSocket;
			}
		}

		throw new SocketException("Unable to create socket");
	}
}
