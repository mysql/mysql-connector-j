/*
 Copyright  2002-2007 MySQL AB, 2008 Sun Microsystems

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

	public static final String TCP_NO_DELAY_PROPERTY_NAME = "tcpNoDelay";

	public static final String TCP_KEEP_ALIVE_DEFAULT_VALUE = "true";

	public static final String TCP_KEEP_ALIVE_PROPERTY_NAME = "tcpKeepAlive";

	public static final String TCP_RCV_BUF_PROPERTY_NAME = "tcpRcvBuf";

	public static final String TCP_SND_BUF_PROPERTY_NAME = "tcpSndBuf";

	public static final String TCP_TRAFFIC_CLASS_PROPERTY_NAME = "tcpTrafficClass";

	public static final String TCP_RCV_BUF_DEFAULT_VALUE = "0";

	public static final String TCP_SND_BUF_DEFAULT_VALUE = "0";

	public static final String TCP_TRAFFIC_CLASS_DEFAULT_VALUE = "0";

	public static final String TCP_NO_DELAY_DEFAULT_VALUE = "true";

	/** Use reflection for pre-1.4 VMs */

	private static Method setTraficClassMethod;

	static {
		try {
			setTraficClassMethod = Socket.class.getMethod("setTrafficClass",
					new Class[] { Integer.TYPE });
		} catch (SecurityException e) {
			setTraficClassMethod = null;
		} catch (NoSuchMethodException e) {
			setTraficClassMethod = null;
		}
	}

	/** The hostname to connect to */
	protected String host = null;

	/** The port number to connect to */
	protected int port = 3306;

	/** The underlying TCP/IP socket to use */
	protected Socket rawSocket = null;

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
	 * Configures socket properties based on properties from the connection
	 * (tcpNoDelay, snd/rcv buf, traffic class, etc).
	 * 
	 * @param props
	 * @throws SocketException
	 * @throws IOException
	 */
	private void configureSocket(Socket sock, Properties props) throws SocketException,
			IOException {
		try {
			sock.setTcpNoDelay(Boolean.valueOf(
					props.getProperty(TCP_NO_DELAY_PROPERTY_NAME,
							TCP_NO_DELAY_DEFAULT_VALUE)).booleanValue());

			String keepAlive = props.getProperty(TCP_KEEP_ALIVE_PROPERTY_NAME,
					TCP_KEEP_ALIVE_DEFAULT_VALUE);

			if (keepAlive != null && keepAlive.length() > 0) {
				sock.setKeepAlive(Boolean.valueOf(keepAlive)
						.booleanValue());
			}

			int receiveBufferSize = Integer.parseInt(props.getProperty(
					TCP_RCV_BUF_PROPERTY_NAME, TCP_RCV_BUF_DEFAULT_VALUE));

			if (receiveBufferSize > 0) {
				sock.setReceiveBufferSize(receiveBufferSize);
			}

			int sendBufferSize = Integer.parseInt(props.getProperty(
					TCP_SND_BUF_PROPERTY_NAME, TCP_SND_BUF_DEFAULT_VALUE));

			if (sendBufferSize > 0) {
				sock.setSendBufferSize(sendBufferSize);
			}

			int trafficClass = Integer.parseInt(props.getProperty(
					TCP_TRAFFIC_CLASS_PROPERTY_NAME,
					TCP_TRAFFIC_CLASS_DEFAULT_VALUE));

			if (trafficClass > 0 && setTraficClassMethod != null) {
				setTraficClassMethod.invoke(sock,
						new Object[] { new Integer(trafficClass) });
			}
		} catch (Throwable t) {
			unwrapExceptionToProperClassAndThrowIt(t);
		}
	}

	/**
	 * @see com.mysql.jdbc.SocketFactory#createSocket(Properties)
	 */
	public Socket connect(String hostname, int portNumber, Properties props)
			throws SocketException, IOException {

		if (props != null) {
			this.host = hostname;

			this.port = portNumber;

			Method connectWithTimeoutMethod = null;
			Method socketBindMethod = null;
			Class socketAddressClass = null;

			String localSocketHostname = props
					.getProperty("localSocketAddress");

			String connectTimeoutStr = props.getProperty("connectTimeout");

			int connectTimeout = 0;

			boolean wantsTimeout = (connectTimeoutStr != null
					&& connectTimeoutStr.length() > 0 && !connectTimeoutStr
					.equals("0"));

			boolean wantsLocalBind = (localSocketHostname != null && localSocketHostname
					.length() > 0);

			boolean needsConfigurationBeforeConnect = socketNeedsConfigurationBeforeConnect(props);
			
			if (wantsTimeout || wantsLocalBind || needsConfigurationBeforeConnect) {

				if (connectTimeoutStr != null) {
					try {
						connectTimeout = Integer.parseInt(connectTimeoutStr);
					} catch (NumberFormatException nfe) {
						throw new SocketException("Illegal value '"
								+ connectTimeoutStr + "' for connectTimeout");
					}
				}

				try {
					// Have to do this with reflection, otherwise older JVMs
					// croak
					socketAddressClass = Class
							.forName("java.net.SocketAddress");

					connectWithTimeoutMethod = Socket.class.getMethod(
							"connect", new Class[] { socketAddressClass,
									Integer.TYPE });

					socketBindMethod = Socket.class.getMethod("bind",
							new Class[] { socketAddressClass });

				} catch (NoClassDefFoundError noClassDefFound) {
					// ignore, we give a better error below if needed
				} catch (NoSuchMethodException noSuchMethodEx) {
					// ignore, we give a better error below if needed
				} catch (Throwable catchAll) {
					// ignore, we give a better error below if needed
				}

				if (wantsLocalBind && socketBindMethod == null) {
					throw new SocketException(
							"Can't specify \"localSocketAddress\" on JVMs older than 1.4");
				}

				if (wantsTimeout && connectWithTimeoutMethod == null) {
					throw new SocketException(
							"Can't specify \"connectTimeout\" on JVMs older than 1.4");
				}
			}

			if (this.host != null) {
				if (!(wantsLocalBind || wantsTimeout || needsConfigurationBeforeConnect)) {
					InetAddress[] possibleAddresses = InetAddress
							.getAllByName(this.host);

					Throwable caughtWhileConnecting = null;

					// Need to loop through all possible addresses, in case
					// someone has IPV6 configured (SuSE, for example...)

					for (int i = 0; i < possibleAddresses.length; i++) {
						try {
							this.rawSocket = new Socket(possibleAddresses[i],
									port);

							configureSocket(this.rawSocket, props);

							break;
						} catch (Exception ex) {
							caughtWhileConnecting = ex;
						}
					}

					if (rawSocket == null) {
						unwrapExceptionToProperClassAndThrowIt(caughtWhileConnecting);
					}
				} else {
					// must explicitly state this due to classloader issues
					// when running on older JVMs :(
					try {

						InetAddress[] possibleAddresses = InetAddress
								.getAllByName(this.host);

						Throwable caughtWhileConnecting = null;

						Object localSockAddr = null;

						Class inetSocketAddressClass = null;

						Constructor addrConstructor = null;

						try {
							inetSocketAddressClass = Class
									.forName("java.net.InetSocketAddress");

							addrConstructor = inetSocketAddressClass
									.getConstructor(new Class[] {
											InetAddress.class, Integer.TYPE });

							if (wantsLocalBind) {
								localSockAddr = addrConstructor
										.newInstance(new Object[] {
												InetAddress
														.getByName(localSocketHostname),
												new Integer(0 /*
																 * use ephemeral
																 * port
																 */) });

							}
						} catch (Throwable ex) {
							unwrapExceptionToProperClassAndThrowIt(ex);
						}

						// Need to loop through all possible addresses, in case
						// someone has IPV6 configured (SuSE, for example...)

						for (int i = 0; i < possibleAddresses.length; i++) {

							try {
								this.rawSocket = new Socket();

								configureSocket(this.rawSocket, props);

								Object sockAddr = addrConstructor
										.newInstance(new Object[] {
												possibleAddresses[i],
												new Integer(port) });
								// bind to the local port, null is 'ok', it
								// means
								// use the ephemeral port
								socketBindMethod.invoke(rawSocket,
										new Object[] { localSockAddr });

								connectWithTimeoutMethod.invoke(rawSocket,
										new Object[] { sockAddr,
												new Integer(connectTimeout) });

								break;
							} catch (Exception ex) {	
								this.rawSocket = null;

								caughtWhileConnecting = ex;
							}
						}

						if (this.rawSocket == null) {
							unwrapExceptionToProperClassAndThrowIt(caughtWhileConnecting);
						}

					} catch (Throwable t) {
						unwrapExceptionToProperClassAndThrowIt(t);
					}
				}

				return this.rawSocket;
			}
		}

		throw new SocketException("Unable to create socket");
	}

	/**
	 * Does the configureSocket() need to be called before the socket is
	 * connect()d based on the properties supplied?
	 * 
	 */
	private boolean socketNeedsConfigurationBeforeConnect(Properties props) {
		int receiveBufferSize = Integer.parseInt(props.getProperty(
				TCP_RCV_BUF_PROPERTY_NAME, TCP_RCV_BUF_DEFAULT_VALUE));

		if (receiveBufferSize > 0) {
			return true;
		}

		int sendBufferSize = Integer.parseInt(props.getProperty(
				TCP_SND_BUF_PROPERTY_NAME, TCP_SND_BUF_DEFAULT_VALUE));

		if (sendBufferSize > 0) {
			return true;
		}

		int trafficClass = Integer.parseInt(props.getProperty(
				TCP_TRAFFIC_CLASS_PROPERTY_NAME,
				TCP_TRAFFIC_CLASS_DEFAULT_VALUE));

		if (trafficClass > 0 && setTraficClassMethod != null) {
			return true;
		}

		return false;
	}

	private void unwrapExceptionToProperClassAndThrowIt(
			Throwable caughtWhileConnecting) throws SocketException,
			IOException {
		if (caughtWhileConnecting instanceof InvocationTargetException) {

			// Replace it with the target, don't use 1.4 chaining as this still
			// needs to run on older VMs
			caughtWhileConnecting = ((InvocationTargetException) caughtWhileConnecting)
					.getTargetException();
		}

		if (caughtWhileConnecting instanceof SocketException) {
			throw (SocketException) caughtWhileConnecting;
		}

		if (caughtWhileConnecting instanceof IOException) {
			throw (IOException) caughtWhileConnecting;
		}

		throw new SocketException(caughtWhileConnecting.toString());
	}
}