/*
  Copyright (c) 2008, 2014, Oracle and/or its affiliates. All rights reserved.

  The MySQL Connector/J is licensed under the terms of the GPLv2
  <http://www.gnu.org/licenses/old-licenses/gpl-2.0.html>, like most MySQL Connectors.
  There are special exceptions to the terms and conditions of the GPLv2 as it is applied to
  this software, see the FLOSS License Exception
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

package testsuite;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.nio.channels.SocketChannel;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import com.mysql.jdbc.NonRegisteringDriver;
import com.mysql.jdbc.SocketFactory;
import com.mysql.jdbc.StandardSocketFactory;

/**
 * Configure "socketFactory" to use this class in your JDBC URL, and it will operate
 * as normal, unless you map some host aliases to actual IP addresses, and then have
 * the test driver call hangOnConnect/Read/Write() which simulate the given failure
 * condition for the host with the <b>alias</b> argument, and will honor connect or 
 * socket timeout properties.
 * 
 * You can also cause a host to be immediately-downed by calling downHost() with an alias.
 */
public class UnreliableSocketFactory extends StandardSocketFactory {
	public static final long DEFAULT_TIMEOUT_MILLIS = 10 * 60 * 1000; // ugh

	private static final Map<String, String> MAPPED_HOSTS = new HashMap<String, String>();
	
	static final Set<String> HUNG_READ_HOSTS = new HashSet<String>();
	
	static final Set<String> HUNG_WRITE_HOSTS = new HashSet<String>();
	
	static final Set<String> HUNG_CONNECT_HOSTS = new HashSet<String>();
	
	static final Set<String> IMMEDIATELY_DOWNED_HOSTS = new HashSet<String>();
	
	private String hostname;
	private int portNumber;
	private Properties props;
	
	public static void flushAllHostLists(){
		IMMEDIATELY_DOWNED_HOSTS.clear();
		HUNG_CONNECT_HOSTS.clear();
		HUNG_READ_HOSTS.clear();
		HUNG_WRITE_HOSTS.clear();
	}
	
	
	public static void mapHost(String alias, String orig) {
		MAPPED_HOSTS.put(alias, orig);
	}
	
	public static void hangOnRead(String hostname) {
		HUNG_READ_HOSTS.add(hostname);
	}
	
	public static void dontHangOnRead(String hostname) {
		HUNG_READ_HOSTS.remove(hostname);
	}
	
	public static void hangOnWrite(String hostname) {
		HUNG_WRITE_HOSTS.add(hostname);
	}
	
	public static void dontHangOnWrite (String hostname) {
		HUNG_WRITE_HOSTS.remove(hostname);
	}
	
	public static void hangOnConnect(String hostname) {
		HUNG_CONNECT_HOSTS.add(hostname);
	}
	
	public static void dontHangOnConnect(String hostname) {
		HUNG_CONNECT_HOSTS.remove(hostname);
	}
	
	public static void downHost(String hostname) {
		IMMEDIATELY_DOWNED_HOSTS.add(hostname);
		
	}
	
	public static void dontDownHost(String hostname) {
		IMMEDIATELY_DOWNED_HOSTS.remove(hostname);
	}
	
	public Socket connect(String host_name, int port_number, Properties prop)
			throws SocketException, IOException {
		this.hostname = host_name;
		this.portNumber = port_number;
		this.props = prop;
		return getNewSocket();
	}
	
	private Socket getNewSocket() throws SocketException, IOException {
		if (IMMEDIATELY_DOWNED_HOSTS.contains(hostname)) {

			sleepMillisForProperty(props, "connectTimeout");

			throw new SocketTimeoutException();
		}
		
		String hostnameToConnectTo = MAPPED_HOSTS.get(hostname);
		
		if (hostnameToConnectTo == null) {
			hostnameToConnectTo = hostname;
		}
		
		if (NonRegisteringDriver.isHostPropertiesList(hostnameToConnectTo)) {
			Properties hostSpecificProps = NonRegisteringDriver.expandHostKeyValues(hostnameToConnectTo);
			
			String protocol = hostSpecificProps.getProperty(NonRegisteringDriver.PROTOCOL_PROPERTY_KEY);
			
			if ("unix".equalsIgnoreCase(protocol)) {
				SocketFactory factory;
				try {
					factory = (SocketFactory) Class
							.forName(
									"org.newsclub.net.mysql.AFUNIXDatabaseSocketFactory")
							.newInstance();
				} catch (InstantiationException e) {
					throw new SocketException(e.getMessage());
				} catch (IllegalAccessException e) {
					throw new SocketException(e.getMessage());
				} catch (ClassNotFoundException e) {
					throw new SocketException(e.getMessage());
				}

				String path = hostSpecificProps
						.getProperty(NonRegisteringDriver.PATH_PROPERTY_KEY);

				if (path != null) {
					hostSpecificProps.setProperty("junixsocket.file", path);
				}

				return new HangingSocket(factory.connect(hostnameToConnectTo,
						portNumber, hostSpecificProps), props, hostname);
			}

		}
		
		return new HangingSocket(super.connect(hostnameToConnectTo, portNumber, props), props, hostname);
	}
	
	

	public Socket afterHandshake() throws SocketException, IOException {
		return getNewSocket();
	}

	public Socket beforeHandshake() throws SocketException, IOException {
		return getNewSocket();
	}

	static void sleepMillisForProperty(Properties props, String name) {
		try {
			Thread.sleep(Long.parseLong(props.getProperty(name, String
					.valueOf(DEFAULT_TIMEOUT_MILLIS))));
		} catch (NumberFormatException e) {
			throw new RuntimeException(e);
		} catch (InterruptedException e) {
			// ignore
		}
	}

	class HangingSocket extends Socket {
		public void bind(SocketAddress bindpoint) throws IOException {

			underlyingSocket.bind(bindpoint);
		}

		public synchronized void close() throws IOException {

			underlyingSocket.close();
		}

		public SocketChannel getChannel() {

			return underlyingSocket.getChannel();
		}

		public InetAddress getInetAddress() {

			return underlyingSocket.getInetAddress();
		}

		public InputStream getInputStream() throws IOException {

			return new HangingInputStream(underlyingSocket.getInputStream(), props, aliasedHostname);
		}

		public boolean getKeepAlive() throws SocketException {

			return underlyingSocket.getKeepAlive();
		}

		public InetAddress getLocalAddress() {

			return underlyingSocket.getLocalAddress();
		}

		public int getLocalPort() {

			return underlyingSocket.getLocalPort();
		}

		public SocketAddress getLocalSocketAddress() {

			return underlyingSocket.getLocalSocketAddress();
		}

		public boolean getOOBInline() throws SocketException {

			return underlyingSocket.getOOBInline();
		}

		public OutputStream getOutputStream() throws IOException {
			return new HangingOutputStream(underlyingSocket.getOutputStream(), props, aliasedHostname);
		}

		public int getPort() {

			return underlyingSocket.getPort();
		}

		public synchronized int getReceiveBufferSize() throws SocketException {

			return underlyingSocket.getReceiveBufferSize();
		}

		public SocketAddress getRemoteSocketAddress() {

			return underlyingSocket.getRemoteSocketAddress();
		}

		public boolean getReuseAddress() throws SocketException {

			return underlyingSocket.getReuseAddress();
		}

		public synchronized int getSendBufferSize() throws SocketException {

			return underlyingSocket.getSendBufferSize();
		}

		public int getSoLinger() throws SocketException {

			return underlyingSocket.getSoLinger();
		}

		public synchronized int getSoTimeout() throws SocketException {

			return underlyingSocket.getSoTimeout();
		}

		public boolean getTcpNoDelay() throws SocketException {
			return underlyingSocket.getTcpNoDelay();
		}

		public int getTrafficClass() throws SocketException {
			return underlyingSocket.getTrafficClass();
		}

		public boolean isBound() {
			return underlyingSocket.isBound();
		}

		public boolean isClosed() {
			return underlyingSocket.isClosed();
		}

		public boolean isConnected() {
			return underlyingSocket.isConnected();
		}

		public boolean isInputShutdown() {
			return underlyingSocket.isInputShutdown();
		}

		public boolean isOutputShutdown() {
			return underlyingSocket.isOutputShutdown();
		}

		public void sendUrgentData(int data) throws IOException {
			underlyingSocket.sendUrgentData(data);
		}

		public void setKeepAlive(boolean on) throws SocketException {
			underlyingSocket.setKeepAlive(on);
		}

		public void setOOBInline(boolean on) throws SocketException {
			underlyingSocket.setOOBInline(on);
		}

		public synchronized void setReceiveBufferSize(int size)
				throws SocketException {
			underlyingSocket.setReceiveBufferSize(size);
		}

		public void setReuseAddress(boolean on) throws SocketException {
			underlyingSocket.setReuseAddress(on);
		}

		public synchronized void setSendBufferSize(int size)
				throws SocketException {
			underlyingSocket.setSendBufferSize(size);
		}

		public void setSoLinger(boolean on, int linger) throws SocketException {
			underlyingSocket.setSoLinger(on, linger);
		}

		public synchronized void setSoTimeout(int timeout)
				throws SocketException {
			underlyingSocket.setSoTimeout(timeout);
		}

		public void setTcpNoDelay(boolean on) throws SocketException {
			underlyingSocket.setTcpNoDelay(on);
		}

		public void setTrafficClass(int tc) throws SocketException {
			underlyingSocket.setTrafficClass(tc);
		}

		public void shutdownInput() throws IOException {
			underlyingSocket.shutdownInput();
		}

		public void shutdownOutput() throws IOException {
			underlyingSocket.shutdownOutput();
		}

		public String toString() {
			return underlyingSocket.toString();
		}

		final Socket underlyingSocket;
		final Properties props;
		final String aliasedHostname;
		
		HangingSocket(Socket realSocket, Properties props, String aliasedHostname) {
			underlyingSocket = realSocket;
			this.props = props;
			this.aliasedHostname = aliasedHostname;
		}

	}

	static class HangingInputStream extends InputStream {
		final InputStream underlyingInputStream;
		final Properties props;
		final String aliasedHostname;
		
		HangingInputStream(InputStream realInputStream, Properties props, String aliasedHostname) {
			underlyingInputStream = realInputStream;
			this.props = props;
			this.aliasedHostname = aliasedHostname;
		}

		public int available() throws IOException {
			return underlyingInputStream.available();
		}

		public void close() throws IOException {
			underlyingInputStream.close();
		}

		public synchronized void mark(int readlimit) {
			underlyingInputStream.mark(readlimit);
		}

		public boolean markSupported() {
			return underlyingInputStream.markSupported();
		}

		public int read(byte[] b, int off, int len) throws IOException {
			failIfRequired();

			return underlyingInputStream.read(b, off, len);
		}

		public int read(byte[] b) throws IOException {
			failIfRequired();

			return underlyingInputStream.read(b);
		}

		public synchronized void reset() throws IOException {
			underlyingInputStream.reset();
		}

		public long skip(long n) throws IOException {
			failIfRequired();

			return underlyingInputStream.skip(n);
		}

		private void failIfRequired() throws SocketTimeoutException {
			if (HUNG_READ_HOSTS.contains(aliasedHostname) || IMMEDIATELY_DOWNED_HOSTS.contains(aliasedHostname)) {
				sleepMillisForProperty(props, "socketTimeout");

				throw new SocketTimeoutException();
			}
		}

		public int read() throws IOException {
			failIfRequired();

			return underlyingInputStream.read();
		}
	}
	
	static class HangingOutputStream extends OutputStream {

			final Properties props;
			final String aliasedHostname;
			final OutputStream underlyingOutputStream;
			
			HangingOutputStream(OutputStream realOutputStream, Properties props, String aliasedHostname) {
				underlyingOutputStream = realOutputStream;
				this.props = props;
				this.aliasedHostname = aliasedHostname;
			}

			public void close() throws IOException {
				failIfRequired();
				underlyingOutputStream.close();
			}

			public void flush() throws IOException {
				underlyingOutputStream.flush();
			}

			public void write(byte[] b, int off, int len) throws IOException {
				failIfRequired();
				underlyingOutputStream.write(b, off, len);
			}

			public void write(byte[] b) throws IOException {
				failIfRequired();
				underlyingOutputStream.write(b);
			}

			public void write(int b) throws IOException {
				failIfRequired();
				underlyingOutputStream.write(b);
			}
			
			private void failIfRequired() throws SocketTimeoutException {
				if (HUNG_WRITE_HOSTS.contains(aliasedHostname) || IMMEDIATELY_DOWNED_HOSTS.contains(aliasedHostname)) {
					sleepMillisForProperty(props, "socketTimeout");

					throw new SocketTimeoutException();
				}
			}
		
	}
}
