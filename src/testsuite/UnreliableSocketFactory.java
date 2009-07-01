/*
 Copyright  2008 MySQL AB, 2008 Sun Microsystems

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

	private static final Map MAPPED_HOSTS = new HashMap();
	
	static final Set HUNG_READ_HOSTS = new HashSet();
	
	static final Set HUNG_WRITE_HOSTS = new HashSet();
	
	static final Set HUNG_CONNECT_HOSTS = new HashSet();
	
	static final Set IMMEDIATELY_DOWNED_HOSTS = new HashSet();
	
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
	
	public Socket connect(String hostname, int portNumber, Properties props)
			throws SocketException, IOException {
		this.hostname = hostname;
		this.portNumber = portNumber;
		this.props = props;
		return getNewSocket();
	}
	
	private Socket getNewSocket() throws SocketException, IOException {
		if (IMMEDIATELY_DOWNED_HOSTS.contains(hostname)) {

			sleepMillisForProperty(props, "connectTimeout");

			throw new SocketTimeoutException();
		}
		
		String hostnameToConnectTo = (String) MAPPED_HOSTS.get(hostname);
		
		if (hostnameToConnectTo == null) {
			hostnameToConnectTo = hostname;
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
