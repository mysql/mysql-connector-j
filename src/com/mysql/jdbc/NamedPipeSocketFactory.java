/*
 Copyright (c) 2002, 2011, Oracle and/or its affiliates. All rights reserved.
 

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
package com.mysql.jdbc;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.net.Socket;
import java.net.SocketException;
import java.sql.SQLException;
import java.util.Properties;

/**
 * A socket factory for named pipes (on Windows)
 * 
 * @author Mark Matthews
 */
public class NamedPipeSocketFactory implements SocketFactory, SocketMetadata {
	/**
	 * A socket that encapsulates named pipes on Windows
	 */
	class NamedPipeSocket extends Socket {
		private boolean isClosed = false;

		private RandomAccessFile namedPipeFile;

		NamedPipeSocket(String filePath) throws IOException {
			if ((filePath == null) || (filePath.length() == 0)) {
				throw new IOException(Messages
						.getString("NamedPipeSocketFactory.4")); //$NON-NLS-1$
			}

			this.namedPipeFile = new RandomAccessFile(filePath, "rw"); //$NON-NLS-1$
		}

		/**
		 * @see java.net.Socket#close()
		 */
		public synchronized void close() throws IOException {
			this.namedPipeFile.close();
			this.isClosed = true;
		}

		/**
		 * @see java.net.Socket#getInputStream()
		 */
		public InputStream getInputStream() throws IOException {
			return new RandomAccessFileInputStream(this.namedPipeFile);
		}

		/**
		 * @see java.net.Socket#getOutputStream()
		 */
		public OutputStream getOutputStream() throws IOException {
			return new RandomAccessFileOutputStream(this.namedPipeFile);
		}

		/**
		 * @see java.net.Socket#isClosed()
		 */
		public boolean isClosed() {
			return this.isClosed;
		}
	}

	/**
	 * Enables OutputStream-type functionality for a RandomAccessFile
	 */
	class RandomAccessFileInputStream extends InputStream {
		RandomAccessFile raFile;

		RandomAccessFileInputStream(RandomAccessFile file) {
			this.raFile = file;
		}

		/**
		 * @see java.io.InputStream#available()
		 */
		public int available() throws IOException {
			return -1;
		}

		/**
		 * @see java.io.InputStream#close()
		 */
		public void close() throws IOException {
			this.raFile.close();
		}

		/**
		 * @see java.io.InputStream#read()
		 */
		public int read() throws IOException {
			return this.raFile.read();
		}

		/**
		 * @see java.io.InputStream#read(byte[])
		 */
		public int read(byte[] b) throws IOException {
			return this.raFile.read(b);
		}

		/**
		 * @see java.io.InputStream#read(byte[], int, int)
		 */
		public int read(byte[] b, int off, int len) throws IOException {
			return this.raFile.read(b, off, len);
		}
	}

	/**
	 * Enables OutputStream-type functionality for a RandomAccessFile
	 */
	class RandomAccessFileOutputStream extends OutputStream {
		RandomAccessFile raFile;

		RandomAccessFileOutputStream(RandomAccessFile file) {
			this.raFile = file;
		}

		/**
		 * @see java.io.OutputStream#close()
		 */
		public void close() throws IOException {
			this.raFile.close();
		}

		/**
		 * @see java.io.OutputStream#write(byte[])
		 */
		public void write(byte[] b) throws IOException {
			this.raFile.write(b);
		}

		/**
		 * @see java.io.OutputStream#write(byte[], int, int)
		 */
		public void write(byte[] b, int off, int len) throws IOException {
			this.raFile.write(b, off, len);
		}

		/**
		 * @see java.io.OutputStream#write(int)
		 */
		public void write(int b) throws IOException {
		}
	}

	public static final String NAMED_PIPE_PROP_NAME = "namedPipePath"; //$NON-NLS-1$

	private Socket namedPipeSocket;

	/**
	 * Constructor for NamedPipeSocketFactory.
	 */
	public NamedPipeSocketFactory() {
		super();
	}

	/**
	 * @see com.mysql.jdbc.SocketFactory#afterHandshake()
	 */
	public Socket afterHandshake() throws SocketException, IOException {
		return this.namedPipeSocket;
	}

	/**
	 * @see com.mysql.jdbc.SocketFactory#beforeHandshake()
	 */
	public Socket beforeHandshake() throws SocketException, IOException {
		return this.namedPipeSocket;
	}

	/**
	 * @see com.mysql.jdbc.SocketFactory#connect(String, Properties)
	 */
	public Socket connect(String host, int portNumber /* ignored */,
			Properties props) throws SocketException, IOException {
		String namedPipePath = props.getProperty(NAMED_PIPE_PROP_NAME);

		if (namedPipePath == null) {
			namedPipePath = "\\\\.\\pipe\\MySQL"; //$NON-NLS-1$
		} else if (namedPipePath.length() == 0) {
			throw new SocketException(Messages
					.getString("NamedPipeSocketFactory.2") //$NON-NLS-1$
					+ NAMED_PIPE_PROP_NAME
					+ Messages.getString("NamedPipeSocketFactory.3")); //$NON-NLS-1$
		}

		this.namedPipeSocket = new NamedPipeSocket(namedPipePath);

		return this.namedPipeSocket;
	}

	public boolean isLocallyConnected(ConnectionImpl conn) throws SQLException {
		// Until I learn otherwise (or learn how to detect it), I assume that we are
		return true;
	}
}
