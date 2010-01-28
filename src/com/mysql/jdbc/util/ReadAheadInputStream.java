/*
 Copyright  2002-2005 MySQL AB, 2008 Sun Microsystems
 All rights reserved. Use is subject to license terms.

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

package com.mysql.jdbc.util;

import java.io.IOException;
import java.io.InputStream;

import com.mysql.jdbc.log.Log;

/**
 * A non-blocking buffered input stream. Reads more if it can, won't block to
 * fill the buffer, only blocks to satisfy a request of read(byte[])
 * 
 * @author Mark Matthews
 * 
 * @version $Id: ReadAheadInputStream.java,v 1.1.2.1 2005/05/13 18:58:39
 *          mmatthews Exp $
 */
public class ReadAheadInputStream extends InputStream {

	private final static int DEFAULT_BUFFER_SIZE = 4096;

	private InputStream underlyingStream;

	private byte buf[];

	protected int endOfCurrentData;

	protected int currentPosition;

	protected boolean doDebug = false;
	
	protected Log log;

	private void fill(int readAtLeastTheseManyBytes) throws IOException {
		checkClosed();

		this.currentPosition = 0; /* no mark: throw away the buffer */

		this.endOfCurrentData = currentPosition;

		// Read at least as many bytes as the caller wants, but don't
		// block to fill the whole buffer (like java.io.BufferdInputStream
		// does)

		int bytesToRead = Math.min(this.buf.length - currentPosition,
				readAtLeastTheseManyBytes);

		int bytesAvailable = this.underlyingStream.available();

		if (bytesAvailable > bytesToRead) {

			// Great, there's more available, let's grab those
			// bytes too! (read-ahead)

			bytesToRead = Math.min(this.buf.length - currentPosition,
					bytesAvailable);
		}

		if (this.doDebug) {
			StringBuffer debugBuf = new StringBuffer();
			debugBuf.append("  ReadAheadInputStream.fill(");
			debugBuf.append(readAtLeastTheseManyBytes);
			debugBuf.append("), buffer_size=");
			debugBuf.append(this.buf.length);
			debugBuf.append(", current_position=");
			debugBuf.append(currentPosition);
			debugBuf.append(", need to read ");
			debugBuf.append(Math.min(this.buf.length - currentPosition,
					readAtLeastTheseManyBytes));
			debugBuf.append(" bytes to fill request,");

			if (bytesAvailable > 0) {
				debugBuf.append(" underlying InputStream reports ");
				debugBuf.append(bytesAvailable);

				debugBuf.append(" total bytes available,");
			}

			debugBuf.append(" attempting to read ");
			debugBuf.append(bytesToRead);
			debugBuf.append(" bytes.");

			if (this.log != null) {
				this.log.logTrace(debugBuf.toString());
			} else {
				System.err.println(debugBuf.toString());
			}
		}

		int n = this.underlyingStream.read(this.buf, currentPosition,
				bytesToRead);

		if (n > 0) {
			endOfCurrentData = n + currentPosition;
		}
	}

	private int readFromUnderlyingStreamIfNecessary(byte[] b, int off, int len)
			throws IOException {
		checkClosed();

		int avail = endOfCurrentData - currentPosition;

		if (this.doDebug) {
			StringBuffer debugBuf = new StringBuffer();
			debugBuf.append("ReadAheadInputStream.readIfNecessary(");
			debugBuf.append(b);
			debugBuf.append(",");
			debugBuf.append(off);
			debugBuf.append(",");
			debugBuf.append(len);
			debugBuf.append(")");

			if (avail <= 0) {
				debugBuf
						.append(" not all data available in buffer, must read from stream");

				if (len >= this.buf.length) {
					debugBuf
							.append(", amount requested > buffer, returning direct read() from stream");
				}
			}

			if (this.log != null) {
				this.log.logTrace(debugBuf.toString());
			} else {
				System.err.println(debugBuf.toString());
			}
		}

		if (avail <= 0) {

			if (len >= this.buf.length) {
				return this.underlyingStream.read(b, off, len);
			}

			fill(len);

			avail = endOfCurrentData - currentPosition;

			if (avail <= 0)
				return -1;
		}

		int bytesActuallyRead = (avail < len) ? avail : len;

		System.arraycopy(this.buf, currentPosition, b, off, bytesActuallyRead);

		this.currentPosition += bytesActuallyRead;

		return bytesActuallyRead;
	}

	public synchronized int read(byte b[], int off, int len) throws IOException {
		checkClosed(); // Check for closed stream
		if ((off | len | (off + len) | (b.length - (off + len))) < 0) {
			throw new IndexOutOfBoundsException();
		} else if (len == 0) {
			return 0;
		}

		int totalBytesRead = 0;

		while (true) {
			int bytesReadThisRound = readFromUnderlyingStreamIfNecessary(b, off
					+ totalBytesRead, len - totalBytesRead);

			// end-of-stream?
			if (bytesReadThisRound <= 0) {
				if (totalBytesRead == 0) {
					totalBytesRead = bytesReadThisRound;
				}

				break;
			}

			totalBytesRead += bytesReadThisRound;

			// Read _at_least_ enough bytes
			if (totalBytesRead >= len) {
				break;
			}

			// Nothing to read?
			if (this.underlyingStream.available() <= 0) {
				break;
			}
		}

		return totalBytesRead;
	}

	public int read() throws IOException {
		checkClosed();

		if (currentPosition >= endOfCurrentData) {
			fill(1);
			if (currentPosition >= endOfCurrentData)
				return -1;
		}

		return this.buf[currentPosition++] & 0xff;
	}

	public int available() throws IOException {
		checkClosed();

		return this.underlyingStream.available()
				+ (this.endOfCurrentData - this.currentPosition);
	}

	private void checkClosed() throws IOException {

		if (this.buf == null) {
			throw new IOException("Stream closed");
		}
	}

	/**
	 * 
	 */
	public ReadAheadInputStream(InputStream toBuffer, boolean debug, Log logTo) {
		this(toBuffer, DEFAULT_BUFFER_SIZE, debug, logTo);
	}

	public ReadAheadInputStream(InputStream toBuffer, int bufferSize,
			boolean debug,
			Log logTo) {
		this.underlyingStream = toBuffer;
		this.buf = new byte[bufferSize];
		this.doDebug = debug;
		this.log = logTo;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.io.Closeable#close()
	 */
	public void close() throws IOException {
		if (this.underlyingStream != null) {
			try {
				this.underlyingStream.close();
			} finally {
				this.underlyingStream = null;
				this.buf = null;
				this.log = null;
			}
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.io.InputStream#markSupported()
	 */
	public boolean markSupported() {
		return false;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.io.InputStream#skip(long)
	 */
	public long skip(long n) throws IOException {
		checkClosed();
		if (n <= 0) {
			return 0;
		}

		long bytesAvailInBuffer = this.endOfCurrentData - this.currentPosition;

		if (bytesAvailInBuffer <= 0) {

			fill((int) n);
			bytesAvailInBuffer = this.endOfCurrentData - this.currentPosition;
			if (bytesAvailInBuffer <= 0)
				return 0;
		}

		long bytesSkipped = (bytesAvailInBuffer < n) ? bytesAvailInBuffer : n;
		this.currentPosition += bytesSkipped;
		return bytesSkipped;
	}
}
