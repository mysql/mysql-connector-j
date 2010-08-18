/*
 Copyright (c) 2002, 2010, Oracle and/or its affiliates. All rights reserved.
 

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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.sql.SQLException;

/**
 * The representation (mapping) in the JavaTM programming language of an SQL
 * BLOB value. An SQL BLOB is a built-in type that stores a Binary Large Object
 * as a column value in a row of a database table. The driver implements Blob
 * using an SQL locator(BLOB), which means that a Blob object contains a logical
 * pointer to the SQL BLOB data rather than the data itself. A Blob object is
 * valid for the duration of the transaction in which is was created. Methods in
 * the interfaces ResultSet, CallableStatement, and PreparedStatement, such as
 * getBlob and setBlob allow a programmer to access an SQL BLOB value. The Blob
 * interface provides methods for getting the length of an SQL BLOB (Binary
 * Large Object) value, for materializing a BLOB value on the client, and for
 * determining the position of a pattern of bytes within a BLOB value. This
 * class is new in the JDBC 2.0 API.
 * 
 * @author Mark Matthews
 * @version $Id$
 */
public class Blob implements java.sql.Blob, OutputStreamWatcher {

	//
	// This is a real brain-dead implementation of BLOB. Once I add
	// streamability to the I/O for MySQL this will be more efficiently
	// implemented (except for the position() method, ugh).
	//

	/** The binary data that makes up this BLOB */
	private byte[] binaryData = null;
	private boolean isClosed = false;
	private ExceptionInterceptor exceptionInterceptor;
	
	/**
     * Creates a Blob without data
     */
    Blob(ExceptionInterceptor exceptionInterceptor) {
        setBinaryData(Constants.EMPTY_BYTE_ARRAY);
        this.exceptionInterceptor = exceptionInterceptor;
    }
    
	/**
	 * Creates a BLOB encapsulating the given binary data
	 * 
	 * @param data
	 *            DOCUMENT ME!
	 */
	Blob(byte[] data, ExceptionInterceptor exceptionInterceptor) {
		setBinaryData(data);
		this.exceptionInterceptor = exceptionInterceptor;
	}

	/**
	 * Creates an updatable BLOB that can update in-place (not implemented yet).
	 * 
	 * @param data
	 *            DOCUMENT ME!
	 * @param creatorResultSetToSet
	 *            DOCUMENT ME!
	 * @param columnIndexToSet
	 *            DOCUMENT ME!
	 */
	Blob(byte[] data, ResultSetInternalMethods creatorResultSetToSet, int columnIndexToSet) {
		setBinaryData(data);
	}

	private synchronized byte[] getBinaryData() {
		return this.binaryData;
	}

	/**
	 * Retrieves the BLOB designated by this Blob instance as a stream.
	 * 
	 * @return this BLOB represented as a binary stream of bytes.
	 * 
	 * @throws SQLException
	 *             if a database error occurs
	 */
	public synchronized java.io.InputStream getBinaryStream() throws SQLException {
		checkClosed();
		
		return new ByteArrayInputStream(getBinaryData());
	}

	/**
	 * Returns as an array of bytes, part or all of the BLOB value that this
	 * Blob object designates.
	 * 
	 * @param pos
	 *            where to start the part of the BLOB
	 * @param length
	 *            the length of the part of the BLOB you want returned.
	 * 
	 * @return the bytes stored in the blob starting at position
	 *         <code>pos</code> and having a length of <code>length</code>.
	 * 
	 * @throws SQLException
	 *             if a database error occurs
	 */
	public synchronized byte[] getBytes(long pos, int length) throws SQLException {
		checkClosed();
		
		if (pos < 1) {
			throw SQLError.createSQLException(Messages.getString("Blob.2"), //$NON-NLS-1$
					SQLError.SQL_STATE_ILLEGAL_ARGUMENT, this.exceptionInterceptor);
		}

		pos--;
		
		if (pos > this.binaryData.length) {
			throw SQLError.createSQLException("\"pos\" argument can not be larger than the BLOB's length.", 
					SQLError.SQL_STATE_ILLEGAL_ARGUMENT, this.exceptionInterceptor);
		}
		
		if (pos + length > this.binaryData.length) {
			throw SQLError.createSQLException("\"pos\" + \"length\" arguments can not be larger than the BLOB's length.", 
					SQLError.SQL_STATE_ILLEGAL_ARGUMENT, this.exceptionInterceptor);
		}
		
		byte[] newData = new byte[length];
		System.arraycopy(getBinaryData(), (int) (pos), newData, 0, length);

		return newData;
	}

	/**
	 * Returns the number of bytes in the BLOB value designated by this Blob
	 * object.
	 * 
	 * @return the length of this blob
	 * 
	 * @throws SQLException
	 *             if a database error occurs
	 */
	public synchronized long length() throws SQLException {
		checkClosed();
		
		return getBinaryData().length;
	}

	/**
	 * @see java.sql.Blob#position(byte[], long)
	 */
	public synchronized long position(byte[] pattern, long start) throws SQLException {
		throw SQLError.createSQLException("Not implemented", this.exceptionInterceptor); //$NON-NLS-1$
	}

	/**
	 * Finds the position of the given pattern in this BLOB.
	 * 
	 * @param pattern
	 *            the pattern to find
	 * @param start
	 *            where to start finding the pattern
	 * 
	 * @return the position where the pattern is found in the BLOB, -1 if not
	 *         found
	 * 
	 * @throws SQLException
	 *             if a database error occurs
	 */
	public synchronized long position(java.sql.Blob pattern, long start) throws SQLException {
		checkClosed();
		
		return position(pattern.getBytes(0, (int) pattern.length()), start);
	}

	private synchronized void setBinaryData(byte[] newBinaryData) {
		this.binaryData = newBinaryData;
	}

	/**
	 * @see Blob#setBinaryStream(long)
	 */
	public synchronized OutputStream setBinaryStream(long indexToWriteAt)
			throws SQLException {
		checkClosed();
		
		if (indexToWriteAt < 1) {
			throw SQLError.createSQLException(Messages.getString("Blob.0"), //$NON-NLS-1$
					SQLError.SQL_STATE_ILLEGAL_ARGUMENT, this.exceptionInterceptor);
		}

		WatchableOutputStream bytesOut = new WatchableOutputStream();
		bytesOut.setWatcher(this);

		if (indexToWriteAt > 0) {
			bytesOut.write(this.binaryData, 0, (int) (indexToWriteAt - 1));
		}

		return bytesOut;
	}

	/**
	 * @see Blob#setBytes(long, byte[])
	 */
	public synchronized int setBytes(long writeAt, byte[] bytes) throws SQLException {
		checkClosed();
		
		return setBytes(writeAt, bytes, 0, bytes.length);
	}

	/**
	 * @see Blob#setBytes(long, byte[], int, int)
	 */
	public synchronized int setBytes(long writeAt, byte[] bytes, int offset, int length)
			throws SQLException {
		checkClosed();
		
		OutputStream bytesOut = setBinaryStream(writeAt);

		try {
			bytesOut.write(bytes, offset, length);
		} catch (IOException ioEx) {
			SQLException sqlEx = SQLError.createSQLException(Messages.getString("Blob.1"), //$NON-NLS-1$
					SQLError.SQL_STATE_GENERAL_ERROR, this.exceptionInterceptor);
			sqlEx.initCause(ioEx);
			
			throw sqlEx;
		} finally {
			try {
				bytesOut.close();
			} catch (IOException doNothing) {
				; // do nothing
			}
		}

		return length;
	}

	/**
	 * @see com.mysql.jdbc.OutputStreamWatcher#streamClosed(byte[])
	 */
	public synchronized void streamClosed(byte[] byteData) {
		this.binaryData = byteData;
	}

	/**
	 * @see com.mysql.jdbc.OutputStreamWatcher#streamClosed(byte[])
	 */
	public synchronized void streamClosed(WatchableOutputStream out) {
		int streamSize = out.size();

		if (streamSize < this.binaryData.length) {
			out.write(this.binaryData, streamSize, this.binaryData.length
					- streamSize);
		}

		this.binaryData = out.toByteArray();
	}

	/**
     * Truncates the <code>BLOB</code> value that this <code>Blob</code>
     * object represents to be <code>len</code> bytes in length.
     * <p>
     * <b>Note:</b> If the value specified for <code>len</code>
     * is greater then the length+1 of the <code>BLOB</code> value then the 
     * behavior is undefined. Some JDBC drivers may throw a 
     * <code>SQLException</code> while other drivers may support this 
     * operation.
     *
     * @param len the length, in bytes, to which the <code>BLOB</code> value
     *        that this <code>Blob</code> object represents should be truncated
     * @exception SQLException if there is an error accessing the
     *            <code>BLOB</code> value or if len is less than 0
     * @exception SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @since 1.4
     */
	public synchronized void truncate(long len) throws SQLException {
		checkClosed();
		
		if (len < 0) {
			throw SQLError.createSQLException("\"len\" argument can not be < 1.", 
					SQLError.SQL_STATE_ILLEGAL_ARGUMENT, this.exceptionInterceptor);
		}
		
		if (len > this.binaryData.length) {
			throw SQLError.createSQLException("\"len\" argument can not be larger than the BLOB's length.", 
					SQLError.SQL_STATE_ILLEGAL_ARGUMENT, this.exceptionInterceptor);
		}
		
		// TODO: Do this without copying byte[]s by maintaining some end pointer
		// on the original data
		
		byte[] newData = new byte[(int)len];
		System.arraycopy(getBinaryData(), 0, newData, 0, (int)len);
		this.binaryData = newData;
	}

	/**
     * This method frees the <code>Blob</code> object and releases the resources that 
     * it holds. The object is invalid once the <code>free</code>
     * method is called.
     *<p>
     * After <code>free</code> has been called, any attempt to invoke a
     * method other than <code>free</code> will result in a <code>SQLException</code> 
     * being thrown.  If <code>free</code> is called multiple times, the subsequent
     * calls to <code>free</code> are treated as a no-op.
     *<p>
     * 
     * @throws SQLException if an error occurs releasing
     * the Blob's resources
     * @exception SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @since 1.6
     */
	
	public synchronized void free() throws SQLException {
		this.binaryData = null;
		this.isClosed = true;
	}

	/**
     * Returns an <code>InputStream</code> object that contains a partial <code>Blob</code> value, 
     * starting  with the byte specified by pos, which is length bytes in length.
     *
     * @param pos the offset to the first byte of the partial value to be retrieved.
     *  The first byte in the <code>Blob</code> is at position 1
     * @param length the length in bytes of the partial value to be retrieved
     * @return <code>InputStream</code> through which the partial <code>Blob</code> value can be read.
     * @throws SQLException if pos is less than 1 or if pos is greater than the number of bytes
     * in the <code>Blob</code> or if pos + length is greater than the number of bytes 
     * in the <code>Blob</code>
     *
     * @exception SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @since 1.6
     */
	public synchronized InputStream getBinaryStream(long pos, long length) throws SQLException {
		checkClosed();
		
		if (pos < 1) {
			throw SQLError.createSQLException("\"pos\" argument can not be < 1.", 
					SQLError.SQL_STATE_ILLEGAL_ARGUMENT, this.exceptionInterceptor);
		}
		
		pos--;
		
		if (pos > this.binaryData.length) {
			throw SQLError.createSQLException("\"pos\" argument can not be larger than the BLOB's length.", 
					SQLError.SQL_STATE_ILLEGAL_ARGUMENT, this.exceptionInterceptor);
		}
		
		if (pos + length > this.binaryData.length) {
			throw SQLError.createSQLException("\"pos\" + \"length\" arguments can not be larger than the BLOB's length.", 
					SQLError.SQL_STATE_ILLEGAL_ARGUMENT, this.exceptionInterceptor);
		}
		
		return new ByteArrayInputStream(getBinaryData(), (int)pos, (int)length);
	}
	
	private synchronized void checkClosed() throws SQLException {
		if (this.isClosed) {
			throw SQLError.createSQLException("Invalid operation on closed BLOB", 
					SQLError.SQL_STATE_ILLEGAL_ARGUMENT, this.exceptionInterceptor);
		}
	}
}
