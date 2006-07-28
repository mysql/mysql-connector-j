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

import java.io.ByteArrayInputStream;
import java.io.IOException;
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

	/**
	 * Creates a BLOB encapsulating the given binary data
	 * 
	 * @param data
	 *            DOCUMENT ME!
	 */
	Blob(byte[] data) {
		setBinaryData(data);
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
	Blob(byte[] data, ResultSet creatorResultSetToSet, int columnIndexToSet) {
		setBinaryData(data);
	}

	private byte[] getBinaryData() {
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
	public java.io.InputStream getBinaryStream() throws SQLException {
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
	public byte[] getBytes(long pos, int length) throws SQLException {
		if (pos < 1) {
			throw SQLError.createSQLException(Messages.getString("Blob.2"), //$NON-NLS-1$
					SQLError.SQL_STATE_ILLEGAL_ARGUMENT);
		}

		byte[] newData = new byte[length];
		System.arraycopy(getBinaryData(), (int) (pos - 1), newData, 0, length);

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
	public long length() throws SQLException {
		return getBinaryData().length;
	}

	/**
	 * @see java.sql.Blob#position(byte[], long)
	 */
	public long position(byte[] pattern, long start) throws SQLException {
		throw SQLError.createSQLException("Not implemented"); //$NON-NLS-1$
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
	public long position(java.sql.Blob pattern, long start) throws SQLException {
		return position(pattern.getBytes(0, (int) pattern.length()), start);
	}

	private void setBinaryData(byte[] newBinaryData) {
		this.binaryData = newBinaryData;
	}

	/**
	 * @see Blob#setBinaryStream(long)
	 */
	public OutputStream setBinaryStream(long indexToWriteAt)
			throws SQLException {
		if (indexToWriteAt < 1) {
			throw SQLError.createSQLException(Messages.getString("Blob.0"), //$NON-NLS-1$
					SQLError.SQL_STATE_ILLEGAL_ARGUMENT);
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
	public int setBytes(long writeAt, byte[] bytes) throws SQLException {
		return setBytes(writeAt, bytes, 0, bytes.length);
	}

	/**
	 * @see Blob#setBytes(long, byte[], int, int)
	 */
	public int setBytes(long writeAt, byte[] bytes, int offset, int length)
			throws SQLException {
		OutputStream bytesOut = setBinaryStream(writeAt);

		try {
			bytesOut.write(bytes, offset, length);
		} catch (IOException ioEx) {
			throw SQLError.createSQLException(Messages.getString("Blob.1"), //$NON-NLS-1$
					SQLError.SQL_STATE_GENERAL_ERROR);
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
	public void streamClosed(byte[] byteData) {
		this.binaryData = byteData;
	}

	/**
	 * @see com.mysql.jdbc.OutputStreamWatcher#streamClosed(byte[])
	 */
	public void streamClosed(WatchableOutputStream out) {
		int streamSize = out.size();

		if (streamSize < this.binaryData.length) {
			out.write(this.binaryData, streamSize, this.binaryData.length
					- streamSize);
		}

		this.binaryData = out.toByteArray();
	}

	/**
	 * @see Blob#truncate(long)
	 */
	public void truncate(long arg0) throws SQLException {
		throw new NotImplemented();
	}
}
