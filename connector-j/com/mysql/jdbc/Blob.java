/*
   Copyright (C) 2002 MySQL AB
   
      This program is free software; you can redistribute it and/or modify
      it under the terms of the GNU General Public License as published by
      the Free Software Foundation; either version 2 of the License, or
      (at your option) any later version.
   
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
import java.io.OutputStream;
import java.sql.SQLException;


/**
 * The representation (mapping) in the JavaTM programming language  of an SQL
 * BLOB value. An SQL BLOB is a built-in type that stores  a Binary Large
 * Object as a column value in a row of a database  table. The driver
 * implements Blob using an SQL locator(BLOB),  which means that a Blob object
 * contains a logical pointer to the  SQL BLOB data rather than the data
 * itself. A Blob object is valid  for the duration of the transaction in
 * which is was created.   Methods in the interfaces ResultSet,
 * CallableStatement, and  PreparedStatement, such as getBlob and setBlob
 * allow a programmer  to access an SQL BLOB value. The Blob interface
 * provides methods  for getting the length of an SQL BLOB (Binary Large
 * Object) value,  for materializing a BLOB value on the client, and for
 * determining  the position of a pattern of bytes within a BLOB value.   This
 * class is new in the JDBC 2.0 API.
 * 
 * @author Mark Matthews
 */
public class Blob
    implements java.sql.Blob, OutputStreamWatcher {

    //~ Instance/static variables .............................................

    //
    // This is a real brain-dead implementation of BLOB. Once I add
    // streamability to the I/O for MySQL this will be more efficiently
    // implemented (except for the position() method, ugh).
    //

    /** The binary data that makes up this BLOB */
    private byte[] binaryData = null;
    
    /** The ResultSet that created this BLOB */
    
    private ResultSet creatorResultSet;
    
    /** The column that this BLOB came from */
    
    private int columnIndex;

    //~ Constructors ..........................................................

    /**
     * Creates a BLOB encapsulating the given binary data
     */
    Blob(byte[] data) {
        setBinaryData(data);
        this.creatorResultSet = null;
        this.columnIndex = 0;
    }
    
    /**
     * Creates an updatable BLOB that can update in-place 
     * (not implemented yet).
     */
    Blob(byte[] data, ResultSet creatorResultSet, int columnIndex) {
        setBinaryData(data);
        this.creatorResultSet = creatorResultSet;
        this.columnIndex = columnIndex;
    }

    //~ Methods ...............................................................

    /**
     * @see Blob#setBinaryStream(long)
     */
    public OutputStream setBinaryStream(long indexToWriteAt)
                                 throws SQLException {
        if (indexToWriteAt < 1) {
            throw new SQLException("indexToWriteAt must be >= 1", "S1009");
        }
        
        WatchableOutputStream bytesOut = new WatchableOutputStream();
        bytesOut.setWatcher(this);
        
        if (indexToWriteAt > 0) {
            bytesOut.write(this.binaryData, 0, (int) (indexToWriteAt - 1));
        }
        
        return bytesOut;
    }

    /**
     * Retrieves the BLOB designated by this Blob instance as a stream.
     * 
     * @return this BLOB represented as a binary stream of bytes.
     * 
     * @throws SQLException if a database error occurs
     */
    public java.io.InputStream getBinaryStream()
                                        throws SQLException {

        return new ByteArrayInputStream(getBinaryData());
    }

    /**
     * @see Blob#setBytes(long, byte[], int, int)
     */
    public int setBytes(long arg0, byte[] arg1, int arg2, int arg3)
                 throws SQLException {
        throw new NotImplemented();
    }

    /**
     * @see Blob#setBytes(long, byte[])
     */
    public int setBytes(long arg0, byte[] arg1)
                 throws SQLException {
        throw new NotImplemented();
    }

    /**
     * Returns as an array of bytes, part or all of the BLOB value that this
     * Blob object designates.
     * 
     * @param pos where to start the part of the BLOB
     * @param length the length of the part of the BLOB you want returned.
     * 
     * @return the bytes stored in the blob starting at position
     *         <code>pos</code> and having a length of <code>length</code>.
     * 
     * @throws SQLException if a database error occurs
     */
    public byte[] getBytes(long pos, int length)
                    throws SQLException {
        if (pos < 1) {
            throw new SQLException("Position 'pos' can not be < 1", "S1009");
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
     * @throws SQLException if a database error occurs
     */
    public long length()
                throws SQLException {

        return getBinaryData().length;
    }

    /**
     * Finds the position of the given pattern in this BLOB.
     * 
     * @param pattern the pattern to find
     * @param start where to start finding the pattern
     * 
     * @return the position where the pattern is found in the BLOB, -1 if not
     *         found
     * 
     * @throws SQLException if a database error occurs
     */
    public long position(java.sql.Blob pattern, long start)
                  throws SQLException {

        return position(pattern.getBytes(0, (int) pattern.length()), start);
    }

    /**
     * DOCUMENT ME!
     * 
     * @param pattern DOCUMENT ME!
     * @param start DOCUMENT ME!
     * 
     * @return DOCUMENT ME!
     * 
     * @throws SQLException DOCUMENT ME!
     */
    public long position(byte[] pattern, long start)
                  throws SQLException {
        throw new SQLException("Not implemented");
    }

    /**
     * @see Blob#truncate(long)
     */
    public void truncate(long arg0)
                  throws SQLException {
        throw new NotImplemented();
    }

    private void setBinaryData(byte[] binaryData) {
        this.binaryData = binaryData;
    }

    private byte[] getBinaryData() {

        return binaryData;
    }
    
	/**
	 * @see com.mysql.jdbc.OutputStreamWatcher#streamClosed(byte[])
	 */
	public void streamClosed(byte[] byteData) {
        this.binaryData = byteData;
	}

}