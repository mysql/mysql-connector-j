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
 * The representation (mapping) in the JavaTM programming language 
 * of an SQL BLOB value. An SQL BLOB is a built-in type that stores 
 * a Binary Large Object as a column value in a row of a database 
 * table. The driver implements Blob using an SQL locator(BLOB), 
 * which means that a Blob object contains a logical pointer to the 
 * SQL BLOB data rather than the data itself. A Blob object is valid 
 * for the duration of the transaction in which is was created. 
 * 
 * Methods in the interfaces ResultSet, CallableStatement, and 
 * PreparedStatement, such as getBlob and setBlob allow a programmer 
 * to access an SQL BLOB value. The Blob interface provides methods 
 * for getting the length of an SQL BLOB (Binary Large Object) value, 
 * for materializing a BLOB value on the client, and for determining 
 * the position of a pattern of bytes within a BLOB value. 
 * 
 * This class is new in the JDBC 2.0 API. 
 */
public class Blob
    implements java.sql.Blob
{

    //~ Instance/static variables .............................................

    //
    // This is a real brain-dead implementation of BLOB. Once I add
    // streamability to the I/O for MySQL this will be more efficiently
    // implemented (except for the position() method, ugh).
    //

    /** The binary data that makes up this BLOB */
    byte[] binaryData = null;

    //~ Constructors ..........................................................

    Blob(byte[] data)
    {
        binaryData = data;
    }

    //~ Methods ...............................................................

    /**
     * @see Blob#setBinaryStream(long)
     */
    public OutputStream setBinaryStream(long arg0)
                                 throws SQLException
    {
        throw new NotImplemented();
    }

    /**
   * Retrieves the BLOB designated by this Blob instance
   * as a stream.
   */
    public java.io.InputStream getBinaryStream()
                                        throws SQLException
    {

        return new ByteArrayInputStream(binaryData);
    }

    /**
     * @see Blob#setBytes(long, byte[], int, int)
     */
    public int setBytes(long arg0, byte[] arg1, int arg2, int arg3)
                 throws SQLException
    {
        throw new NotImplemented();
    }

    /**
     * @see Blob#setBytes(long, byte[])
     */
    public int setBytes(long arg0, byte[] arg1)
                 throws SQLException
    {
        throw new NotImplemented();
    }

    /**
   * Returns as an array of bytes, part or all of the BLOB
   * value that this Blob object designates.
   */
    public byte[] getBytes(long pos, int length)
                    throws SQLException
    {

        byte[] newData = new byte[length];
        System.arraycopy(binaryData, (int)(pos - 1), newData, 0, length);

        return newData;
    }

    /**
   * Returns the number of bytes in the BLOB value designated
   * by this Blob object.
   */
    public long length()
                throws SQLException
    {

        return binaryData.length;
    }

    public long position(java.sql.Blob pattern, long start)
                  throws SQLException
    {

        return position(pattern.getBytes(0, (int)pattern.length()), start);
    }

    public long position(byte[] pattern, long start)
                  throws SQLException
    {
        throw new SQLException("Not implemented");
    }

    /**
     * @see Blob#truncate(long)
     */
    public void truncate(long arg0)
                  throws SQLException
    {
        throw new NotImplemented();
    }
}