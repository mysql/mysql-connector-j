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
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;
import java.io.StringReader;
import java.io.Writer;

import java.sql.SQLException;


/**
 * Simplistic implementation of java.sql.Clob for MySQL Connector/J
 * 
 * @version $Id$
 * @author Mark Matthews
 */
public class Clob
    implements java.sql.Clob, OutputStreamWatcher, WriterWatcher {

    //~ Instance/static variables .............................................

    private String charData;

    //~ Constructors ..........................................................

    Clob(String charData) {
        this.charData = charData;
    }

    //~ Methods ...............................................................

    /**
     * @see java.sql.Clob#setAsciiStream(long)
     */
    public OutputStream setAsciiStream(long indexToWriteAt)
                                throws SQLException {
        
        if (indexToWriteAt < 1) {
            throw new SQLException("indexToWriteAt must be >= 1", "S1009");
        }
        
        WatchableOutputStream bytesOut = new WatchableOutputStream();
        bytesOut.setWatcher(this);
        
        if (indexToWriteAt > 0) {
            bytesOut.write(this.charData.getBytes(), 0, (int) (indexToWriteAt - 1));
        }
        
        return bytesOut;
    }

    /**
     * @see java.sql.Clob#getAsciiStream()
     */
    public InputStream getAsciiStream()
                               throws SQLException {

        if (this.charData != null) {

            return new ByteArrayInputStream(this.charData.getBytes());
        } else {

            return null;
        }
    }

    /**
     * @see java.sql.Clob#setCharacterStream(long)
     */
    public Writer setCharacterStream(long indexToWriteAt)
                              throws SQLException {
        if (indexToWriteAt < 1) {
            throw new SQLException("indexToWriteAt must be >= 1", "S1009");
        }
        
        WatchableWriter writer = new WatchableWriter();
        writer.setWatcher(this);
        
        if (indexToWriteAt > 0) {
            writer.write(this.charData, 0, (int) (indexToWriteAt - 1));
        }
        
        return writer;
    }

    /**
     * @see java.sql.Clob#getCharacterStream()
     */
    public Reader getCharacterStream()
                              throws SQLException {

        if (this.charData != null) {

            return new StringReader(this.charData);
        } else {

            return null;
        }
    }

    /**
     * @see java.sql.Clob#setString(long, String)
     */
    public int setString(long pos, String str)
                  throws SQLException {
        
        if (pos < 1) {
            throw new SQLException("Starting position can not be < 1", "S1009");
        }
        
        if (str == null) {
            throw new SQLException("String to set can not be NULL", "S1009");
        }
       
        
        StringBuffer charBuf = new StringBuffer(this.charData);
        
        pos--;
        
        int strLength = str.length();
        
        charBuf.replace((int) pos, (int) (pos +  strLength), str);
        
        this.charData = charBuf.toString();
        
        return strLength;
    }

    /**
     * @see java.sql.Clob#setString(long, String, int, int)
     */
    public int setString(long pos, String str, int offset, int len)
                  throws SQLException {
                    
        if (pos < 1) {
            throw new SQLException("Starting position can not be < 1", "S1009");
        }
        
        if (str == null) {
            throw new SQLException("String to set can not be NULL", "S1009");
        }
       
        
        StringBuffer charBuf = new StringBuffer(this.charData);
        
        pos--;
        
        String replaceString = str.substring(offset, len);
        
        charBuf.replace((int) pos, (int) (pos +  replaceString.length()), replaceString);
        
        this.charData = charBuf.toString();
        
        return len;
    }

    /**
     * @see java.sql.Clob#getSubString(long, int)
     */
    public String getSubString(long startPos, int length)
                        throws SQLException {

        if (startPos < 1) {
            throw new SQLException("CLOB start position can not be < 1", "S1009");
        }
        
        if (this.charData != null) {

            
            if ((startPos - 1) + length > charData.length()) {
                throw new SQLException("CLOB start position + length can not be > length of CLOB", "S1009");
            }
                
            return this.charData.substring((int) (startPos - 1), length);
        } else {

            return null;
        }
    }

    /**
     * @see java.sql.Clob#length()
     */
    public long length()
                throws SQLException {

        if (this.charData != null) {

            return this.charData.length();
        } else {

            return 0;
        }
    }

    /**
     * @see java.sql.Clob#position(String, long)
     */
    public long position(String stringToFind, long startPos)
                  throws SQLException {

        if (startPos < 1) {
                throw new SQLException("Illegal starting position for search, '" + startPos + "'", "S1009");
        }
            
        if (this.charData != null) {
            if ((startPos - 1) > this.charData.length()) {
                throw new SQLException("Starting position for search is past end of CLOB", "S1009");
            }
            
            int pos = this.charData.indexOf(stringToFind, (int) (startPos - 1));
            
            return (pos == -1) ? -1 : pos + 1;
        } else {

            return -1;
        }
    }

    /**
     * @see java.sql.Clob#position(Clob, long)
     */
    public long position(java.sql.Clob arg0, long arg1)
                  throws SQLException {

        return position(arg0.getSubString(0L, (int) arg0.length()), arg1);
    }

    /**
     * @see java.sql.Clob#truncate(long)
     */
    public void truncate(long length)
                  throws SQLException {
        this.charData = this.charData.substring((int) length);
    }
	/**
	 * @see com.mysql.jdbc.OutputStreamWatcher#streamClosed(byte[])
	 */
	public void streamClosed(byte[] byteData) {
        this.charData = StringUtils.toAsciiString(byteData);
	}

	/**
	 * @see com.mysql.jdbc.WriterWatcher#writerClosed(char[])
	 */
	public void writerClosed(char[] charData) {
        this.charData = new String(charData);
	}

}