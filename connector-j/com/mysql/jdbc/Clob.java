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
    implements java.sql.Clob
{

    //~ Instance/static variables .............................................

    private String charData;

    //~ Constructors ..........................................................

    Clob(String charData)
    {
        this.charData = charData;
    }

    //~ Methods ...............................................................

    /**
     * @see java.sql.Clob#setAsciiStream(long)
     */
    public OutputStream setAsciiStream(long arg0)
                                throws SQLException
    {
        throw new NotImplemented();
    }

    /**
     * @see java.sql.Clob#getAsciiStream()
     */
    public InputStream getAsciiStream()
                               throws SQLException
    {

        if (this.charData != null) {

            return new ByteArrayInputStream(this.charData.getBytes());
        } else {

            return null;
        }
    }

    /**
     * @see java.sql.Clob#setCharacterStream(long)
     */
    public Writer setCharacterStream(long arg0)
                              throws SQLException
    {
        throw new NotImplemented();
    }

    /**
     * @see java.sql.Clob#getCharacterStream()
     */
    public Reader getCharacterStream()
                              throws SQLException
    {

        if (this.charData != null) {

            return new StringReader(this.charData);
        } else {

            return null;
        }
    }

    /**
     * @see java.sql.Clob#setString(long, String)
     */
    public int setString(long arg0, String arg1)
                  throws SQLException
    {
        throw new NotImplemented();
    }

    /**
     * @see java.sql.Clob#setString(long, String, int, int)
     */
    public int setString(long arg0, String arg1, int arg2, int arg3)
                  throws SQLException
    {
        throw new NotImplemented();
    }

    /**
     * @see java.sql.Clob#getSubString(long, int)
     */
    public String getSubString(long arg0, int arg1)
                        throws SQLException
    {

        if (this.charData != null) {

            return this.charData.substring((int)arg0, arg1);
        } else {

            return null;
        }
    }

    /**
     * @see java.sql.Clob#length()
     */
    public long length()
                throws SQLException
    {

        if (this.charData != null) {

            return this.charData.length();
        } else {

            return 0;
        }
    }

    /**
     * @see java.sql.Clob#position(String, long)
     */
    public long position(String arg0, long arg1)
                  throws SQLException
    {

        if (this.charData != null) {

            return this.charData.indexOf(arg0, (int)arg1);
        } else {

            return -1;
        }
    }

    /**
     * @see java.sql.Clob#position(Clob, long)
     */
    public long position(java.sql.Clob arg0, long arg1)
                  throws SQLException
    {

        return position(arg0.getSubString(0L, (int)arg0.length()), arg1);
    }

    /**
     * @see java.sql.Clob#truncate(long)
     */
    public void truncate(long arg0)
                  throws SQLException
    {
        throw new NotImplemented();
    }
}