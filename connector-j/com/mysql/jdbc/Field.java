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

/**
 * Field is a class used to describe fields in a
 * ResultSet
 * 
 * @author Mark Matthews <mmatthew@worldserver.com>
 * @version $Id$
 */
package com.mysql.jdbc;

import java.sql.*;

import java.util.*;


public class Field
{

    //~ Instance/static variables .............................................

    private final static int _AUTO_INCREMENT_FLAG = 512;
    int                      _colDecimals;
    short                    _colFlag;
    int                      _length; // Internal length of the field;
    int                      _mysqlType = -1; // the MySQL type
    String                   _name; // The Field name
    int                      _sqlType   = -1; // the java.sql.Type
    String                   _tableName; // The Name of the Table
    private String           _fullName;

    //~ Constructors ..........................................................

    /**
   * Constructor used by DatabaseMetaData methods.
   */
    public Field(String Table, String Name, int jdbc_type, int length)
    {
        this._tableName = Table;
        this._name      = Name;
        this._length    = length;
        _sqlType        = jdbc_type;
        _colFlag        = 0;
        _colDecimals    = 0;
    }

    Field(String Table, String Name, int length, int mysql_type, 
          short col_flag, int col_decimals)
    {
        this._tableName = Table;
        this._name      = Name;
        this._length    = length;
        _colFlag        = col_flag;
        _colDecimals    = col_decimals;
        this._mysqlType = mysql_type;

        // Map MySqlTypes to java.sql Types
        _sqlType = MysqlDefs.mysqlToJavaType(mysql_type);

        StringBuffer fullNameBuf = new StringBuffer(
                                           _tableName.length() + 1 + 
                                           _name.length());
        fullNameBuf.append(_tableName);
        fullNameBuf.append(".");
        fullNameBuf.append(_name);
        _fullName   = fullNameBuf.toString();
        fullNameBuf = null;

        boolean is_binary = isBinary();

        //
        // Handle TEXT type (special case), Fix proposed by Peter McKeown
        //
        if (_sqlType == java.sql.Types.LONGVARBINARY && !is_binary) {
            _sqlType = java.sql.Types.LONGVARCHAR;
        } else if (_sqlType == java.sql.Types.VARBINARY && !is_binary) {
            _sqlType = java.sql.Types.VARCHAR;
        }
    }

    //~ Methods ...............................................................

    public boolean isAutoIncrement()
    {

        if ((_colFlag & _AUTO_INCREMENT_FLAG) > 0) {

            return true;
        } else {

            return false;
        }
    }

    public boolean isBinary()
    {

        if ((_colFlag & 128) > 0) {

            return true;
        }
         else {

            return false;
        }
    }

    public boolean isBlob()
    {

        if ((_colFlag & 16) > 0) {

            return true;
        }
         else {

            return false;
        }
    }

    public String getFullName()
    {

        return _fullName;
    }

    public int getLength()
    {

        return _length;
    }

    public boolean isMultipleKey()
    {

        if ((_colFlag & 8) > 0) {

            return true;
        }
         else {

            return false;
        }
    }

    public int getMysqlType()
    {

        return _mysqlType;
    }

    public String getName()
    {

        if (_name != null) {

            return _name;
        }
         else {

            return null;
        }
    }

    public boolean isPrimaryKey()
    {

        if ((_colFlag & 2) > 0) {

            return true;
        }
         else {

            return false;
        }
    }

    public int getSQLType()
    {

        return _sqlType;
    }

    public String getTable()
    {

        if (_tableName != null) {

            return _tableName;
        }
         else {

            return null;
        }
    }

    public String getTableName()
    {

        return _tableName;
    }

    public boolean isUniqueKey()
    {

        if ((_colFlag & 4) > 0) {

            return true;
        }
         else {

            return false;
        }
    }

    public boolean isUnsigned()
    {

        if ((_colFlag & 32) > 0) {

            return true;
        }
         else {

            return false;
        }
    }

    public boolean isZeroFill()
    {

        if ((_colFlag & 64) > 0) {

            return true;
        }
         else {

            return false;
        }
    }

    public String toString()
    {

        return getFullName();
    }

    int getDecimals()
    {

        return _colDecimals;
    }

    boolean isNotNull()
    {

        if ((_colFlag & 1) > 0) {

            return true;
        }
         else {

            return false;
        }
    }
}