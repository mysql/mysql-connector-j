/*
 * MM JDBC Drivers for MySQL
 *
 * $Id$
 *
 * Copyright (C) 1998 Mark Matthews <mmatthew@worldserver.com>
 * 
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Library General Public
 * License as published by the Free Software Foundation; either
 * version 2 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Library General Public License for more details.
 * 
 * You should have received a copy of the GNU Library General Public
 * License along with this library; if not, write to the
 * Free Software Foundation, Inc., 59 Temple Place - Suite 330,
 * Boston, MA  02111-1307, USA.
 *
 * See the COPYING file located in the top-level-directory of
 * the archive of this library for complete text of license.
 */


/**
 * Field is a class used to describe fields in a
 * ResultSet
 * 
 * @author Mark Matthews <mmatthew@worldserver.com>
 * @version $Id$
 */

package org.gjt.mm.mysql;

import java.sql.*;
import java.util.*;

public class Field
{
  int _length;          // Internal length of the field;
  String _name;         // The Field name
  String _tableName;    // The Name of the Table
  int _sqlType = -1;   // the java.sql.Type
  int _mysqlType = -1; // the MySQL type
  short _colFlag;
  int _colDecimals;

  private final static int _AUTO_INCREMENT_FLAG = 512;

  Field(String Table, String Name, int length, int mysql_type, 
               short col_flag, int col_decimals)
  {
    this._tableName = new String(Table);
    this._name = new String(Name);
    this._length = length;
    _colFlag = col_flag;
    _colDecimals = col_decimals;
    this._mysqlType = mysql_type;

    // Map MySqlTypes to java.sql Types

    _sqlType = MysqlDefs.mysqlToJavaType(mysql_type);

    boolean is_binary = isBinary();

    //
    // Handle TEXT type (special case), Fix proposed by Peter McKeown
    //

    if (_sqlType == java.sql.Types.LONGVARBINARY && !is_binary) {
	_sqlType = java.sql.Types.LONGVARCHAR;
    }
    else if (_sqlType == java.sql.Types.VARBINARY && !is_binary) {
	_sqlType = java.sql.Types.VARCHAR;
    }
  }
  
  /**
   * Constructor used by DatabaseMetaData methods.
   */
   
  public Field(String Table, String Name, int jdbc_type, int length)
  { 
    this._tableName = new String(Table);
    this._name = new String(Name);
    this._length = length;
    _sqlType = jdbc_type;
    _colFlag = 0;
    _colDecimals = 0;
  }
      
  public String getTable() 
  {
    if (_tableName != null)
      return _tableName;
    else
      return null;
  }
  
  public String getName() 
  {
    if (_name != null)
      return new String(_name);
    else
      return null;
  }      
 
  public String getFullName() 
  {
    String FullName = _tableName + "." + _name;
    return FullName;
  }

  public String getTableName()
  {
    return _tableName;
  }
  
  public int getLength() 
  {
    return _length;
  }
  
  public int getSQLType()
  {
    return _sqlType;
  }

  public int getMysqlType()
  {
    return _mysqlType;
  }

  int getDecimals() 
  {
    return _colDecimals;
  }
  
  boolean isNotNull() 
  {
    if ((_colFlag & 1) > 0) 
      return true;
    else 
      return false;
  }

  public boolean isPrimaryKey() 
  {
    if ((_colFlag & 2) > 0) 
      return true;
    else 
      return false;
  }

  public boolean isUniqueKey() 
  {
    if ((_colFlag & 4) > 0) 
      return true;
    else 
      return false;
  }
  
  public boolean isMultipleKey() 
  {
    if ((_colFlag & 8) > 0) return true;
    else return false;
  }

  public boolean isBlob() 
  {
    if (( _colFlag & 16) > 0) 
      return true;
    else 
      return false;
  }

  public boolean isUnsigned() 
  {
    if ((_colFlag & 32) > 0) 
      return true;
    else 
      return false;
  }

  public boolean isZeroFill() 
  {
    if ((_colFlag & 64) > 0) 
      return true;
    else 
      return false;
  }
  
  public boolean isBinary() 
  {
    if ((_colFlag & 128) > 0) 
      return true;
    else 
      return false;
  }

  public boolean isAutoIncrement()
  {
    if ((_colFlag & _AUTO_INCREMENT_FLAG) > 0) {
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

  
}
