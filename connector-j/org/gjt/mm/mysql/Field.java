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
  int length;          // Internal length of the field;
  String Name;         // The Field name
  String TableName;    // The Name of the Table
  int sql_type = -1;   // the java.sql.Type
  int mysql_type = -1; // the MySQL type
  short colFlag;
  int colDecimals;

  private final static int AUTO_INCREMENT_FLAG = 512;

  Field(String Table, String Name, int length, int mysql_type, 
               short col_flag, int col_decimals)
  {
    this.TableName = new String(Table);
    this.Name = new String(Name);
    this.length = length;
    colFlag = col_flag;
    colDecimals = col_decimals;
    this.mysql_type = mysql_type;

    // Map MySqlTypes to java.sql Types

    sql_type = MysqlDefs.mysqlToJavaType(mysql_type);

    boolean is_binary = isBinary();

    //
    // Handle TEXT type (special case), Fix proposed by Peter McKeown
    //

    if (sql_type == java.sql.Types.LONGVARBINARY && !is_binary) {
	sql_type = java.sql.Types.LONGVARCHAR;
    }
    else if (sql_type == java.sql.Types.VARBINARY && !is_binary) {
	sql_type = java.sql.Types.VARCHAR;
    }
  }
  
  /**
   * Constructor used by DatabaseMetaData methods.
   */
   
  public Field(String Table, String Name, int jdbc_type, int length)
  { 
    this.TableName = new String(Table);
    this.Name = new String(Name);
    this.length = length;
    sql_type = jdbc_type;
    colFlag = 0;
    colDecimals = 0;
  }
      
  public String getTable() 
  {
    if (TableName != null)
      return TableName;
    else
      return null;
  }
  
  public String getName() 
  {
    if (Name != null)
      return new String(Name);
    else
      return null;
  }      
 
  public String getFullName() 
  {
    String FullName = TableName + "." + Name;
    return FullName;
  }

  public String getTableName()
  {
    return TableName;
  }
  
  public int getLength() 
  {
    return length;
  }
  
  public int getSQLType()
  {
    return sql_type;
  }

  public int getMysqlType()
  {
    return mysql_type;
  }

  int getDecimals() 
  {
    return colDecimals;
  }
  
  boolean isNotNull() 
  {
    if ((colFlag & 1) > 0) 
      return true;
    else 
      return false;
  }

  public boolean isPrimaryKey() 
  {
    if ((colFlag & 2) > 0) 
      return true;
    else 
      return false;
  }

  public boolean isUniqueKey() 
  {
    if ((colFlag & 4) > 0) 
      return true;
    else 
      return false;
  }
  
  public boolean isMultipleKey() 
  {
    if ((colFlag & 8) > 0) return true;
    else return false;
  }

  public boolean isBlob() 
  {
    if (( colFlag & 16) > 0) 
      return true;
    else 
      return false;
  }

  public boolean isUnsigned() 
  {
    if ((colFlag & 32) > 0) 
      return true;
    else 
      return false;
  }

  public boolean isZeroFill() 
  {
    if ((colFlag & 64) > 0) 
      return true;
    else 
      return false;
  }
  
  public boolean isBinary() 
  {
    if ((colFlag & 128) > 0) 
      return true;
    else 
      return false;
  }

  public boolean isAutoIncrement()
  {
    if ((colFlag & AUTO_INCREMENT_FLAG) > 0) {
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
