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
 * MysqlDefs contains many values that are needed for communication
 * with the MySQL server.
 * 
 * @author Mark Matthews <mmatthew@worldserver.com>
 * @version $Id$
 */

package org.gjt.mm.mysql;

import java.sql.Types;

final class MysqlDefs
{

  //
  // Constants defined from mysql
  //

  // DB Operations

  static final int SLEEP        =  0;
  static final int QUIT         =  1;
  static final int INIT_DB      =  2;
  static final int QUERY        =  3;
  static final int FIELD_LIST   =  4;
  static final int CREATE_DB    =  5;
  static final int DROP_DB      =  6;
  static final int RELOAD       =  7;
  static final int SHUTDOWN     =  8;
  static final int STATISTICS   =  9;
  static final int PROCESS_INFO = 10;
  static final int CONNECT      = 11;
  static final int PROCESS_KILL = 12;
  static final int DEBUG        = 13;
  static final int PING         = 14;
  static final int TIME         = 15;
  static final int DELAYED_INSERT = 16;
  static final int CHANGE_USER    = 17;
    

  // Data Types

  static final int FIELD_TYPE_DECIMAL     =   0;
  static final int FIELD_TYPE_TINY        =   1;
  static final int FIELD_TYPE_SHORT       =   2;
  static final int FIELD_TYPE_LONG        =   3;
  static final int FIELD_TYPE_FLOAT       =   4;
  static final int FIELD_TYPE_DOUBLE      =   5;
  static final int FIELD_TYPE_NULL        =   6;
  static final int FIELD_TYPE_TIMESTAMP   =   7;
  static final int FIELD_TYPE_LONGLONG    =   8;
  static final int FIELD_TYPE_INT24       =   9;
  static final int FIELD_TYPE_DATE        =  10;
  static final int FIELD_TYPE_TIME        =  11;
  static final int FIELD_TYPE_DATETIME    =  12;
  
  // Newer data types
  static final int FIELD_TYPE_YEAR        =  13;
  static final int FIELD_TYPE_NEWDATE     =  14;
  static final int FIELD_TYPE_ENUM        = 247;
  static final int FIELD_TYPE_SET         = 248;
  
  // Older data types
  static final int FIELD_TYPE_TINY_BLOB   = 249;
  static final int FIELD_TYPE_MEDIUM_BLOB = 250;
  static final int FIELD_TYPE_LONG_BLOB   = 251;
  static final int FIELD_TYPE_BLOB        = 252;
  static final int FIELD_TYPE_VAR_STRING  = 253;
  static final int FIELD_TYPE_STRING      = 254;

  // Limitations
  static final int MAX_ROWS = 50000000; // From the MySQL FAQ

  static int mysqlToJavaType(int mysql_type)
  {
	  int sql_type;

	  switch (mysql_type) {
		case MysqlDefs.FIELD_TYPE_DECIMAL     :   sql_type = Types.DECIMAL; break;
		case MysqlDefs.FIELD_TYPE_TINY        :   sql_type = Types.TINYINT; break;
		case MysqlDefs.FIELD_TYPE_SHORT       :   sql_type = Types.SMALLINT; break;
		case MysqlDefs.FIELD_TYPE_LONG        :   sql_type = Types.INTEGER; break;
		case MysqlDefs.FIELD_TYPE_FLOAT       :   sql_type = Types.FLOAT; break;
		case MysqlDefs.FIELD_TYPE_DOUBLE      :   sql_type = Types.DOUBLE; break;
		case MysqlDefs.FIELD_TYPE_NULL        :   sql_type = Types.NULL; break;
		case MysqlDefs.FIELD_TYPE_TIMESTAMP   :   sql_type = Types.TIMESTAMP; break;
		case MysqlDefs.FIELD_TYPE_LONGLONG    :   sql_type = Types.BIGINT; break;
		case MysqlDefs.FIELD_TYPE_INT24       :   sql_type = Types.INTEGER; break;
		case MysqlDefs.FIELD_TYPE_DATE        :   sql_type = Types.DATE; break;
		case MysqlDefs.FIELD_TYPE_TIME        :   sql_type = Types.TIME; break;
		case MysqlDefs.FIELD_TYPE_DATETIME    :   sql_type = Types.TIMESTAMP; break;
	  case MysqlDefs.FIELD_TYPE_YEAR        : sql_type = Types.DATE; break;
	  case MysqlDefs.FIELD_TYPE_NEWDATE     : sql_type = Types.DATE; break;
	  case MysqlDefs.FIELD_TYPE_ENUM        : sql_type = Types.CHAR; break;
	  case MysqlDefs.FIELD_TYPE_SET         : sql_type = Types.CHAR; break;
	  
		case MysqlDefs.FIELD_TYPE_TINY_BLOB   :   sql_type = Types.VARBINARY; break;
		case MysqlDefs.FIELD_TYPE_MEDIUM_BLOB :   sql_type = Types.LONGVARBINARY; break;
		case MysqlDefs.FIELD_TYPE_LONG_BLOB   :   sql_type = Types.LONGVARBINARY; break;
		case MysqlDefs.FIELD_TYPE_BLOB        :   sql_type = Types.LONGVARBINARY; break;
		case MysqlDefs.FIELD_TYPE_VAR_STRING  :   sql_type = Types.VARCHAR; break;
		case MysqlDefs.FIELD_TYPE_STRING      :   sql_type = Types.CHAR; break;
   
		default:   sql_type = Types.VARCHAR;
    } 
	  return sql_type;
  }

  static int mysqlToJavaType(String MySQLType) 
  {
	  MySQLType = MySQLType.toUpperCase();

	  if (MySQLType.equals("TINYINT")) {
		  return java.sql.Types.TINYINT;
	  }
	  else if (MySQLType.equals("SMALLINT")) {
		  return java.sql.Types.SMALLINT;
	  }
	  else if (MySQLType.equals("MEDIUMINT")) {
		  return java.sql.Types.SMALLINT;
	  }
	  else if (MySQLType.equals("INT")) {
		  return java.sql.Types.INTEGER;
	  }
	  else if (MySQLType.equals("INTEGER")) {
		  return java.sql.Types.INTEGER;
	  }
	  else if (MySQLType.equals("BIGINT")) {
		  return java.sql.Types.BIGINT;
	  }
	  else if (MySQLType.equals("INT24")) {
		  return java.sql.Types.BIGINT;
	  }
	  else if (MySQLType.equals("REAL")) {
		  return java.sql.Types.REAL;
	  }
	  else if (MySQLType.equals("FLOAT")) {
		  return java.sql.Types.FLOAT;
	  }
	  else if (MySQLType.equals("DECIMAL")) {
		  return java.sql.Types.DECIMAL;
	  }
	  else if (MySQLType.equals("NUMERIC")) {
		  return java.sql.Types.NUMERIC;
	  }
	  else if (MySQLType.equals("DOUBLE")) {
		  return java.sql.Types.DOUBLE;
	  }
	  else if (MySQLType.equals("CHAR")) {
		  return java.sql.Types.CHAR;
	  }
	  else if (MySQLType.equals("VARCHAR")) {
		  return java.sql.Types.VARCHAR;
	  }
	  else if (MySQLType.equals("DATE")) {
		  return java.sql.Types.DATE;
	  }
      else if (MySQLType.equals("TIME")) {
		  return java.sql.Types.TIME;
	  }
	  else if (MySQLType.equals("TIMESTAMP")) {
		  return java.sql.Types.TIMESTAMP;
	  }
	  else if (MySQLType.equals("DATETIME")) {
		  return java.sql.Types.TIMESTAMP;
	  }
	  else if (MySQLType.equals("TINYBLOB")) {
		  return java.sql.Types.BINARY;
	  }
	  else if (MySQLType.equals("BLOB")) {
		  return java.sql.Types.VARBINARY;
	  }
	  else if (MySQLType.equals("MEDIUMBLOB")) {
		  return java.sql.Types.VARBINARY;
	  }
	  else if (MySQLType.equals("LONGBLOB")) {
		  return java.sql.Types.LONGVARBINARY;
	  }
	  else if (MySQLType.equals("TINYTEXT")) {
		  return java.sql.Types.VARCHAR;
	  }
	  else if (MySQLType.equals("TEXT")) {
		  return java.sql.Types.LONGVARCHAR;
	  }
	  else if (MySQLType.equals("MEDIUMTEXT")) {
		  return java.sql.Types.LONGVARCHAR;
	  }
	  else if (MySQLType.equals("ENUM")) {
		  return java.sql.Types.CHAR;
	  }
	  else if (MySQLType.equals("SET")) {
		  return java.sql.Types.CHAR;
	  }

	  // Punt
	  return java.sql.Types.OTHER;
  }
}
