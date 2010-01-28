/*
 Copyright  2002-2007 MySQL AB, 2008 Sun Microsystems
 All rights reserved. Use is subject to license terms.

  The MySQL Connector/J is licensed under the terms of the GPL,
  like most MySQL Connectors. There are special exceptions to the
  terms and conditions of the GPL as it is applied to this software,
  see the FLOSS License Exception available on mysql.com.

  This program is free software; you can redistribute it and/or
  modify it under the terms of the GNU General Public License as
  published by the Free Software Foundation; version 2 of the
  License.

  This program is distributed in the hope that it will be useful,  
  but WITHOUT ANY WARRANTY; without even the implied warranty of
  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
  GNU General Public License for more details.

  You should have received a copy of the GNU General Public License
  along with this program; if not, write to the Free Software
  Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
  02110-1301 USA



 */
package com.mysql.jdbc;

import java.sql.Types;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * MysqlDefs contains many values that are needed for communication with the
 * MySQL server.
 * 
 * @author Mark Matthews
 * @version $Id$
 */
public final class MysqlDefs {
	// ~ Static fields/initializers
	// ---------------------------------------------

	static final int COM_BINLOG_DUMP = 18;

	static final int COM_CHANGE_USER = 17;

	static final int COM_CLOSE_STATEMENT = 25;

	static final int COM_CONNECT_OUT = 20;

	static final int COM_END = 29;

	static final int COM_EXECUTE = 23;

	static final int COM_FETCH = 28;

	static final int COM_LONG_DATA = 24;

	static final int COM_PREPARE = 22;

	static final int COM_REGISTER_SLAVE = 21;

	static final int COM_RESET_STMT = 26;

	static final int COM_SET_OPTION = 27;

	static final int COM_TABLE_DUMP = 19;

	static final int CONNECT = 11;

	static final int CREATE_DB = 5;

	static final int DEBUG = 13;

	static final int DELAYED_INSERT = 16;

	static final int DROP_DB = 6;

	static final int FIELD_LIST = 4;

	static final int FIELD_TYPE_BIT = 16;

	public static final int FIELD_TYPE_BLOB = 252;

	static final int FIELD_TYPE_DATE = 10;

	static final int FIELD_TYPE_DATETIME = 12;

	// Data Types
	static final int FIELD_TYPE_DECIMAL = 0;

	static final int FIELD_TYPE_DOUBLE = 5;

	static final int FIELD_TYPE_ENUM = 247;

	static final int FIELD_TYPE_FLOAT = 4;

	static final int FIELD_TYPE_GEOMETRY = 255;

	static final int FIELD_TYPE_INT24 = 9;

	static final int FIELD_TYPE_LONG = 3;

	static final int FIELD_TYPE_LONG_BLOB = 251;

	static final int FIELD_TYPE_LONGLONG = 8;

	static final int FIELD_TYPE_MEDIUM_BLOB = 250;

	static final int FIELD_TYPE_NEW_DECIMAL = 246;

	static final int FIELD_TYPE_NEWDATE = 14;

	static final int FIELD_TYPE_NULL = 6;

	static final int FIELD_TYPE_SET = 248;

	static final int FIELD_TYPE_SHORT = 2;

	static final int FIELD_TYPE_STRING = 254;

	static final int FIELD_TYPE_TIME = 11;

	static final int FIELD_TYPE_TIMESTAMP = 7;

	static final int FIELD_TYPE_TINY = 1;

	// Older data types
	static final int FIELD_TYPE_TINY_BLOB = 249;

	static final int FIELD_TYPE_VAR_STRING = 253;

	static final int FIELD_TYPE_VARCHAR = 15;

	// Newer data types
	static final int FIELD_TYPE_YEAR = 13;

	static final int INIT_DB = 2;

	static final long LENGTH_BLOB = 65535;

	static final long LENGTH_LONGBLOB = 4294967295L;

	static final long LENGTH_MEDIUMBLOB = 16777215;

	static final long LENGTH_TINYBLOB = 255;

	// Limitations
	static final int MAX_ROWS = 50000000; // From the MySQL FAQ

	/**
	 * Used to indicate that the server sent no field-level character set
	 * information, so the driver should use the connection-level character
	 * encoding instead.
	 */
	public static final int NO_CHARSET_INFO = -1;

	static final byte OPEN_CURSOR_FLAG = 1;

	static final int PING = 14;

	static final int PROCESS_INFO = 10;

	static final int PROCESS_KILL = 12;

	static final int QUERY = 3;

	static final int QUIT = 1;

	// ~ Methods
	// ----------------------------------------------------------------

	static final int RELOAD = 7;

	static final int SHUTDOWN = 8;

	//
	// Constants defined from mysql
	//
	// DB Operations
	static final int SLEEP = 0;

	static final int STATISTICS = 9;

	static final int TIME = 15;

	/**
	 * Maps the given MySQL type to the correct JDBC type.
	 */
	static int mysqlToJavaType(int mysqlType) {
		int jdbcType;

		switch (mysqlType) {
		case MysqlDefs.FIELD_TYPE_NEW_DECIMAL:
		case MysqlDefs.FIELD_TYPE_DECIMAL:
			jdbcType = Types.DECIMAL;

			break;

		case MysqlDefs.FIELD_TYPE_TINY:
			jdbcType = Types.TINYINT;

			break;

		case MysqlDefs.FIELD_TYPE_SHORT:
			jdbcType = Types.SMALLINT;

			break;

		case MysqlDefs.FIELD_TYPE_LONG:
			jdbcType = Types.INTEGER;

			break;

		case MysqlDefs.FIELD_TYPE_FLOAT:
			jdbcType = Types.REAL;

			break;

		case MysqlDefs.FIELD_TYPE_DOUBLE:
			jdbcType = Types.DOUBLE;

			break;

		case MysqlDefs.FIELD_TYPE_NULL:
			jdbcType = Types.NULL;

			break;

		case MysqlDefs.FIELD_TYPE_TIMESTAMP:
			jdbcType = Types.TIMESTAMP;

			break;

		case MysqlDefs.FIELD_TYPE_LONGLONG:
			jdbcType = Types.BIGINT;

			break;

		case MysqlDefs.FIELD_TYPE_INT24:
			jdbcType = Types.INTEGER;

			break;

		case MysqlDefs.FIELD_TYPE_DATE:
			jdbcType = Types.DATE;

			break;

		case MysqlDefs.FIELD_TYPE_TIME:
			jdbcType = Types.TIME;

			break;

		case MysqlDefs.FIELD_TYPE_DATETIME:
			jdbcType = Types.TIMESTAMP;

			break;

		case MysqlDefs.FIELD_TYPE_YEAR:
			jdbcType = Types.DATE;

			break;

		case MysqlDefs.FIELD_TYPE_NEWDATE:
			jdbcType = Types.DATE;

			break;

		case MysqlDefs.FIELD_TYPE_ENUM:
			jdbcType = Types.CHAR;

			break;

		case MysqlDefs.FIELD_TYPE_SET:
			jdbcType = Types.CHAR;

			break;

		case MysqlDefs.FIELD_TYPE_TINY_BLOB:
			jdbcType = Types.VARBINARY;

			break;

		case MysqlDefs.FIELD_TYPE_MEDIUM_BLOB:
			jdbcType = Types.LONGVARBINARY;

			break;

		case MysqlDefs.FIELD_TYPE_LONG_BLOB:
			jdbcType = Types.LONGVARBINARY;

			break;

		case MysqlDefs.FIELD_TYPE_BLOB:
			jdbcType = Types.LONGVARBINARY;

			break;

		case MysqlDefs.FIELD_TYPE_VAR_STRING:
		case MysqlDefs.FIELD_TYPE_VARCHAR:
			jdbcType = Types.VARCHAR;

			break;

		case MysqlDefs.FIELD_TYPE_STRING:
			jdbcType = Types.CHAR;

			break;
		case MysqlDefs.FIELD_TYPE_GEOMETRY:
			jdbcType = Types.BINARY;

			break;
		case MysqlDefs.FIELD_TYPE_BIT:
			jdbcType = Types.BIT;

			break;
		default:
			jdbcType = Types.VARCHAR;
		}

		return jdbcType;
	}

	/**
	 * Maps the given MySQL type to the correct JDBC type.
	 */
	static int mysqlToJavaType(String mysqlType) {
		if (mysqlType.equalsIgnoreCase("BIT")) {
			return mysqlToJavaType(FIELD_TYPE_BIT);
		} else if (mysqlType.equalsIgnoreCase("TINYINT")) { //$NON-NLS-1$
			return mysqlToJavaType(FIELD_TYPE_TINY);
		} else if (mysqlType.equalsIgnoreCase("SMALLINT")) { //$NON-NLS-1$
			return mysqlToJavaType(FIELD_TYPE_SHORT);
		} else if (mysqlType.equalsIgnoreCase("MEDIUMINT")) { //$NON-NLS-1$
			return mysqlToJavaType(FIELD_TYPE_INT24);
		} else if (mysqlType.equalsIgnoreCase("INT") || mysqlType.equalsIgnoreCase("INTEGER")) { //$NON-NLS-1$ //$NON-NLS-2$
			return mysqlToJavaType(FIELD_TYPE_LONG);
		} else if (mysqlType.equalsIgnoreCase("BIGINT")) { //$NON-NLS-1$
			return mysqlToJavaType(FIELD_TYPE_LONGLONG);
		} else if (mysqlType.equalsIgnoreCase("INT24")) { //$NON-NLS-1$
			return mysqlToJavaType(FIELD_TYPE_INT24);
		} else if (mysqlType.equalsIgnoreCase("REAL")) { //$NON-NLS-1$
			return mysqlToJavaType(FIELD_TYPE_DOUBLE);
		} else if (mysqlType.equalsIgnoreCase("FLOAT")) { //$NON-NLS-1$
			return mysqlToJavaType(FIELD_TYPE_FLOAT);
		} else if (mysqlType.equalsIgnoreCase("DECIMAL")) { //$NON-NLS-1$
			return mysqlToJavaType(FIELD_TYPE_DECIMAL);
		} else if (mysqlType.equalsIgnoreCase("NUMERIC")) { //$NON-NLS-1$
			return mysqlToJavaType(FIELD_TYPE_DECIMAL);
		} else if (mysqlType.equalsIgnoreCase("DOUBLE")) { //$NON-NLS-1$
			return mysqlToJavaType(FIELD_TYPE_DOUBLE);
		} else if (mysqlType.equalsIgnoreCase("CHAR")) { //$NON-NLS-1$
			return mysqlToJavaType(FIELD_TYPE_STRING);
		} else if (mysqlType.equalsIgnoreCase("VARCHAR")) { //$NON-NLS-1$
			return mysqlToJavaType(FIELD_TYPE_VAR_STRING);
		} else if (mysqlType.equalsIgnoreCase("DATE")) { //$NON-NLS-1$
			return mysqlToJavaType(FIELD_TYPE_DATE);
		} else if (mysqlType.equalsIgnoreCase("TIME")) { //$NON-NLS-1$
			return mysqlToJavaType(FIELD_TYPE_TIME);
		} else if (mysqlType.equalsIgnoreCase("YEAR")) { //$NON-NLS-1$
			return mysqlToJavaType(FIELD_TYPE_YEAR);
		} else if (mysqlType.equalsIgnoreCase("TIMESTAMP")) { //$NON-NLS-1$
			return mysqlToJavaType(FIELD_TYPE_TIMESTAMP);
		} else if (mysqlType.equalsIgnoreCase("DATETIME")) { //$NON-NLS-1$
			return mysqlToJavaType(FIELD_TYPE_DATETIME);
		} else if (mysqlType.equalsIgnoreCase("TINYBLOB")) { //$NON-NLS-1$
			return java.sql.Types.BINARY;
		} else if (mysqlType.equalsIgnoreCase("BLOB")) { //$NON-NLS-1$
			return java.sql.Types.LONGVARBINARY;
		} else if (mysqlType.equalsIgnoreCase("MEDIUMBLOB")) { //$NON-NLS-1$
			return java.sql.Types.LONGVARBINARY;
		} else if (mysqlType.equalsIgnoreCase("LONGBLOB")) { //$NON-NLS-1$
			return java.sql.Types.LONGVARBINARY;
		} else if (mysqlType.equalsIgnoreCase("TINYTEXT")) { //$NON-NLS-1$
			return java.sql.Types.VARCHAR;
		} else if (mysqlType.equalsIgnoreCase("TEXT")) { //$NON-NLS-1$
			return java.sql.Types.LONGVARCHAR;
		} else if (mysqlType.equalsIgnoreCase("MEDIUMTEXT")) { //$NON-NLS-1$
			return java.sql.Types.LONGVARCHAR;
		} else if (mysqlType.equalsIgnoreCase("LONGTEXT")) { //$NON-NLS-1$
			return java.sql.Types.LONGVARCHAR;
		} else if (mysqlType.equalsIgnoreCase("ENUM")) { //$NON-NLS-1$
			return mysqlToJavaType(FIELD_TYPE_ENUM);
		} else if (mysqlType.equalsIgnoreCase("SET")) { //$NON-NLS-1$
			return mysqlToJavaType(FIELD_TYPE_SET);
		} else if (mysqlType.equalsIgnoreCase("GEOMETRY")) {
			return mysqlToJavaType(FIELD_TYPE_GEOMETRY);
		} else if (mysqlType.equalsIgnoreCase("BINARY")) {
			return Types.BINARY; // no concrete type on the wire
		} else if (mysqlType.equalsIgnoreCase("VARBINARY")) {
			return Types.VARBINARY; // no concrete type on the wire
		} else if (mysqlType.equalsIgnoreCase("BIT")) {
			return mysqlToJavaType(FIELD_TYPE_BIT);
		}

		// Punt
		return java.sql.Types.OTHER;
	}

	/**
	 * @param mysqlType
	 * @return
	 */
	public static String typeToName(int mysqlType) {
		switch (mysqlType) {
		case MysqlDefs.FIELD_TYPE_DECIMAL:
			return "FIELD_TYPE_DECIMAL";

		case MysqlDefs.FIELD_TYPE_TINY:
			return "FIELD_TYPE_TINY";

		case MysqlDefs.FIELD_TYPE_SHORT:
			return "FIELD_TYPE_SHORT";

		case MysqlDefs.FIELD_TYPE_LONG:
			return "FIELD_TYPE_LONG";

		case MysqlDefs.FIELD_TYPE_FLOAT:
			return "FIELD_TYPE_FLOAT";

		case MysqlDefs.FIELD_TYPE_DOUBLE:
			return "FIELD_TYPE_DOUBLE";

		case MysqlDefs.FIELD_TYPE_NULL:
			return "FIELD_TYPE_NULL";

		case MysqlDefs.FIELD_TYPE_TIMESTAMP:
			return "FIELD_TYPE_TIMESTAMP";

		case MysqlDefs.FIELD_TYPE_LONGLONG:
			return "FIELD_TYPE_LONGLONG";

		case MysqlDefs.FIELD_TYPE_INT24:
			return "FIELD_TYPE_INT24";

		case MysqlDefs.FIELD_TYPE_DATE:
			return "FIELD_TYPE_DATE";

		case MysqlDefs.FIELD_TYPE_TIME:
			return "FIELD_TYPE_TIME";

		case MysqlDefs.FIELD_TYPE_DATETIME:
			return "FIELD_TYPE_DATETIME";

		case MysqlDefs.FIELD_TYPE_YEAR:
			return "FIELD_TYPE_YEAR";

		case MysqlDefs.FIELD_TYPE_NEWDATE:
			return "FIELD_TYPE_NEWDATE";

		case MysqlDefs.FIELD_TYPE_ENUM:
			return "FIELD_TYPE_ENUM";

		case MysqlDefs.FIELD_TYPE_SET:
			return "FIELD_TYPE_SET";

		case MysqlDefs.FIELD_TYPE_TINY_BLOB:
			return "FIELD_TYPE_TINY_BLOB";

		case MysqlDefs.FIELD_TYPE_MEDIUM_BLOB:
			return "FIELD_TYPE_MEDIUM_BLOB";

		case MysqlDefs.FIELD_TYPE_LONG_BLOB:
			return "FIELD_TYPE_LONG_BLOB";

		case MysqlDefs.FIELD_TYPE_BLOB:
			return "FIELD_TYPE_BLOB";

		case MysqlDefs.FIELD_TYPE_VAR_STRING:
			return "FIELD_TYPE_VAR_STRING";

		case MysqlDefs.FIELD_TYPE_STRING:
			return "FIELD_TYPE_STRING";

		case MysqlDefs.FIELD_TYPE_VARCHAR:
			return "FIELD_TYPE_VARCHAR";

		case MysqlDefs.FIELD_TYPE_GEOMETRY:
			return "FIELD_TYPE_GEOMETRY";

		default:
			return " Unknown MySQL Type # " + mysqlType;
		}
	}

	private static Map mysqlToJdbcTypesMap = new HashMap();

	static {
		mysqlToJdbcTypesMap.put("BIT", Constants.integerValueOf(
				mysqlToJavaType(FIELD_TYPE_BIT)));

		mysqlToJdbcTypesMap.put("TINYINT", Constants.integerValueOf(
				mysqlToJavaType(FIELD_TYPE_TINY)));
		mysqlToJdbcTypesMap.put("SMALLINT", Constants.integerValueOf(
				mysqlToJavaType(FIELD_TYPE_SHORT)));
		mysqlToJdbcTypesMap.put("MEDIUMINT", Constants.integerValueOf(
				mysqlToJavaType(FIELD_TYPE_INT24)));
		mysqlToJdbcTypesMap.put("INT", Constants.integerValueOf(
				mysqlToJavaType(FIELD_TYPE_LONG)));
		mysqlToJdbcTypesMap.put("INTEGER", Constants.integerValueOf(
				mysqlToJavaType(FIELD_TYPE_LONG)));
		mysqlToJdbcTypesMap.put("BIGINT", Constants.integerValueOf(
				mysqlToJavaType(FIELD_TYPE_LONGLONG)));
		mysqlToJdbcTypesMap.put("INT24", Constants.integerValueOf(
				mysqlToJavaType(FIELD_TYPE_INT24)));
		mysqlToJdbcTypesMap.put("REAL", Constants.integerValueOf(
				mysqlToJavaType(FIELD_TYPE_DOUBLE)));
		mysqlToJdbcTypesMap.put("FLOAT", Constants.integerValueOf(
				mysqlToJavaType(FIELD_TYPE_FLOAT)));
		mysqlToJdbcTypesMap.put("DECIMAL", Constants.integerValueOf(
				mysqlToJavaType(FIELD_TYPE_DECIMAL)));
		mysqlToJdbcTypesMap.put("NUMERIC", Constants.integerValueOf(
				mysqlToJavaType(FIELD_TYPE_DECIMAL)));
		mysqlToJdbcTypesMap.put("DOUBLE", Constants.integerValueOf(
				mysqlToJavaType(FIELD_TYPE_DOUBLE)));
		mysqlToJdbcTypesMap.put("CHAR", Constants.integerValueOf(
				mysqlToJavaType(FIELD_TYPE_STRING)));
		mysqlToJdbcTypesMap.put("VARCHAR", Constants.integerValueOf(
				mysqlToJavaType(FIELD_TYPE_VAR_STRING)));
		mysqlToJdbcTypesMap.put("DATE", Constants.integerValueOf(
				mysqlToJavaType(FIELD_TYPE_DATE)));
		mysqlToJdbcTypesMap.put("TIME", Constants.integerValueOf(
				mysqlToJavaType(FIELD_TYPE_TIME)));
		mysqlToJdbcTypesMap.put("YEAR", Constants.integerValueOf(
				mysqlToJavaType(FIELD_TYPE_YEAR)));
		mysqlToJdbcTypesMap.put("TIMESTAMP", Constants.integerValueOf(
				mysqlToJavaType(FIELD_TYPE_TIMESTAMP)));
		mysqlToJdbcTypesMap.put("DATETIME", Constants.integerValueOf(
				mysqlToJavaType(FIELD_TYPE_DATETIME)));
		mysqlToJdbcTypesMap.put("TINYBLOB", Constants.integerValueOf(java.sql.Types.BINARY));
		mysqlToJdbcTypesMap.put("BLOB", Constants.integerValueOf(
				java.sql.Types.LONGVARBINARY));
		mysqlToJdbcTypesMap.put("MEDIUMBLOB", Constants.integerValueOf(
				java.sql.Types.LONGVARBINARY));
		mysqlToJdbcTypesMap.put("LONGBLOB", Constants.integerValueOf(
				java.sql.Types.LONGVARBINARY));
		mysqlToJdbcTypesMap
				.put("TINYTEXT", Constants.integerValueOf(java.sql.Types.VARCHAR));
		mysqlToJdbcTypesMap
				.put("TEXT", Constants.integerValueOf(java.sql.Types.LONGVARCHAR));
		mysqlToJdbcTypesMap.put("MEDIUMTEXT", Constants.integerValueOf(
				java.sql.Types.LONGVARCHAR));
		mysqlToJdbcTypesMap.put("LONGTEXT", Constants.integerValueOf(
				java.sql.Types.LONGVARCHAR));
		mysqlToJdbcTypesMap.put("ENUM", Constants.integerValueOf(
				mysqlToJavaType(FIELD_TYPE_ENUM)));
		mysqlToJdbcTypesMap.put("SET", Constants.integerValueOf(
				mysqlToJavaType(FIELD_TYPE_SET)));
		mysqlToJdbcTypesMap.put("GEOMETRY", Constants.integerValueOf(
				mysqlToJavaType(FIELD_TYPE_GEOMETRY)));
	}

	static final void appendJdbcTypeMappingQuery(StringBuffer buf, String mysqlTypeColumnName) {

		buf.append("CASE ");
		Map typesMap = new HashMap();
		typesMap.putAll(mysqlToJdbcTypesMap);
		typesMap.put("BINARY", Constants.integerValueOf(Types.BINARY));
		typesMap.put("VARBINARY", Constants.integerValueOf(Types.VARBINARY));
		
		Iterator mysqlTypes = typesMap.keySet().iterator();
		
		while (mysqlTypes.hasNext()) {
			String mysqlTypeName = (String)mysqlTypes.next();
			buf.append(" WHEN ");
			buf.append(mysqlTypeColumnName);
			buf.append("='");
			buf.append(mysqlTypeName);
			buf.append("' THEN ");
			buf.append(typesMap.get(mysqlTypeName));
			
			if (mysqlTypeName.equalsIgnoreCase("DOUBLE") ||
					mysqlTypeName.equalsIgnoreCase("FLOAT") ||
					mysqlTypeName.equalsIgnoreCase("DECIMAL") ||
					mysqlTypeName.equalsIgnoreCase("NUMERIC")) {
				buf.append(" WHEN ");
				buf.append(mysqlTypeColumnName);
				buf.append("='");
				buf.append(mysqlTypeName);
				buf.append(" unsigned' THEN ");
				buf.append(typesMap.get(mysqlTypeName));	
			}	
		}
		
		buf.append(" ELSE ");
		buf.append(Types.OTHER);
		buf.append(" END ");
		
	}
}
