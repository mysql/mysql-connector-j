/*
  Copyright (c) 2002, 2015, Oracle and/or its affiliates. All rights reserved.

  The MySQL Connector/J is licensed under the terms of the GPLv2
  <http://www.gnu.org/licenses/old-licenses/gpl-2.0.html>, like most MySQL Connectors.
  There are special exceptions to the terms and conditions of the GPLv2 as it is applied to
  this software, see the FLOSS License Exception
  <http://www.mysql.com/about/legal/licensing/foss-exception.html>.

  This program is free software; you can redistribute it and/or modify it under the terms
  of the GNU General Public License as published by the Free Software Foundation; version 2
  of the License.

  This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
  without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
  See the GNU General Public License for more details.

  You should have received a copy of the GNU General Public License along with this
  program; if not, write to the Free Software Foundation, Inc., 51 Franklin St, Fifth
  Floor, Boston, MA 02110-1301  USA

 */

package com.mysql.cj.jdbc;

import java.sql.Types;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import com.mysql.cj.mysqla.MysqlaConstants;

/**
 * MysqlDefs contains many values that are needed for communication with the MySQL server.
 */
public final class MysqlDefs {

    public static final long LENGTH_BLOB = 65535;

    public static final long LENGTH_LONGBLOB = 4294967295L;

    public static final long LENGTH_MEDIUMBLOB = 16777215;

    public static final long LENGTH_TINYBLOB = 255;

    // Limitations
    public static final int MAX_ROWS = 50000000; // From the MySQL FAQ

    /**
     * Used to indicate that the server sent no field-level character set information, so the driver should use the connection-level character encoding instead.
     */
    public static final int NO_CHARSET_INFO = -1;

    public static final byte OPEN_CURSOR_FLAG = 1;

    /**
     * Maps the given MySQL type to the correct JDBC type.
     */
    public static int mysqlToJavaType(int mysqlType) {
        int jdbcType;

        switch (mysqlType) {
            case MysqlaConstants.FIELD_TYPE_NEW_DECIMAL:
            case MysqlaConstants.FIELD_TYPE_DECIMAL:
                jdbcType = Types.DECIMAL;

                break;

            case MysqlaConstants.FIELD_TYPE_TINY:
                jdbcType = Types.TINYINT;

                break;

            case MysqlaConstants.FIELD_TYPE_SHORT:
                jdbcType = Types.SMALLINT;

                break;

            case MysqlaConstants.FIELD_TYPE_LONG:
                jdbcType = Types.INTEGER;

                break;

            case MysqlaConstants.FIELD_TYPE_FLOAT:
                jdbcType = Types.REAL;

                break;

            case MysqlaConstants.FIELD_TYPE_DOUBLE:
                jdbcType = Types.DOUBLE;

                break;

            case MysqlaConstants.FIELD_TYPE_NULL:
                jdbcType = Types.NULL;

                break;

            case MysqlaConstants.FIELD_TYPE_TIMESTAMP:
                jdbcType = Types.TIMESTAMP;

                break;

            case MysqlaConstants.FIELD_TYPE_LONGLONG:
                jdbcType = Types.BIGINT;

                break;

            case MysqlaConstants.FIELD_TYPE_INT24:
                jdbcType = Types.INTEGER;

                break;

            case MysqlaConstants.FIELD_TYPE_DATE:
                jdbcType = Types.DATE;

                break;

            case MysqlaConstants.FIELD_TYPE_TIME:
                jdbcType = Types.TIME;

                break;

            case MysqlaConstants.FIELD_TYPE_DATETIME:
                jdbcType = Types.TIMESTAMP;

                break;

            case MysqlaConstants.FIELD_TYPE_YEAR:
                jdbcType = Types.DATE;

                break;

            case MysqlaConstants.FIELD_TYPE_NEWDATE:
                jdbcType = Types.DATE;

                break;

            case MysqlaConstants.FIELD_TYPE_ENUM:
                jdbcType = Types.CHAR;

                break;

            case MysqlaConstants.FIELD_TYPE_SET:
                jdbcType = Types.CHAR;

                break;

            case MysqlaConstants.FIELD_TYPE_TINY_BLOB:
                jdbcType = Types.VARBINARY;

                break;

            case MysqlaConstants.FIELD_TYPE_MEDIUM_BLOB:
                jdbcType = Types.LONGVARBINARY;

                break;

            case MysqlaConstants.FIELD_TYPE_LONG_BLOB:
                jdbcType = Types.LONGVARBINARY;

                break;

            case MysqlaConstants.FIELD_TYPE_BLOB:
                jdbcType = Types.LONGVARBINARY;

                break;

            case MysqlaConstants.FIELD_TYPE_VAR_STRING:
            case MysqlaConstants.FIELD_TYPE_VARCHAR:
                jdbcType = Types.VARCHAR;

                break;

            case MysqlaConstants.FIELD_TYPE_STRING:
                jdbcType = Types.CHAR;

                break;
            case MysqlaConstants.FIELD_TYPE_GEOMETRY:
                jdbcType = Types.BINARY;

                break;
            case MysqlaConstants.FIELD_TYPE_BIT:
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
    public static int mysqlToJavaType(String mysqlType) {
        if (mysqlType.equalsIgnoreCase("BIT")) {
            return mysqlToJavaType(MysqlaConstants.FIELD_TYPE_BIT);
        } else if (mysqlType.equalsIgnoreCase("TINYINT")) {
            return mysqlToJavaType(MysqlaConstants.FIELD_TYPE_TINY);
        } else if (mysqlType.equalsIgnoreCase("SMALLINT")) {
            return mysqlToJavaType(MysqlaConstants.FIELD_TYPE_SHORT);
        } else if (mysqlType.equalsIgnoreCase("MEDIUMINT")) {
            return mysqlToJavaType(MysqlaConstants.FIELD_TYPE_INT24);
        } else if (mysqlType.equalsIgnoreCase("INT") || mysqlType.equalsIgnoreCase("INTEGER")) {
            return mysqlToJavaType(MysqlaConstants.FIELD_TYPE_LONG);
        } else if (mysqlType.equalsIgnoreCase("BIGINT")) {
            return mysqlToJavaType(MysqlaConstants.FIELD_TYPE_LONGLONG);
        } else if (mysqlType.equalsIgnoreCase("INT24")) {
            return mysqlToJavaType(MysqlaConstants.FIELD_TYPE_INT24);
        } else if (mysqlType.equalsIgnoreCase("REAL")) {
            return mysqlToJavaType(MysqlaConstants.FIELD_TYPE_DOUBLE);
        } else if (mysqlType.equalsIgnoreCase("FLOAT")) {
            return mysqlToJavaType(MysqlaConstants.FIELD_TYPE_FLOAT);
        } else if (mysqlType.equalsIgnoreCase("DECIMAL")) {
            return mysqlToJavaType(MysqlaConstants.FIELD_TYPE_DECIMAL);
        } else if (mysqlType.equalsIgnoreCase("NUMERIC")) {
            return mysqlToJavaType(MysqlaConstants.FIELD_TYPE_DECIMAL);
        } else if (mysqlType.equalsIgnoreCase("DOUBLE")) {
            return mysqlToJavaType(MysqlaConstants.FIELD_TYPE_DOUBLE);
        } else if (mysqlType.equalsIgnoreCase("CHAR")) {
            return mysqlToJavaType(MysqlaConstants.FIELD_TYPE_STRING);
        } else if (mysqlType.equalsIgnoreCase("VARCHAR")) {
            return mysqlToJavaType(MysqlaConstants.FIELD_TYPE_VAR_STRING);
        } else if (mysqlType.equalsIgnoreCase("DATE")) {
            return mysqlToJavaType(MysqlaConstants.FIELD_TYPE_DATE);
        } else if (mysqlType.equalsIgnoreCase("TIME")) {
            return mysqlToJavaType(MysqlaConstants.FIELD_TYPE_TIME);
        } else if (mysqlType.equalsIgnoreCase("YEAR")) {
            return mysqlToJavaType(MysqlaConstants.FIELD_TYPE_YEAR);
        } else if (mysqlType.equalsIgnoreCase("TIMESTAMP")) {
            return mysqlToJavaType(MysqlaConstants.FIELD_TYPE_TIMESTAMP);
        } else if (mysqlType.equalsIgnoreCase("DATETIME")) {
            return mysqlToJavaType(MysqlaConstants.FIELD_TYPE_DATETIME);
        } else if (mysqlType.equalsIgnoreCase("TINYBLOB")) {
            return java.sql.Types.BINARY;
        } else if (mysqlType.equalsIgnoreCase("BLOB")) {
            return java.sql.Types.LONGVARBINARY;
        } else if (mysqlType.equalsIgnoreCase("MEDIUMBLOB")) {
            return java.sql.Types.LONGVARBINARY;
        } else if (mysqlType.equalsIgnoreCase("LONGBLOB")) {
            return java.sql.Types.LONGVARBINARY;
        } else if (mysqlType.equalsIgnoreCase("TINYTEXT")) {
            return java.sql.Types.VARCHAR;
        } else if (mysqlType.equalsIgnoreCase("TEXT")) {
            return java.sql.Types.LONGVARCHAR;
        } else if (mysqlType.equalsIgnoreCase("MEDIUMTEXT")) {
            return java.sql.Types.LONGVARCHAR;
        } else if (mysqlType.equalsIgnoreCase("LONGTEXT")) {
            return java.sql.Types.LONGVARCHAR;
        } else if (mysqlType.equalsIgnoreCase("ENUM")) {
            return mysqlToJavaType(MysqlaConstants.FIELD_TYPE_ENUM);
        } else if (mysqlType.equalsIgnoreCase("SET")) {
            return mysqlToJavaType(MysqlaConstants.FIELD_TYPE_SET);
        } else if (mysqlType.equalsIgnoreCase("GEOMETRY")) {
            return mysqlToJavaType(MysqlaConstants.FIELD_TYPE_GEOMETRY);
        } else if (mysqlType.equalsIgnoreCase("BINARY")) {
            return Types.BINARY; // no concrete type on the wire
        } else if (mysqlType.equalsIgnoreCase("VARBINARY")) {
            return Types.VARBINARY; // no concrete type on the wire
        } else if (mysqlType.equalsIgnoreCase("BIT")) {
            return mysqlToJavaType(MysqlaConstants.FIELD_TYPE_BIT);
        }

        // Punt
        return java.sql.Types.OTHER;
    }

    /**
     * @param mysqlType
     */
    public static String typeToName(int mysqlType) {
        switch (mysqlType) {
            case MysqlaConstants.FIELD_TYPE_DECIMAL:
                return "FIELD_TYPE_DECIMAL";

            case MysqlaConstants.FIELD_TYPE_TINY:
                return "FIELD_TYPE_TINY";

            case MysqlaConstants.FIELD_TYPE_SHORT:
                return "FIELD_TYPE_SHORT";

            case MysqlaConstants.FIELD_TYPE_LONG:
                return "FIELD_TYPE_LONG";

            case MysqlaConstants.FIELD_TYPE_FLOAT:
                return "FIELD_TYPE_FLOAT";

            case MysqlaConstants.FIELD_TYPE_DOUBLE:
                return "FIELD_TYPE_DOUBLE";

            case MysqlaConstants.FIELD_TYPE_NULL:
                return "FIELD_TYPE_NULL";

            case MysqlaConstants.FIELD_TYPE_TIMESTAMP:
                return "FIELD_TYPE_TIMESTAMP";

            case MysqlaConstants.FIELD_TYPE_LONGLONG:
                return "FIELD_TYPE_LONGLONG";

            case MysqlaConstants.FIELD_TYPE_INT24:
                return "FIELD_TYPE_INT24";

            case MysqlaConstants.FIELD_TYPE_DATE:
                return "FIELD_TYPE_DATE";

            case MysqlaConstants.FIELD_TYPE_TIME:
                return "FIELD_TYPE_TIME";

            case MysqlaConstants.FIELD_TYPE_DATETIME:
                return "FIELD_TYPE_DATETIME";

            case MysqlaConstants.FIELD_TYPE_YEAR:
                return "FIELD_TYPE_YEAR";

            case MysqlaConstants.FIELD_TYPE_NEWDATE:
                return "FIELD_TYPE_NEWDATE";

            case MysqlaConstants.FIELD_TYPE_ENUM:
                return "FIELD_TYPE_ENUM";

            case MysqlaConstants.FIELD_TYPE_SET:
                return "FIELD_TYPE_SET";

            case MysqlaConstants.FIELD_TYPE_TINY_BLOB:
                return "FIELD_TYPE_TINY_BLOB";

            case MysqlaConstants.FIELD_TYPE_MEDIUM_BLOB:
                return "FIELD_TYPE_MEDIUM_BLOB";

            case MysqlaConstants.FIELD_TYPE_LONG_BLOB:
                return "FIELD_TYPE_LONG_BLOB";

            case MysqlaConstants.FIELD_TYPE_BLOB:
                return "FIELD_TYPE_BLOB";

            case MysqlaConstants.FIELD_TYPE_VAR_STRING:
                return "FIELD_TYPE_VAR_STRING";

            case MysqlaConstants.FIELD_TYPE_STRING:
                return "FIELD_TYPE_STRING";

            case MysqlaConstants.FIELD_TYPE_VARCHAR:
                return "FIELD_TYPE_VARCHAR";

            case MysqlaConstants.FIELD_TYPE_GEOMETRY:
                return "FIELD_TYPE_GEOMETRY";

            default:
                return " Unknown MySQL Type # " + mysqlType;
        }
    }

    private static Map<String, Integer> mysqlToJdbcTypesMap = new HashMap<String, Integer>();

    static {
        mysqlToJdbcTypesMap.put("BIT", Integer.valueOf(mysqlToJavaType(MysqlaConstants.FIELD_TYPE_BIT)));

        mysqlToJdbcTypesMap.put("TINYINT", Integer.valueOf(mysqlToJavaType(MysqlaConstants.FIELD_TYPE_TINY)));
        mysqlToJdbcTypesMap.put("SMALLINT", Integer.valueOf(mysqlToJavaType(MysqlaConstants.FIELD_TYPE_SHORT)));
        mysqlToJdbcTypesMap.put("MEDIUMINT", Integer.valueOf(mysqlToJavaType(MysqlaConstants.FIELD_TYPE_INT24)));
        mysqlToJdbcTypesMap.put("INT", Integer.valueOf(mysqlToJavaType(MysqlaConstants.FIELD_TYPE_LONG)));
        mysqlToJdbcTypesMap.put("INTEGER", Integer.valueOf(mysqlToJavaType(MysqlaConstants.FIELD_TYPE_LONG)));
        mysqlToJdbcTypesMap.put("BIGINT", Integer.valueOf(mysqlToJavaType(MysqlaConstants.FIELD_TYPE_LONGLONG)));
        mysqlToJdbcTypesMap.put("INT24", Integer.valueOf(mysqlToJavaType(MysqlaConstants.FIELD_TYPE_INT24)));
        mysqlToJdbcTypesMap.put("REAL", Integer.valueOf(mysqlToJavaType(MysqlaConstants.FIELD_TYPE_DOUBLE)));
        mysqlToJdbcTypesMap.put("FLOAT", Integer.valueOf(mysqlToJavaType(MysqlaConstants.FIELD_TYPE_FLOAT)));
        mysqlToJdbcTypesMap.put("DECIMAL", Integer.valueOf(mysqlToJavaType(MysqlaConstants.FIELD_TYPE_DECIMAL)));
        mysqlToJdbcTypesMap.put("NUMERIC", Integer.valueOf(mysqlToJavaType(MysqlaConstants.FIELD_TYPE_DECIMAL)));
        mysqlToJdbcTypesMap.put("DOUBLE", Integer.valueOf(mysqlToJavaType(MysqlaConstants.FIELD_TYPE_DOUBLE)));
        mysqlToJdbcTypesMap.put("CHAR", Integer.valueOf(mysqlToJavaType(MysqlaConstants.FIELD_TYPE_STRING)));
        mysqlToJdbcTypesMap.put("VARCHAR", Integer.valueOf(mysqlToJavaType(MysqlaConstants.FIELD_TYPE_VAR_STRING)));
        mysqlToJdbcTypesMap.put("DATE", Integer.valueOf(mysqlToJavaType(MysqlaConstants.FIELD_TYPE_DATE)));
        mysqlToJdbcTypesMap.put("TIME", Integer.valueOf(mysqlToJavaType(MysqlaConstants.FIELD_TYPE_TIME)));
        mysqlToJdbcTypesMap.put("YEAR", Integer.valueOf(mysqlToJavaType(MysqlaConstants.FIELD_TYPE_YEAR)));
        mysqlToJdbcTypesMap.put("TIMESTAMP", Integer.valueOf(mysqlToJavaType(MysqlaConstants.FIELD_TYPE_TIMESTAMP)));
        mysqlToJdbcTypesMap.put("DATETIME", Integer.valueOf(mysqlToJavaType(MysqlaConstants.FIELD_TYPE_DATETIME)));
        mysqlToJdbcTypesMap.put("TINYBLOB", Integer.valueOf(java.sql.Types.BINARY));
        mysqlToJdbcTypesMap.put("BLOB", Integer.valueOf(java.sql.Types.LONGVARBINARY));
        mysqlToJdbcTypesMap.put("MEDIUMBLOB", Integer.valueOf(java.sql.Types.LONGVARBINARY));
        mysqlToJdbcTypesMap.put("LONGBLOB", Integer.valueOf(java.sql.Types.LONGVARBINARY));
        mysqlToJdbcTypesMap.put("TINYTEXT", Integer.valueOf(java.sql.Types.VARCHAR));
        mysqlToJdbcTypesMap.put("TEXT", Integer.valueOf(java.sql.Types.LONGVARCHAR));
        mysqlToJdbcTypesMap.put("MEDIUMTEXT", Integer.valueOf(java.sql.Types.LONGVARCHAR));
        mysqlToJdbcTypesMap.put("LONGTEXT", Integer.valueOf(java.sql.Types.LONGVARCHAR));
        mysqlToJdbcTypesMap.put("ENUM", Integer.valueOf(mysqlToJavaType(MysqlaConstants.FIELD_TYPE_ENUM)));
        mysqlToJdbcTypesMap.put("SET", Integer.valueOf(mysqlToJavaType(MysqlaConstants.FIELD_TYPE_SET)));
        mysqlToJdbcTypesMap.put("GEOMETRY", Integer.valueOf(mysqlToJavaType(MysqlaConstants.FIELD_TYPE_GEOMETRY)));
    }

    public static final void appendJdbcTypeMappingQuery(StringBuilder buf, String mysqlTypeColumnName) {

        buf.append("CASE ");
        Map<String, Integer> typesMap = new HashMap<String, Integer>();
        typesMap.putAll(mysqlToJdbcTypesMap);
        typesMap.put("BINARY", Integer.valueOf(Types.BINARY));
        typesMap.put("VARBINARY", Integer.valueOf(Types.VARBINARY));

        Iterator<String> mysqlTypes = typesMap.keySet().iterator();

        while (mysqlTypes.hasNext()) {
            String mysqlTypeName = mysqlTypes.next();
            buf.append(" WHEN ");
            buf.append(mysqlTypeColumnName);
            buf.append("='");
            buf.append(mysqlTypeName);
            buf.append("' THEN ");
            buf.append(typesMap.get(mysqlTypeName));

            if (mysqlTypeName.equalsIgnoreCase("DOUBLE") || mysqlTypeName.equalsIgnoreCase("FLOAT") || mysqlTypeName.equalsIgnoreCase("DECIMAL")
                    || mysqlTypeName.equalsIgnoreCase("NUMERIC")) {
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
