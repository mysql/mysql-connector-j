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

import java.io.UnsupportedEncodingException;


/**
 * Field is a class used to describe fields in a
 * ResultSet
 * 
 * @author Mark Matthews
 * @version $Id$
 */
public class Field {

    //~ Instance/static variables .............................................

    private static final int AUTO_INCREMENT_FLAG = 512;
    private int colDecimals;
    private short colFlag;
    private int length; // Internal length of the field;
    private int mysqlType = -1; // the MySQL type
    private String name; // The Field name
    private int nameStart;
    private int nameLength;
    private int sqlType = -1; // the java.sql.Type
    private String tableName; // The Name of the Table
    private int tableNameStart;
    private int tableNameLength;
    private byte[] buffer;
    private String fullName = null;
    private Connection connection = null;

    //~ Constructors ..........................................................

    /**
   * Constructor used by DatabaseMetaData methods.
   */
    Field(String tableName, String columnName, int jdbcType, int length) {
        this.tableName = tableName;
        this.name = columnName;
        this.length = length;
        sqlType = jdbcType;
        colFlag = 0;
        colDecimals = 0;
    }

    Field(byte[] buffer, int nameStart, int nameLength, int tableNameStart, 
          int tableNameLength, int length, int mysqlType, short colFlag, 
          int colDecimals) {
        this.buffer = buffer;
        this.nameStart = nameStart;
        this.nameLength = nameLength;
        this.tableNameStart = tableNameStart;
        this.tableNameLength = tableNameLength;
        this.length = length;
        this.colFlag = colFlag;
        this.colDecimals = colDecimals;
        this.mysqlType = mysqlType;

        // Map MySqlTypes to java.sql Types
        sqlType = MysqlDefs.mysqlToJavaType(mysqlType);

        boolean isBinary = isBinary();

        //
        // Handle TEXT type (special case), Fix proposed by Peter McKeown
        //
        if (sqlType == java.sql.Types.LONGVARBINARY && !isBinary) {
            sqlType = java.sql.Types.LONGVARCHAR;
        } else if (sqlType == java.sql.Types.VARBINARY && !isBinary) {
            sqlType = java.sql.Types.VARCHAR;
        }
    }

    //~ Methods ...............................................................

    /**
     * DOCUMENT ME!
     * 
     * @return DOCUMENT ME! 
     */
    public boolean isAutoIncrement() {

        if ((colFlag & AUTO_INCREMENT_FLAG) > 0) {

            return true;
        } else {

            return false;
        }
    }

    /**
     * DOCUMENT ME!
     * 
     * @return DOCUMENT ME! 
     */
    public boolean isBinary() {

        if ((colFlag & 128) > 0) {

            return true;
        } else {

            return false;
        }
    }

    /**
     * DOCUMENT ME!
     * 
     * @return DOCUMENT ME! 
     */
    public boolean isBlob() {

        if ((colFlag & 16) > 0) {

            return true;
        } else {

            return false;
        }
    }

    /**
     * DOCUMENT ME!
     * 
     * @return DOCUMENT ME! 
     */
    public String getFullName() {

        if (fullName == null) {

            StringBuffer fullNameBuf = new StringBuffer(
                                               getTableName().length() + 1
                                               + getName().length());
            fullNameBuf.append(tableName);

            // much faster to append a char than a String
            fullNameBuf.append('.');
            fullNameBuf.append(name);
            fullName = fullNameBuf.toString();
            fullNameBuf = null;
        }

        return fullName;
    }

    /**
     * DOCUMENT ME!
     * 
     * @return DOCUMENT ME! 
     */
    public int getLength() {

        return length;
    }

    /**
     * DOCUMENT ME!
     * 
     * @return DOCUMENT ME! 
     */
    public boolean isMultipleKey() {

        if ((colFlag & 8) > 0) {

            return true;
        } else {

            return false;
        }
    }

    /**
     * DOCUMENT ME!
     * 
     * @return DOCUMENT ME! 
     */
    public int getMysqlType() {

        return mysqlType;
    }

    /**
     * DOCUMENT ME!
     * 
     * @return DOCUMENT ME! 
     */
    public String getName() {

        if (name == null) {
            name = getStringFromBytes(nameStart, nameLength);
        }

        return name;
    }

    /**
     * DOCUMENT ME!
     * 
     * @return DOCUMENT ME! 
     */
    public boolean isPrimaryKey() {

        if ((colFlag & 2) > 0) {

            return true;
        } else {

            return false;
        }
    }

    /**
     * DOCUMENT ME!
     * 
     * @return DOCUMENT ME! 
     */
    public int getSQLType() {

        return sqlType;
    }

    /**
     * DOCUMENT ME!
     * 
     * @return DOCUMENT ME! 
     */
    public String getTable() {

        return getTableName();
    }

    /**
     * DOCUMENT ME!
     * 
     * @return DOCUMENT ME! 
     */
    public String getTableName() {

        if (tableName == null) {
            tableName = getStringFromBytes(tableNameStart, tableNameLength);
        }

        return tableName;
    }

    /**
     * Create a string with the correct charset encoding from the
     * byte-buffer that contains the data for this field
     */
    private String getStringFromBytes(int stringStart, int stringLength) {

        String stringVal = null;

        if (connection != null) {

            if (connection.useUnicode()) {

                String encoding = connection.getEncoding();

                if (encoding != null) {

                    SingleByteCharsetConverter converter = null;

                    try {
                        converter = SingleByteCharsetConverter.getInstance(
                                            encoding);
                    } catch (UnsupportedEncodingException uee) {

                        // ignore, code further down handles this
                    }

                    if (converter != null) { // we have a converter
                        stringVal = converter.toString(buffer, stringStart, 
                                                       stringLength);
                    } else {
                        // we have no converter, use JVM standard charset 
                        stringVal = StringUtils.toAsciiString3(buffer, 
                                                               stringStart, 
                                                               stringLength);
                    }
                } else {
                     // we have no encoding, use JVM standard charset
                    stringVal = StringUtils.toAsciiString3(buffer, stringStart, 
                                                           stringLength);
                }
            }  else {
                // we are not using unicode, so use JVM standard charset 
                stringVal = StringUtils.toAsciiString3(buffer, stringStart, 
                                                       stringLength);
            }
        } else {
            // we don't have a connection, so punt 
            stringVal = StringUtils.toAsciiString3(buffer, stringStart, 
                                                   stringLength);
        }

        return stringVal;
    }

    /**
     * DOCUMENT ME!
     * 
     * @return DOCUMENT ME! 
     */
    public boolean isUniqueKey() {

        if ((colFlag & 4) > 0) {

            return true;
        } else {

            return false;
        }
    }

    /**
     * DOCUMENT ME!
     * 
     * @return DOCUMENT ME! 
     */
    public boolean isUnsigned() {

        if ((colFlag & 32) > 0) {

            return true;
        } else {

            return false;
        }
    }

    /**
     * DOCUMENT ME!
     * 
     * @return DOCUMENT ME! 
     */
    public boolean isZeroFill() {

        if ((colFlag & 64) > 0) {

            return true;
        } else {

            return false;
        }
    }

    /**
     * DOCUMENT ME!
     * 
     * @return DOCUMENT ME! 
     */
    public String toString() {

        return getFullName();
    }

    /**
     * DOCUMENT ME!
     * 
     * @param conn DOCUMENT ME!
     */
    public void setConnection(Connection conn) {
        this.connection = conn;
    }

    int getDecimals() {

        return colDecimals;
    }

    boolean isNotNull() {

        if ((colFlag & 1) > 0) {

            return true;
        } else {

            return false;
        }
    }
}