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

public class Field
{

    //~ Instance/static variables .............................................

    private final static int AUTO_INCREMENT_FLAG = 512;
    int colDecimals;
    short colFlag;
    int length; // Internal length of the field;
    int mysqlType = -1; // the MySQL type
    String name; // The Field name
    int sqlType = -1; // the java.sql.Type
    String tableName; // The Name of the Table
    private String fullName;

    //~ Constructors ..........................................................

    /**
   * Constructor used by DatabaseMetaData methods.
   */
    public Field(String tableName, String columnName, int jdbcType, int length)
    {
        this.tableName = tableName;
        this.name = columnName;
        this.length = length;
        sqlType = jdbcType;
        colFlag = 0;
        colDecimals = 0;
    }

    Field(String tableName, String columnName, int length, int mysqlType, 
          short colFlag, int colDecimals)
    {
        this.tableName = tableName;
        this.name = columnName;
        this.length = length;
        this.colFlag = colFlag;
        this.colDecimals = colDecimals;
        this.mysqlType = mysqlType;

        // Map MySqlTypes to java.sql Types
        sqlType = MysqlDefs.mysqlToJavaType(mysqlType);

        StringBuffer fullNameBuf = new StringBuffer(
                                           tableName.length() + 1 + 
                                           name.length());
        fullNameBuf.append(tableName);
        fullNameBuf.append(".");
        fullNameBuf.append(name);
        fullName = fullNameBuf.toString();
        fullNameBuf = null;

        boolean isBinary = isBinary();

        //
        // Handle TEXT type (special case), Fix proposed by Peter McKeown
        //
        if (sqlType == java.sql.Types.LONGVARBINARY && !isBinary)
        {
            sqlType = java.sql.Types.LONGVARCHAR;
        }
        else if (sqlType == java.sql.Types.VARBINARY && !isBinary)
        {
            sqlType = java.sql.Types.VARCHAR;
        }
    }

    //~ Methods ...............................................................

    /**
     * DOCUMENT ME!
     * 
     * @return DOCUMENT ME! 
     */
    public boolean isAutoIncrement()
    {

        if ((colFlag & AUTO_INCREMENT_FLAG) > 0)
        {

            return true;
        }
        else
        {

            return false;
        }
    }

    /**
     * DOCUMENT ME!
     * 
     * @return DOCUMENT ME! 
     */
    public boolean isBinary()
    {

        if ((colFlag & 128) > 0)
        {

            return true;
        }
        else
        {

            return false;
        }
    }

    /**
     * DOCUMENT ME!
     * 
     * @return DOCUMENT ME! 
     */
    public boolean isBlob()
    {

        if ((colFlag & 16) > 0)
        {

            return true;
        }
        else
        {

            return false;
        }
    }

    /**
     * DOCUMENT ME!
     * 
     * @return DOCUMENT ME! 
     */
    public String getFullName()
    {

        return fullName;
    }

    /**
     * DOCUMENT ME!
     * 
     * @return DOCUMENT ME! 
     */
    public int getLength()
    {

        return length;
    }

    /**
     * DOCUMENT ME!
     * 
     * @return DOCUMENT ME! 
     */
    public boolean isMultipleKey()
    {

        if ((colFlag & 8) > 0)
        {

            return true;
        }
        else
        {

            return false;
        }
    }

    /**
     * DOCUMENT ME!
     * 
     * @return DOCUMENT ME! 
     */
    public int getMysqlType()
    {

        return mysqlType;
    }

    /**
     * DOCUMENT ME!
     * 
     * @return DOCUMENT ME! 
     */
    public String getName()
    {

        if (name != null)
        {

            return name;
        }
        else
        {

            return null;
        }
    }

    /**
     * DOCUMENT ME!
     * 
     * @return DOCUMENT ME! 
     */
    public boolean isPrimaryKey()
    {

        if ((colFlag & 2) > 0)
        {

            return true;
        }
        else
        {

            return false;
        }
    }

    /**
     * DOCUMENT ME!
     * 
     * @return DOCUMENT ME! 
     */
    public int getSQLType()
    {

        return sqlType;
    }

    /**
     * DOCUMENT ME!
     * 
     * @return DOCUMENT ME! 
     */
    public String getTable()
    {

        if (tableName != null)
        {

            return tableName;
        }
        else
        {

            return null;
        }
    }

    /**
     * DOCUMENT ME!
     * 
     * @return DOCUMENT ME! 
     */
    public String getTableName()
    {

        return tableName;
    }

    /**
     * DOCUMENT ME!
     * 
     * @return DOCUMENT ME! 
     */
    public boolean isUniqueKey()
    {

        if ((colFlag & 4) > 0)
        {

            return true;
        }
        else
        {

            return false;
        }
    }

    /**
     * DOCUMENT ME!
     * 
     * @return DOCUMENT ME! 
     */
    public boolean isUnsigned()
    {

        if ((colFlag & 32) > 0)
        {

            return true;
        }
        else
        {

            return false;
        }
    }

    /**
     * DOCUMENT ME!
     * 
     * @return DOCUMENT ME! 
     */
    public boolean isZeroFill()
    {

        if ((colFlag & 64) > 0)
        {

            return true;
        }
        else
        {

            return false;
        }
    }

    /**
     * DOCUMENT ME!
     * 
     * @return DOCUMENT ME! 
     */
    public String toString()
    {

        return getFullName();
    }

    int getDecimals()
    {

        return colDecimals;
    }

    boolean isNotNull()
    {

        if ((colFlag & 1) > 0)
        {

            return true;
        }
        else
        {

            return false;
        }
    }
}