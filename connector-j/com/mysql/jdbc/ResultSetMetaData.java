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
 * A ResultSetMetaData object can be used to find out about the types and
 * properties of the columns in a ResultSet
 *
 * @see java.sql.ResultSetMetaData
 * @author Mark Matthews
 * @version $Id$
 */
package com.mysql.jdbc;

import java.sql.SQLException;
import java.sql.Types;


public class ResultSetMetaData
    implements java.sql.ResultSetMetaData
{

    //~ Instance/static variables .............................................

    Field[] Fields;

    //~ Constructors ..........................................................

    /**
   *    Initialise for a result with a tuple set and
   *    a field descriptor set
   *
   * @param rows the Vector of rows returned by the ResultSet
   * @param fields the array of field descriptors
   * 
   * @author Mark Matthews <mmatthew@worldserver.com>
   */
    public ResultSetMetaData(Field[] Fields)
    {
        this.Fields = Fields;
    }

    //~ Methods ...............................................................

    /**
   * Is the column automatically numbered (and thus read-only)
   *
   * MySQL Auto-increment columns are not read only,
   * so to conform to the spec, this method returns false.
   *
   * @param column the first column is 1, the second is 2...
   * @return true if so
   * @exception java.sql.SQLException if a database access error occurs
   * 
   * @author Mark Matthews <mmatthew@worldserver.com>
   */
    public boolean isAutoIncrement(int column)
                            throws java.sql.SQLException
    {

        return false;
    }

    /**
   * Does a column's case matter? ASSUMPTION: Any field that is
   * not obviously case insensitive is assumed to be case sensitive
   *
   * @param column the first column is 1, the second is 2...
   * @return true if so
   * @exception java.sql.SQLException if a database access error occurs
   * 
   * @author Mark Matthews <mmatthew@worldserver.com>
   */
    public boolean isCaseSensitive(int column)
                            throws java.sql.SQLException
    {

        int sql_type = getField(column).getSQLType();

        switch (sql_type) {

            case Types.SMALLINT:
            case Types.INTEGER:
            case Types.FLOAT:
            case Types.REAL:
            case Types.DOUBLE:
            case Types.DATE:
            case Types.TIME:
            case Types.TIMESTAMP:
                return false;

            default:
                return true;
        }
    }

    /**
   * What's a column's table's catalog name?
   *
   * @param column the first column is 1, the second is 2...
   * @return catalog name, or "" if not applicable
   * @exception java.sql.SQLException if a database access error occurs
   * 
   * @author Mark Matthews <mmatthew@worldserver.com>
   */
    public String getCatalogName(int column)
                          throws java.sql.SQLException
    {

        Field F = getField(column);

        return "";
    }

    //--------------------------JDBC 2.0-----------------------------------

    /**
     * JDBC 2.0
     *
     * <p>Return the fully qualified name of the Java class whose instances 
     * are manufactured if ResultSet.getObject() is called to retrieve a value 
     * from the column.  ResultSet.getObject() may return a subClass of the
     * class returned by this method.
     */
    public String getColumnClassName(int column)
                              throws SQLException
    {

        Field f = getField(column);

        switch (f.getSQLType()) {

            case Types.BIT:
                return "java.lang.Boolean";

            case Types.TINYINT:

                if (f.isUnsigned()) {

                    return "java.lang.Integer";
                } else {

                    return "java.lang.Byte";
                }

            case Types.SMALLINT:

                if (f.isUnsigned()) {

                    return "java.lang.Integer";
                } else {

                    return "java.lang.Short";
                }

            case Types.INTEGER:

                if (f.isUnsigned()) {

                    return "java.lang.Long";
                } else {

                    return "java.lang.Integer";
                }

            case Types.BIGINT:
                return "java.lang.Long";

            case Types.DECIMAL:
            case Types.NUMERIC:
                return "java.math.BigDecimal";

            case Types.REAL:
            case Types.FLOAT:
                return "java.lang.Float";

            case Types.DOUBLE:
                return "java.lang.Double";

            case Types.CHAR:
            case Types.VARCHAR:
            case Types.LONGVARCHAR:
                return "java.lang.String";

            case Types.BINARY:
            case Types.VARBINARY:
            case Types.LONGVARBINARY:

                if (!f.isBlob()) {

                    return "java.lang.String";
                } else if (!f.isBinary()) {

                    return "java.lang.String";
                } else {

                    return "java.lang.Object";
                }

            case Types.DATE:
                return "java.sql.Date";

            case Types.TIME:
                return "java.sql.Time";

            case Types.TIMESTAMP:
                return "java.sql.Timestamp";

            default:
                return "java.lang.Object";
        }
    }

    /**
   * Whats the number of columns in the ResultSet?
   *
   * @return the number
   * @exception java.sql.SQLException if a database access error occurs
   * 
   * @author Mark Matthews <mmatthew@worldserver.com>
   */
    public int getColumnCount()
                       throws java.sql.SQLException
    {

        return Fields.length;
    }

    /**
   * What is the column's normal maximum width in characters?
   *
   * @param column the first column is 1, the second is 2, etc.
   * @return the maximum width
   * @exception java.sql.SQLException if a database access error occurs
   * 
   * @author Mark Matthews <mmatthew@worldserver.com>
   */
    public int getColumnDisplaySize(int column)
                             throws java.sql.SQLException
    {

        return getField(column).getLength();
    }

    /**
   * What is the suggested column title for use in printouts and
   * displays?
   *
   * @param column the first column is 1, the second is 2, etc.
   * @return the column label
   * @exception java.sql.SQLException if a database access error occurs
   * 
   * @author Mark Matthews <mmatthew@worldserver.com>
   */
    public String getColumnLabel(int column)
                          throws java.sql.SQLException
    {

        return getColumnName(column);
    }

    /**
   * What's a column's name?
   *
   * @param column the first column is 1, the second is 2, etc.
   * @return the column name
   * @exception java.sql.SQLException if a databvase access error occurs
   *
   * @author Mark Matthews <mmatthew@worldserver.com>
   */
    public String getColumnName(int column)
                         throws java.sql.SQLException
    {

        return getField(column).getName();
    }

    /**
   * What is a column's SQL Type? (java.sql.Type int)
   *
   * @param column the first column is 1, the second is 2, etc.
   * @return the java.sql.Type value
   * @exception java.sql.SQLException if a database access error occurs
   * @see java.sql.Types
   * 
   * @author Mark Matthews <mmatthew@worldserver.com>
   */
    public int getColumnType(int column)
                      throws java.sql.SQLException
    {

        return getField(column).getSQLType();
    }

    /**
   * Whats is the column's data source specific type name?
   *
   * @param column the first column is 1, the second is 2, etc.
   * @return the type name
   * @exception java.sql.SQLException if a database access error occurs
   *   
   * @author Mark Matthews <mmatthew@worldserver.com>
   */
    public String getColumnTypeName(int column)
                             throws java.sql.SQLException
    {

        int mysql_type = getField(column).getMysqlType();

        switch (mysql_type) {

            case MysqlDefs.FIELD_TYPE_DECIMAL:
                return "DECIMAL";

            case MysqlDefs.FIELD_TYPE_TINY:
                return "TINY";

            case MysqlDefs.FIELD_TYPE_SHORT:
                return "SHORT";

            case MysqlDefs.FIELD_TYPE_LONG:
                return "LONG";

            case MysqlDefs.FIELD_TYPE_FLOAT:
                return "FLOAT";

            case MysqlDefs.FIELD_TYPE_DOUBLE:
                return "DOUBLE";

            case MysqlDefs.FIELD_TYPE_NULL:
                return "NULL";

            case MysqlDefs.FIELD_TYPE_TIMESTAMP:
                return "TIMESTAMP";

            case MysqlDefs.FIELD_TYPE_LONGLONG:
                return "LONGLONG";

            case MysqlDefs.FIELD_TYPE_INT24:
                return "INT";

            case MysqlDefs.FIELD_TYPE_DATE:
                return "DATE";

            case MysqlDefs.FIELD_TYPE_TIME:
                return "TIME";

            case MysqlDefs.FIELD_TYPE_DATETIME:
                return "DATETIME";

            case MysqlDefs.FIELD_TYPE_TINY_BLOB:
                return "TINYBLOB";

            case MysqlDefs.FIELD_TYPE_MEDIUM_BLOB:
                return "MEDIUMBLOB";

            case MysqlDefs.FIELD_TYPE_LONG_BLOB:
                return "LONGBLOB";

            case MysqlDefs.FIELD_TYPE_BLOB:

                if (getField(column).isBinary()) {

                    return "TEXT";
                } else {

                    return "BLOB";
                }

            case MysqlDefs.FIELD_TYPE_VAR_STRING:
                return "VARCHAR";

            case MysqlDefs.FIELD_TYPE_STRING:
                return "CHAR";

            case MysqlDefs.FIELD_TYPE_ENUM:
                return "ENUM";

            case MysqlDefs.FIELD_TYPE_SET:
                return "SET";

            default:
                return "UNKNOWN";
        }
    }

    /**
   * Is the column a cash value?
   *
   * @param column the first column is 1, the second is 2...
   * @return true if its a cash column
   * @exception java.sql.SQLException if a database access error occurs
   * 
   * @author Mark Matthews <mmatthew@worldserver.com>
   */
    public boolean isCurrency(int column)
                       throws java.sql.SQLException
    {

        return false;
    }

    /**
   * Will a write on this column definately succeed?
   *
   * @param column the first column is 1, the second is 2, etc..
   * @return true if so
   * @exception java.sql.SQLException if a database access error occurs
   * 
   * @author Mark Matthews <mmatthew@worldserver.com>
   */
    public boolean isDefinitelyWritable(int column)
                                 throws java.sql.SQLException
    {

        return isWritable(column);
    }

    /**
   * Can you put a NULL in this column?  
   *
   * @param column the first column is 1, the second is 2...
   * @return one of the columnNullable values
   * @exception java.sql.SQLException if a database access error occurs
   * 
   * @author Mark Matthews <mmatthew@worldserver.com>
   */
    public int isNullable(int column)
                   throws java.sql.SQLException
    {

        if (!getField(column).isNotNull()) {

            return java.sql.ResultSetMetaData.columnNullable;
        } else {

            return java.sql.ResultSetMetaData.columnNoNulls;
        }
    }

    /**
   * What is a column's number of decimal digits.
   *
   * @param column the first column is 1, the second is 2...
   * @return the precision
   * @exception java.sql.SQLException if a database access error occurs
   * 
   * @author James Klicman <james@klicman.com>
   */
    public int getPrecision(int column)
                     throws java.sql.SQLException
    {

        Field F = getField(column);

        if (isDecimalType(F.getSQLType())) {

            if (F.getDecimals() > 0) {

                return F.getLength() - 1;
            }

            return F.getLength();
        }

        return 0;
    }

    /**
   * Is the column definitely not writable?
   *
   * @param column the first column is 1, the second is 2, etc.
   * @return true if so
   * @exception java.sql.SQLException if a database access error occurs
   * 
   * @author Mark Matthews <mmatthew@worldserver.com>
   */
    public boolean isReadOnly(int column)
                       throws java.sql.SQLException
    {

        return false;
    }

    /**
   * What is a column's number of digits to the right of the
   * decimal point?
   *
   * @param column the first column is 1, the second is 2...
   * @return the scale
   * @exception java.sql.SQLException if a database access error occurs
   * 
   * @author James Klicman <james@klicman.com>
   */
    public int getScale(int column)
                 throws java.sql.SQLException
    {

        Field F = getField(column);

        if (isDecimalType(F.getSQLType())) {

            return F.getDecimals();
        }

        return 0;
    }

    /**
   * What is a column's table's schema?  This relies on us knowing
   * the table name.
   *
   * The JDBC specification allows us to return "" if this is not
   * applicable.
   *
   * @param column the first column is 1, the second is 2...
   * @return the Schema
   * @exception java.sql.SQLException if a database access error occurs
   * 
   * @author Mark Matthews <mmatthew@worldserver.com>
   */
    public String getSchemaName(int column)
                         throws java.sql.SQLException
    {

        return "";
    }

    /**
   * Can the column be used in a WHERE clause?  Basically for
   * this, I split the functions into two types: recognised
   * types (which are always useable), and OTHER types (which
   * may or may not be useable).  The OTHER types, for now, I
   * will assume they are useable.  We should really query the
   * catalog to see if they are useable.
   *
   * @param column the first column is 1, the second is 2...
   * @return true if they can be used in a WHERE clause
   * @exception java.sql.SQLException if a database access error occurs
   * 
   * @author Mark Matthews <mmatthew@worldserver.com>
   */
    public boolean isSearchable(int column)
                         throws java.sql.SQLException
    {

        return true;
    }

    /**
   * Is the column a signed number?
   *
   * @param column the first column is 1, the second is 2...
   * @return true if so
   * @exception java.sql.SQLException if a database access error occurs
   * 
   * @author Mark Matthews <mmatthew@worldserver.com>
   */
    public boolean isSigned(int column)
                     throws java.sql.SQLException
    {

        Field F = getField(column);
        int sql_type = F.getSQLType();

        switch (sql_type) {
			case Types.TINYINT:
            case Types.SMALLINT:
            case Types.INTEGER:
            case Types.BIGINT:
            case Types.FLOAT:
            case Types.REAL:
            case Types.DOUBLE:
            case Types.NUMERIC:
            case Types.DECIMAL:
                return !F.isUnsigned();

            case Types.DATE:
            case Types.TIME:
            case Types.TIMESTAMP:
                return false; // I guess

            default:
                return false;
        }
    }

    /**
   * Whats a column's table's name?  
   *
   * @param column the first column is 1, the second is 2...
   * @return column name, or "" if not applicable
   * @exception java.sql.SQLException if a database access error occurs
   * 
   * @author Mark Matthews <mmatthew@worldserver.com>
   */
    public String getTableName(int column)
                        throws java.sql.SQLException
    {

        return getField(column).getTableName();
    }

    /**
   * Is it possible for a write on the column to succeed?
   *
   * @param column the first column is 1, the second is 2, etc.
   * @return true if so
   * @exception java.sql.SQLException if a database access error occurs
   * 
   * @author Mark Matthews <mmatthew@worldserver.com>
   */
    public boolean isWritable(int column)
                       throws java.sql.SQLException
    {

        if (isReadOnly(column)) {

            return false;
        } else {

            return true;
        }
    }

    // *********************************************************************
    //
    //                END OF PUBLIC INTERFACE
    //
    // *********************************************************************
    protected Field getField(int columnIndex)
                      throws java.sql.SQLException
    {

        if (columnIndex < 1 || columnIndex > Fields.length) {
            throw new java.sql.SQLException("Column index out of range.", 
                                            "S1002");
        }

        return Fields[columnIndex - 1];
    }

    /**
     * Checks if the SQL Type is a Decimal/Number Type
     * @param type SQL Type
     * 
     * @author James Klicman <james@klicman.com>
     */
    private static final boolean isDecimalType(int type)
    {

        switch (type) {

            case Types.BIT:
            case Types.TINYINT:
            case Types.SMALLINT:
            case Types.INTEGER:
            case Types.BIGINT:
            case Types.FLOAT:
            case Types.REAL:
            case Types.DOUBLE:
            case Types.NUMERIC:
            case Types.DECIMAL:
                return true;
        }

        return false;
    }
}