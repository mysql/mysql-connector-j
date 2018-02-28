/*
 * Copyright (c) 2002, 2018, Oracle and/or its affiliates. All rights reserved.
 *
 * This program is free software; you can redistribute it and/or modify it under
 * the terms of the GNU General Public License, version 2.0, as published by the
 * Free Software Foundation.
 *
 * This program is also distributed with certain software (including but not
 * limited to OpenSSL) that is licensed under separate terms, as designated in a
 * particular file or component or in included license documentation. The
 * authors of MySQL hereby grant you an additional permission to link the
 * program and your derivative works with the separately licensed software that
 * they have included with MySQL.
 *
 * Without limiting anything contained in the foregoing, this file, which is
 * part of MySQL Connector/J, is also subject to the Universal FOSS Exception,
 * version 1.0, a copy of which can be found at
 * http://oss.oracle.com/licenses/universal-foss-exception.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 * FOR A PARTICULAR PURPOSE. See the GNU General Public License, version 2.0,
 * for more details.
 *
 * You should have received a copy of the GNU General Public License along with
 * this program; if not, write to the Free Software Foundation, Inc.,
 * 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA
 */

package com.mysql.cj.jdbc.result;

import java.sql.SQLException;

import com.mysql.cj.CharsetMapping;
import com.mysql.cj.Messages;
import com.mysql.cj.MysqlType;
import com.mysql.cj.Session;
import com.mysql.cj.exceptions.ExceptionInterceptor;
import com.mysql.cj.exceptions.MysqlErrorNumbers;
import com.mysql.cj.jdbc.exceptions.SQLError;
import com.mysql.cj.result.Field;

/**
 * A ResultSetMetaData object can be used to find out about the types and properties of the columns in a ResultSet
 */
public class ResultSetMetaData implements java.sql.ResultSetMetaData {
    private static int clampedGetLength(Field f) {
        long fieldLength = f.getLength();

        if (fieldLength > Integer.MAX_VALUE) {
            fieldLength = Integer.MAX_VALUE;
        }

        return (int) fieldLength;
    }

    /* Session, used only for `getMaxBytesPerChar()' */
    private Session session;

    private Field[] fields;
    boolean useOldAliasBehavior = false;
    boolean treatYearAsDate = true;

    private ExceptionInterceptor exceptionInterceptor;

    /**
     * Initialize for a result with a tuple set and a field descriptor set
     * 
     * @param fields
     *            the array of field descriptors
     */
    public ResultSetMetaData(Session session, Field[] fields, boolean useOldAliasBehavior, boolean treatYearAsDate, ExceptionInterceptor exceptionInterceptor) {
        this.session = session;
        this.fields = fields;
        this.useOldAliasBehavior = useOldAliasBehavior;
        this.treatYearAsDate = treatYearAsDate;
        this.exceptionInterceptor = exceptionInterceptor;
    }

    /**
     * What's a column's table's catalog name?
     * 
     * @param column
     *            the first column is 1, the second is 2...
     * 
     * @return catalog name, or "" if not applicable
     * 
     * @throws SQLException
     *             if a database access error occurs
     */
    public String getCatalogName(int column) throws SQLException {
        Field f = getField(column);

        String database = f.getDatabaseName();

        return (database == null) ? "" : database;
    }

    /**
     * What's the Java character encoding name for the given column?
     * 
     * @param column
     *            the first column is 1, the second is 2, etc.
     * 
     * @return the Java character encoding name for the given column, or null if
     *         no Java character encoding maps to the MySQL character set for
     *         the given column.
     * 
     * @throws SQLException
     *             if an invalid column index is given.
     */
    public String getColumnCharacterEncoding(int column) throws SQLException {
        String mysqlName = getColumnCharacterSet(column);

        String javaName = null;

        if (mysqlName != null) {
            try {
                javaName = CharsetMapping.getJavaEncodingForMysqlCharset(mysqlName);
            } catch (RuntimeException ex) {
                SQLException sqlEx = SQLError.createSQLException(ex.toString(), MysqlErrorNumbers.SQL_STATE_ILLEGAL_ARGUMENT, null);
                sqlEx.initCause(ex);
                throw sqlEx;
            }
        }

        return javaName;
    }

    /**
     * What's the MySQL character set name for the given column?
     * 
     * @param column
     *            the first column is 1, the second is 2, etc.
     * 
     * @return the MySQL character set name for the given column
     * 
     * @throws SQLException
     *             if an invalid column index is given.
     */
    public String getColumnCharacterSet(int column) throws SQLException {
        return getField(column).getEncoding();
    }

    // --------------------------JDBC 2.0-----------------------------------

    /**
     * JDBC 2.0
     * 
     * <p>
     * Return the fully qualified name of the Java class whose instances are manufactured if ResultSet.getObject() is called to retrieve a value from the
     * column. ResultSet.getObject() may return a subClass of the class returned by this method.
     * </p>
     * 
     * @param column
     *            the column number to retrieve information for
     * 
     * @return the fully qualified name of the Java class whose instances are
     *         manufactured if ResultSet.getObject() is called to retrieve a
     *         value from the column.
     * 
     * @throws SQLException
     *             if an error occurs
     */
    public String getColumnClassName(int column) throws SQLException {
        Field f = getField(column);

        switch (f.getMysqlType()) {
            case YEAR:
                if (!this.treatYearAsDate) {
                    return Short.class.getName();
                }
                return f.getMysqlType().getClassName();

            default:
                return f.getMysqlType().getClassName();
        }

    }

    /**
     * Whats the number of columns in the ResultSet?
     * 
     * @return the number
     * 
     * @throws SQLException
     *             if a database access error occurs
     */
    public int getColumnCount() throws SQLException {
        return this.fields.length;
    }

    /**
     * What is the column's normal maximum width in characters?
     * 
     * @param column
     *            the first column is 1, the second is 2, etc.
     * 
     * @return the maximum width
     * 
     * @throws SQLException
     *             if a database access error occurs
     */
    public int getColumnDisplaySize(int column) throws SQLException {
        Field f = getField(column);

        int lengthInBytes = clampedGetLength(f);

        return lengthInBytes / this.session.getServerSession().getMaxBytesPerChar(f.getCollationIndex(), f.getEncoding());
    }

    /**
     * What is the suggested column title for use in printouts and displays?
     * 
     * @param column
     *            the first column is 1, the second is 2, etc.
     * 
     * @return the column label
     * 
     * @throws SQLException
     *             if a database access error occurs
     */
    public String getColumnLabel(int column) throws SQLException {
        if (this.useOldAliasBehavior) {
            return getColumnName(column);
        }

        return getField(column).getColumnLabel();
    }

    /**
     * What's a column's name?
     * 
     * @param column
     *            the first column is 1, the second is 2, etc.
     * 
     * @return the column name
     * 
     * @throws SQLException
     *             if a databvase access error occurs
     */
    public String getColumnName(int column) throws SQLException {
        if (this.useOldAliasBehavior) {
            return getField(column).getName();
        }

        String name = getField(column).getOriginalName();

        if (name == null) {
            return getField(column).getName();
        }

        return name;
    }

    /**
     * What is a column's SQL Type? (java.sql.Type int)
     * 
     * @param column
     *            the first column is 1, the second is 2, etc.
     * 
     * @return the java.sql.Type value
     * 
     * @throws SQLException
     *             if a database access error occurs
     */
    public int getColumnType(int column) throws SQLException {
        return getField(column).getJavaType();
    }

    /**
     * Whats is the column's data source specific type name?
     * 
     * @param column
     *            the first column is 1, the second is 2, etc.
     * 
     * @return the type name
     * 
     * @throws SQLException
     *             if a database access error occurs
     */
    public String getColumnTypeName(int column) throws java.sql.SQLException {
        Field field = getField(column);
        return field.getMysqlType().getName();
    }

    /**
     * Returns the field instance for the given column index
     * 
     * @param columnIndex
     *            the column number to retrieve a field instance for
     * 
     * @return the field instance for the given column index
     * 
     * @throws SQLException
     *             if an error occurs
     */
    protected Field getField(int columnIndex) throws SQLException {
        if ((columnIndex < 1) || (columnIndex > this.fields.length)) {
            throw SQLError.createSQLException(Messages.getString("ResultSetMetaData.46"), MysqlErrorNumbers.SQL_STATE_INVALID_COLUMN_NUMBER,
                    this.exceptionInterceptor);
        }

        return this.fields[columnIndex - 1];
    }

    /**
     * What is a column's number of decimal digits.
     * 
     * @param column
     *            the first column is 1, the second is 2...
     * 
     * @return the precision
     * 
     * @throws SQLException
     *             if a database access error occurs
     */
    public int getPrecision(int column) throws SQLException {
        Field f = getField(column);

        if (f.getMysqlType().isDecimal()) {
            if (f.getDecimals() > 0) {
                return clampedGetLength(f) - 1 + getPrecisionAdjustFactor(f);
            }

            return clampedGetLength(f) + getPrecisionAdjustFactor(f);
        }

        switch (f.getMysqlType()) {
            case TINYBLOB:
            case BLOB:
            case MEDIUMBLOB:
            case LONGBLOB:
                // for binary BLOBS returning the full length, *TEXT types are handled by default clause below
                return clampedGetLength(f);

            default:
                return clampedGetLength(f) / this.session.getServerSession().getMaxBytesPerChar(f.getCollationIndex(), f.getEncoding());

        }
    }

    /**
     * Returns amount of correction that should be applied to the precision
     * value.
     *
     * Different versions of MySQL report different precision values.
     *
     * @return the amount to adjust precision value by.
     */
    public int getPrecisionAdjustFactor(Field f) {
        //
        // Handle odd values for 'M' for floating point/decimal numbers
        //
        if (!f.isUnsigned()) {
            switch (f.getMysqlTypeId()) {
                case MysqlType.FIELD_TYPE_DECIMAL:
                case MysqlType.FIELD_TYPE_NEWDECIMAL:
                    return -1;

                case MysqlType.FIELD_TYPE_DOUBLE:
                case MysqlType.FIELD_TYPE_FLOAT:
                    return 1;

            }
        } else {
            switch (f.getMysqlTypeId()) {
                case MysqlType.FIELD_TYPE_DOUBLE:
                case MysqlType.FIELD_TYPE_FLOAT:
                    return 1;

            }
        }
        return 0;
    }

    /**
     * What is a column's number of digits to the right of the decimal point?
     * 
     * @param column
     *            the first column is 1, the second is 2...
     * 
     * @return the scale
     * 
     * @throws SQLException
     *             if a database access error occurs
     */
    public int getScale(int column) throws SQLException {
        Field f = getField(column);

        if (f.getMysqlType().isDecimal()) {
            return f.getDecimals();
        }

        return 0;
    }

    /**
     * What is a column's table's schema? This relies on us knowing the table
     * name. The JDBC specification allows us to return "" if this is not
     * applicable.
     * 
     * @param column
     *            the first column is 1, the second is 2...
     * 
     * @return the Schema
     * 
     * @throws SQLException
     *             if a database access error occurs
     */
    public String getSchemaName(int column) throws SQLException {
        return "";
    }

    /**
     * Whats a column's table's name?
     * 
     * @param column
     *            the first column is 1, the second is 2...
     * 
     * @return column name, or "" if not applicable
     * 
     * @throws SQLException
     *             if a database access error occurs
     */
    public String getTableName(int column) throws SQLException {
        if (this.useOldAliasBehavior) {
            return getField(column).getTableName();
        }

        return getField(column).getOriginalTableName();
    }

    /**
     * Is the column automatically numbered (and thus read-only)
     * 
     * @param column
     *            the first column is 1, the second is 2...
     * 
     * @return true if so
     * 
     * @throws SQLException
     *             if a database access error occurs
     */
    public boolean isAutoIncrement(int column) throws SQLException {
        Field f = getField(column);

        return f.isAutoIncrement();
    }

    /**
     * Does a column's case matter?
     * 
     * @param column
     *            the first column is 1, the second is 2...
     * 
     * @return true if so
     * 
     * @throws java.sql.SQLException
     *             if a database access error occurs
     */
    public boolean isCaseSensitive(int column) throws java.sql.SQLException {
        Field field = getField(column);

        switch (field.getMysqlType()) {
            case BIT:
            case TINYINT:
            case TINYINT_UNSIGNED:
            case SMALLINT:
            case SMALLINT_UNSIGNED:
            case INT:
            case INT_UNSIGNED:
            case MEDIUMINT:
            case MEDIUMINT_UNSIGNED:
            case BIGINT:
            case BIGINT_UNSIGNED:
            case FLOAT:
            case FLOAT_UNSIGNED:
            case DOUBLE:
            case DOUBLE_UNSIGNED:
            case DATE:
            case YEAR:
            case TIME:
            case TIMESTAMP:
            case DATETIME:
                return false;

            case CHAR:
            case VARCHAR:
            case TINYTEXT:
            case TEXT:
            case MEDIUMTEXT:
            case LONGTEXT:
            case JSON:
            case ENUM:
            case SET:
                String collationName = CharsetMapping.COLLATION_INDEX_TO_COLLATION_NAME[field.getCollationIndex()];
                return ((collationName != null) && !collationName.endsWith("_ci"));

            default:
                return true;
        }
    }

    /**
     * Is the column a cash value?
     * 
     * @param column
     *            the first column is 1, the second is 2...
     * 
     * @return true if its a cash column
     * 
     * @throws SQLException
     *             if a database access error occurs
     */
    public boolean isCurrency(int column) throws SQLException {
        return false;
    }

    /**
     * Will a write on this column definately succeed?
     * 
     * @param column
     *            the first column is 1, the second is 2, etc..
     * 
     * @return true if so
     * 
     * @throws SQLException
     *             if a database access error occurs
     */
    public boolean isDefinitelyWritable(int column) throws SQLException {
        return isWritable(column);
    }

    /**
     * Can you put a NULL in this column?
     * 
     * @param column
     *            the first column is 1, the second is 2...
     * 
     * @return one of the columnNullable values
     * 
     * @throws SQLException
     *             if a database access error occurs
     */
    public int isNullable(int column) throws SQLException {
        if (!getField(column).isNotNull()) {
            return java.sql.ResultSetMetaData.columnNullable;
        }

        return java.sql.ResultSetMetaData.columnNoNulls;
    }

    /**
     * Is the column definitely not writable?
     * 
     * @param column
     *            the first column is 1, the second is 2, etc.
     * 
     * @return true if so
     * 
     * @throws SQLException
     *             if a database access error occurs
     */
    public boolean isReadOnly(int column) throws SQLException {
        return getField(column).isReadOnly();
    }

    /**
     * Can the column be used in a WHERE clause? Basically for this, I split the
     * functions into two types: recognised types (which are always useable),
     * and OTHER types (which may or may not be useable). The OTHER types, for
     * now, I will assume they are useable. We should really query the catalog
     * to see if they are useable.
     * 
     * @param column
     *            the first column is 1, the second is 2...
     * 
     * @return true if they can be used in a WHERE clause
     * 
     * @throws SQLException
     *             if a database access error occurs
     */
    public boolean isSearchable(int column) throws SQLException {
        return true;
    }

    /**
     * Is the column a signed number?
     * 
     * @param column
     *            the first column is 1, the second is 2...
     * 
     * @return true if so
     * 
     * @throws SQLException
     *             if a database access error occurs
     */
    public boolean isSigned(int column) throws SQLException {
        return MysqlType.isSigned(getField(column).getMysqlType());
    }

    /**
     * Is it possible for a write on the column to succeed?
     * 
     * @param column
     *            the first column is 1, the second is 2, etc.
     * 
     * @return true if so
     * 
     * @throws SQLException
     *             if a database access error occurs
     */
    public boolean isWritable(int column) throws SQLException {
        return !isReadOnly(column);
    }

    /**
     * Returns a string representation of this object
     * 
     * @return ...
     */
    @Override
    public String toString() {
        StringBuilder toStringBuf = new StringBuilder();
        toStringBuf.append(super.toString());
        toStringBuf.append(" - Field level information: ");

        for (int i = 0; i < this.fields.length; i++) {
            toStringBuf.append("\n\t");
            toStringBuf.append(this.fields[i].toString());
        }

        return toStringBuf.toString();
    }

    /**
     * @see java.sql.Wrapper#isWrapperFor(Class)
     */
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        // This works for classes that aren't actually wrapping anything
        return iface.isInstance(this);
    }

    /**
     * @see java.sql.Wrapper#unwrap(Class)
     */
    public <T> T unwrap(Class<T> iface) throws SQLException {
        try {
            // This works for classes that aren't actually wrapping anything
            return iface.cast(this);
        } catch (ClassCastException cce) {
            throw SQLError.createSQLException(Messages.getString("Common.UnableToUnwrap", new Object[] { iface.toString() }),
                    MysqlErrorNumbers.SQL_STATE_ILLEGAL_ARGUMENT, this.exceptionInterceptor);
        }
    }

    public Field[] getFields() {
        return this.fields;
    }
}
