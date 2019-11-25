/*
 * Copyright (c) 2002, 2019, Oracle and/or its affiliates. All rights reserved.
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
import com.mysql.cj.NativeSession;
import com.mysql.cj.Session;
import com.mysql.cj.conf.PropertyDefinitions.DatabaseTerm;
import com.mysql.cj.conf.PropertyKey;
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
     * @param session
     *            this {@link Session}
     * 
     * @param fields
     *            the array of field descriptors
     * @param useOldAliasBehavior
     *            'useOldAliasMetadataBehavior' property value
     * @param treatYearAsDate
     *            'yearIsDateType' property value
     * @param exceptionInterceptor
     *            exception interceptor
     */
    public ResultSetMetaData(Session session, Field[] fields, boolean useOldAliasBehavior, boolean treatYearAsDate, ExceptionInterceptor exceptionInterceptor) {
        this.session = session;
        this.fields = fields;
        this.useOldAliasBehavior = useOldAliasBehavior;
        this.treatYearAsDate = treatYearAsDate;
        this.exceptionInterceptor = exceptionInterceptor;
    }

    @Override
    public String getCatalogName(int column) throws SQLException {
        if (this.session.getPropertySet().<DatabaseTerm>getEnumProperty(PropertyKey.databaseTerm).getValue() == DatabaseTerm.SCHEMA) {
            return "";
        }
        String database = getField(column).getDatabaseName();
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
        return getField(column).getEncoding();
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
        int index = getField(column).getCollationIndex();

        String charsetName = null;

        if (((NativeSession) this.session).getProtocol().getServerSession().indexToCustomMysqlCharset != null) {
            charsetName = ((NativeSession) this.session).getProtocol().getServerSession().indexToCustomMysqlCharset.get(index);
        }
        if (charsetName == null) {
            charsetName = CharsetMapping.getMysqlCharsetNameForCollationIndex(index);
        }

        return charsetName;
    }

    @Override
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

    @Override
    public int getColumnCount() throws SQLException {
        return this.fields.length;
    }

    @Override
    public int getColumnDisplaySize(int column) throws SQLException {
        Field f = getField(column);

        int lengthInBytes = clampedGetLength(f);

        return lengthInBytes / this.session.getServerSession().getMaxBytesPerChar(f.getCollationIndex(), f.getEncoding());
    }

    @Override
    public String getColumnLabel(int column) throws SQLException {
        if (this.useOldAliasBehavior) {
            return getColumnName(column);
        }

        return getField(column).getColumnLabel();
    }

    @Override
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

    @Override
    public int getColumnType(int column) throws SQLException {
        return getField(column).getJavaType();
    }

    @Override
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

    @Override
    public int getPrecision(int column) throws SQLException {
        Field f = getField(column);

        switch (f.getMysqlType()) {
            case TINYBLOB:
            case BLOB:
            case MEDIUMBLOB:
            case LONGBLOB:
                // for binary BLOBS returning the full length, *TEXT types are handled by default clause below
                return clampedGetLength(f);

            default:
                return f.getMysqlType().isDecimal() ? clampedGetLength(f)
                        : clampedGetLength(f) / this.session.getServerSession().getMaxBytesPerChar(f.getCollationIndex(), f.getEncoding());

        }
    }

    @Override
    public int getScale(int column) throws SQLException {
        Field f = getField(column);

        if (f.getMysqlType().isDecimal()) {
            return f.getDecimals();
        }

        return 0;
    }

    @Override
    public String getSchemaName(int column) throws SQLException {
        if (this.session.getPropertySet().<DatabaseTerm>getEnumProperty(PropertyKey.databaseTerm).getValue() == DatabaseTerm.CATALOG) {
            return "";
        }
        String database = getField(column).getDatabaseName();
        return (database == null) ? "" : database;
    }

    @Override
    public String getTableName(int column) throws SQLException {
        String res = this.useOldAliasBehavior ? getField(column).getTableName() : getField(column).getOriginalTableName();
        return res == null ? "" : res;
    }

    @Override
    public boolean isAutoIncrement(int column) throws SQLException {
        Field f = getField(column);

        return f.isAutoIncrement();
    }

    @Override
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

    @Override
    public boolean isCurrency(int column) throws SQLException {
        return false;
    }

    @Override
    public boolean isDefinitelyWritable(int column) throws SQLException {
        return isWritable(column);
    }

    @Override
    public int isNullable(int column) throws SQLException {
        if (!getField(column).isNotNull()) {
            return java.sql.ResultSetMetaData.columnNullable;
        }

        return java.sql.ResultSetMetaData.columnNoNulls;
    }

    @Override
    public boolean isReadOnly(int column) throws SQLException {
        return getField(column).isReadOnly();
    }

    @Override
    public boolean isSearchable(int column) throws SQLException {
        return true;
    }

    @Override
    public boolean isSigned(int column) throws SQLException {
        return MysqlType.isSigned(getField(column).getMysqlType());
    }

    @Override
    public boolean isWritable(int column) throws SQLException {
        return !isReadOnly(column);
    }

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

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        // This works for classes that aren't actually wrapping anything
        return iface.isInstance(this);
    }

    @Override
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
