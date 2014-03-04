/*
  Copyright (c) 2002, 2014, Oracle and/or its affiliates. All rights reserved.

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

package com.mysql.jdbc;

import java.sql.SQLException;
import java.sql.Types;

/**
 * A ResultSetMetaData object can be used to find out about the types and
 * properties of the columns in a ResultSet
 * 
 * @author Mark Matthews
 * @version $Id: ResultSetMetaData.java,v 1.1.2.1 2005/05/13 18:58:38 mmatthews
 *          Exp $
 * 
 * @see java.sql.ResultSetMetaData
 */
public class ResultSetMetaData implements java.sql.ResultSetMetaData {
	private static int clampedGetLength(Field f) {
		long fieldLength = f.getLength();

		if (fieldLength > Integer.MAX_VALUE) {
			fieldLength = Integer.MAX_VALUE;
		}

		return (int) fieldLength;
	}

	/**
	 * Checks if the SQL Type is a Decimal/Number Type
	 * 
	 * @param type
	 *            SQL Type
	 * 
	 * @return ...
	 */
	private static final boolean isDecimalType(int type) {
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

	Field[] fields;
	boolean useOldAliasBehavior = false;
	boolean treatYearAsDate = true;

	private ExceptionInterceptor exceptionInterceptor;
	
	/**
	 * Initialize for a result with a tuple set and a field descriptor set
	 * 
	 * @param fields
	 *            the array of field descriptors
	 */
	public ResultSetMetaData(Field[] fields, boolean useOldAliasBehavior, boolean treatYearAsDate, ExceptionInterceptor exceptionInterceptor) {
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

		return (database == null) ? "" : database; //$NON-NLS-1$
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
				javaName = CharsetMapping.MYSQL_TO_JAVA_CHARSET_MAP.get(mysqlName);
			} catch (RuntimeException ex) {
				SQLException sqlEx = SQLError.createSQLException(ex.toString(), SQLError.SQL_STATE_ILLEGAL_ARGUMENT, null);
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
		return getField(column).getCharacterSet();
	}

	// --------------------------JDBC 2.0-----------------------------------

	/**
	 * JDBC 2.0
	 * 
	 * <p>
	 * Return the fully qualified name of the Java class whose instances are
	 * manufactured if ResultSet.getObject() is called to retrieve a value from
	 * the column. ResultSet.getObject() may return a subClass of the class
	 * returned by this method.
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

		return getClassNameForJavaType(f.getSQLType(), 
				f.isUnsigned(), 
				f.getMysqlType(), 
				f.isBinary() || f.isBlob(),
				f.isOpaqueBinary(), treatYearAsDate); 
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

		return lengthInBytes / f.getMaxBytesPerCharacter();
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

		String name = getField(column).getNameNoAliases();
		
		if (name != null && name.length() == 0) {
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
	 * 
	 * @see java.sql.Types
	 */
	public int getColumnType(int column) throws SQLException {
		return getField(column).getSQLType();
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

		int mysqlType = field.getMysqlType();
		int jdbcType = field.getSQLType();

		switch (mysqlType) {
		case MysqlDefs.FIELD_TYPE_BIT:
			return "BIT";
		case MysqlDefs.FIELD_TYPE_DECIMAL:
		case MysqlDefs.FIELD_TYPE_NEW_DECIMAL:
			return field.isUnsigned() ? "DECIMAL UNSIGNED" : "DECIMAL";

		case MysqlDefs.FIELD_TYPE_TINY:
			return field.isUnsigned() ? "TINYINT UNSIGNED" : "TINYINT";

		case MysqlDefs.FIELD_TYPE_SHORT:
			return field.isUnsigned() ? "SMALLINT UNSIGNED" : "SMALLINT";

		case MysqlDefs.FIELD_TYPE_LONG:
			return field.isUnsigned() ? "INT UNSIGNED" : "INT";

		case MysqlDefs.FIELD_TYPE_FLOAT:
			return field.isUnsigned() ? "FLOAT UNSIGNED" : "FLOAT";

		case MysqlDefs.FIELD_TYPE_DOUBLE:
			return field.isUnsigned() ? "DOUBLE UNSIGNED" : "DOUBLE";

		case MysqlDefs.FIELD_TYPE_NULL:
			return "NULL"; //$NON-NLS-1$

		case MysqlDefs.FIELD_TYPE_TIMESTAMP:
			return "TIMESTAMP"; //$NON-NLS-1$       

		case MysqlDefs.FIELD_TYPE_LONGLONG:
			return field.isUnsigned() ? "BIGINT UNSIGNED" : "BIGINT";

		case MysqlDefs.FIELD_TYPE_INT24:
			return field.isUnsigned() ? "MEDIUMINT UNSIGNED" : "MEDIUMINT";

		case MysqlDefs.FIELD_TYPE_DATE:
			return "DATE"; //$NON-NLS-1$       

		case MysqlDefs.FIELD_TYPE_TIME:
			return "TIME"; //$NON-NLS-1$       

		case MysqlDefs.FIELD_TYPE_DATETIME:
			return "DATETIME"; //$NON-NLS-1$      

		case MysqlDefs.FIELD_TYPE_TINY_BLOB:
			return "TINYBLOB"; //$NON-NLS-1$

		case MysqlDefs.FIELD_TYPE_MEDIUM_BLOB:
			return "MEDIUMBLOB"; //$NON-NLS-1$

		case MysqlDefs.FIELD_TYPE_LONG_BLOB:
			return "LONGBLOB"; //$NON-NLS-1$

		case MysqlDefs.FIELD_TYPE_BLOB:
			if (getField(column).isBinary()) {
				return "BLOB";//$NON-NLS-1$
			}

			return "TEXT";//$NON-NLS-1$

		case MysqlDefs.FIELD_TYPE_VARCHAR:
			return "VARCHAR"; //$NON-NLS-1$

		case MysqlDefs.FIELD_TYPE_VAR_STRING:
			if (jdbcType == Types.VARBINARY) {
				return "VARBINARY";
			}
			
			return "VARCHAR"; //$NON-NLS-1$

		case MysqlDefs.FIELD_TYPE_STRING:
			if (jdbcType == Types.BINARY) {
				return "BINARY";
			}
			
			return "CHAR"; //$NON-NLS-1$

		case MysqlDefs.FIELD_TYPE_ENUM:
			return "ENUM"; //$NON-NLS-1$

		case MysqlDefs.FIELD_TYPE_YEAR:
			return "YEAR"; // $NON_NLS-1$

		case MysqlDefs.FIELD_TYPE_SET:
			return "SET"; //$NON-NLS-1$
			
		case MysqlDefs.FIELD_TYPE_GEOMETRY:
			return "GEOMETRY"; //$NON-NLS-1$
			
		default:
			return "UNKNOWN"; //$NON-NLS-1$
		}
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
			throw SQLError.createSQLException(Messages.getString("ResultSetMetaData.46"), //$NON-NLS-1$
					SQLError.SQL_STATE_INVALID_COLUMN_NUMBER, this.exceptionInterceptor);
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

		// if (f.getMysqlType() == MysqlDefs.FIELD_TYPE_NEW_DECIMAL) {
		// return f.getLength();
		// }

		if (isDecimalType(f.getSQLType())) {
			if (f.getDecimals() > 0) {
				return clampedGetLength(f) - 1 + f.getPrecisionAdjustFactor();
			}

			return clampedGetLength(f) + f.getPrecisionAdjustFactor();
		}

		switch (f.getMysqlType()) {
		case MysqlDefs.FIELD_TYPE_TINY_BLOB:
		case MysqlDefs.FIELD_TYPE_BLOB:
		case MysqlDefs.FIELD_TYPE_MEDIUM_BLOB:
		case MysqlDefs.FIELD_TYPE_LONG_BLOB:
			return clampedGetLength(f); // this may change in the future
		// for now, the server only
		// returns FIELD_TYPE_BLOB for _all_
		// BLOB types, but varying lengths
		// indicating the _maximum_ size
		// for each BLOB type.
		default:
			return clampedGetLength(f) / f.getMaxBytesPerCharacter();

		}
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

		if (isDecimalType(f.getSQLType())) {
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
		return ""; //$NON-NLS-1$
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
		
		return getField(column).getTableNameNoAliases();
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

		int sqlType = field.getSQLType();

		switch (sqlType) {
		case Types.BIT:
		case Types.TINYINT:
		case Types.SMALLINT:
		case Types.INTEGER:
		case Types.BIGINT:
		case Types.FLOAT:
		case Types.REAL:
		case Types.DOUBLE:
		case Types.DATE:
		case Types.TIME:
		case Types.TIMESTAMP:
			return false;

		case Types.CHAR:
		case Types.VARCHAR:
		case Types.LONGVARCHAR:

			if (field.isBinary()) {
				return true;
			}

			String collationName = field.getCollation();

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
		Field f = getField(column);
		int sqlType = f.getSQLType();

		switch (sqlType) {
		case Types.TINYINT:
		case Types.SMALLINT:
		case Types.INTEGER:
		case Types.BIGINT:
		case Types.FLOAT:
		case Types.REAL:
		case Types.DOUBLE:
		case Types.NUMERIC:
		case Types.DECIMAL:
			return !f.isUnsigned();

		case Types.DATE:
		case Types.TIME:
		case Types.TIMESTAMP:
			return false;

		default:
			return false;
		}
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
	public String toString() {
		StringBuffer toStringBuf = new StringBuffer();
		toStringBuf.append(super.toString());
		toStringBuf.append(" - Field level information: "); //$NON-NLS-1$

		for (int i = 0; i < this.fields.length; i++) {
			toStringBuf.append("\n\t"); //$NON-NLS-1$
			toStringBuf.append(this.fields[i].toString());
		}

		return toStringBuf.toString();
	}

	static String getClassNameForJavaType(int javaType, boolean isUnsigned, int mysqlTypeIfKnown,
			boolean isBinaryOrBlob, boolean isOpaqueBinary, boolean treatYearAsDate) {
		switch (javaType) {
		case Types.BIT:
		case Types.BOOLEAN:
			return "java.lang.Boolean";

		case Types.TINYINT:

			if (isUnsigned) {
				return "java.lang.Integer";
			}

			return "java.lang.Integer";

		case Types.SMALLINT:

			if (isUnsigned) {
				return "java.lang.Integer";
			}

			return "java.lang.Integer";

		case Types.INTEGER:

			if (!isUnsigned || 
					mysqlTypeIfKnown == MysqlDefs.FIELD_TYPE_INT24) {
				return "java.lang.Integer";
			}

			return "java.lang.Long";

		case Types.BIGINT:

			if (!isUnsigned) {
				return "java.lang.Long";
			}

			return "java.math.BigInteger";

		case Types.DECIMAL:
		case Types.NUMERIC:
			return "java.math.BigDecimal";

		case Types.REAL:
			return "java.lang.Float";

		case Types.FLOAT:
		case Types.DOUBLE:
			return "java.lang.Double";

		case Types.CHAR:
		case Types.VARCHAR:
		case Types.LONGVARCHAR:
			if (!isOpaqueBinary) {
				return "java.lang.String";
			}

			return "[B";

		case Types.BINARY:
		case Types.VARBINARY:
		case Types.LONGVARBINARY:

			if (mysqlTypeIfKnown == MysqlDefs.FIELD_TYPE_GEOMETRY) {
				return "[B";
			} else if (isBinaryOrBlob) {
				return "[B";
			} else {
				return "java.lang.String";
			}

		case Types.DATE:
				return (treatYearAsDate || mysqlTypeIfKnown != MysqlDefs.FIELD_TYPE_YEAR) ? "java.sql.Date"
						: "java.lang.Short";

		case Types.TIME:
			return "java.sql.Time";

		case Types.TIMESTAMP:
			return "java.sql.Timestamp";

		default:
			return "java.lang.Object";
		}
	}
	
	/**
     * Returns true if this either implements the interface argument or is directly or indirectly a wrapper
     * for an object that does. Returns false otherwise. If this implements the interface then return true,
     * else if this is a wrapper then return the result of recursively calling <code>isWrapperFor</code> on the wrapped
     * object. If this does not implement the interface and is not a wrapper, return false.
     * This method should be implemented as a low-cost operation compared to <code>unwrap</code> so that
     * callers can use this method to avoid expensive <code>unwrap</code> calls that may fail. If this method
     * returns true then calling <code>unwrap</code> with the same argument should succeed.
     *
     * @param interfaces a Class defining an interface.
     * @return true if this implements the interface or directly or indirectly wraps an object that does.
     * @throws java.sql.SQLException  if an error occurs while determining whether this is a wrapper
     * for an object with the given interface.
     * @since 1.6
     */
	public boolean isWrapperFor(Class<?> iface) throws SQLException {
		// This works for classes that aren't actually wrapping
		// anything
		return iface.isInstance(this);
	}

    /**
     * Returns an object that implements the given interface to allow access to non-standard methods,
     * or standard methods not exposed by the proxy.
     * The result may be either the object found to implement the interface or a proxy for that object.
     * If the receiver implements the interface then that is the object. If the receiver is a wrapper
     * and the wrapped object implements the interface then that is the object. Otherwise the object is
     *  the result of calling <code>unwrap</code> recursively on the wrapped object. If the receiver is not a
     * wrapper and does not implement the interface, then an <code>SQLException</code> is thrown.
     *
     * @param iface A Class defining an interface that the result must implement.
     * @return an object that implements the interface. May be a proxy for the actual implementing object.
     * @throws java.sql.SQLException If no object found that implements the interface 
     * @since 1.6
     */
	public Object unwrap(Class<?> iface) throws java.sql.SQLException {
    	try {
    		// This works for classes that aren't actually wrapping
    		// anything
    		return Util.cast(iface, this);
        } catch (ClassCastException cce) {
            throw SQLError.createSQLException("Unable to unwrap to " + iface.toString(), 
            		SQLError.SQL_STATE_ILLEGAL_ARGUMENT, this.exceptionInterceptor);
        }
    }
}
