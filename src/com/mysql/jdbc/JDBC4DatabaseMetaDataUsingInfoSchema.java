/*
  Copyright (c) 2002, 2013, Oracle and/or its affiliates. All rights reserved.

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

import java.sql.ResultSet;
import java.sql.RowIdLifetime;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;

import java.util.List;

import com.mysql.jdbc.Field;

public class JDBC4DatabaseMetaDataUsingInfoSchema extends DatabaseMetaDataUsingInfoSchema {
	public JDBC4DatabaseMetaDataUsingInfoSchema(MySQLConnection connToSet, String databaseToSet) throws SQLException {
		super(connToSet, databaseToSet);
	}

	public RowIdLifetime getRowIdLifetime() throws SQLException {
		return RowIdLifetime.ROWID_UNSUPPORTED;
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
    public <T> T unwrap(java.lang.Class<T> iface) throws java.sql.SQLException {
    	try {
    		// This works for classes that aren't actually wrapping
    		// anything
            return iface.cast(this);
        } catch (ClassCastException cce) {
            throw SQLError.createSQLException("Unable to unwrap to " + iface.toString(), 
            		SQLError.SQL_STATE_ILLEGAL_ARGUMENT, this.conn.getExceptionInterceptor());
        }
    }

	/**
	 * Redirects to another implementation of #getProcedureColumns. Overrides
	 * DatabaseMetaDataUsingInfoSchema#getProcedureColumnsNoISParametersView.
	 * 
	 * @see DatabaseMetaDataUsingInfoSchema#getProcedureColumns
	 * @see DatabaseMetaDataUsingInfoSchema#getProcedureColumnsNoISParametersView
	 */
	protected ResultSet getProcedureColumnsNoISParametersView(String catalog, String schemaPattern,
			String procedureNamePattern, String columnNamePattern) throws SQLException {
		Field[] fields = createProcedureColumnsFields();

		return getProcedureOrFunctionColumns(fields, catalog, schemaPattern, procedureNamePattern, columnNamePattern,
				true, conn.getGetProceduresReturnsFunctions());
	}
    
	/**
	 * Returns a condition to be injected in the query that returns metadata for procedures only. Overrides
	 * DatabaseMetaDataUsingInfoSchema#injectRoutineTypeConditionForGetProcedures. When not empty must end with "AND ".
	 * 
	 * @return String with the condition to be injected.
	 */
	protected String getRoutineTypeConditionForGetProcedures() {
		return conn.getGetProceduresReturnsFunctions() ? "" : "ROUTINE_TYPE = 'PROCEDURE' AND ";
	}

	/**
	 * Returns a condition to be injected in the query that returns metadata for procedure columns only. Overrides
	 * DatabaseMetaDataUsingInfoSchema#injectRoutineTypeConditionForGetProcedureColumns. When not empty must end with
	 * "AND ".
	 * 
	 * @return String with the condition to be injected.
	 */
	protected String getRoutineTypeConditionForGetProcedureColumns() {
		return conn.getGetProceduresReturnsFunctions() ? "" : "ROUTINE_TYPE = 'PROCEDURE' AND ";
	}
	
	/**
	 * Overrides DatabaseMetaDataUsingInfoSchema#getJDBC4FunctionConstant.
	 * 
	 * @param constant
	 *            the constant id from DatabaseMetaData fields to return.
	 * 
	 * @return one of the java.sql.DatabaseMetaData#function* fields.
	 */
	protected int getJDBC4FunctionConstant(JDBC4FunctionConstant constant) {
		switch (constant) {
			case FUNCTION_COLUMN_IN:
				return functionColumnIn;
			case FUNCTION_COLUMN_INOUT:
				return functionColumnInOut;
			case FUNCTION_COLUMN_OUT:
				return functionColumnOut;
			case FUNCTION_COLUMN_RETURN:
				return functionReturn;
			case FUNCTION_COLUMN_RESULT:
				return functionColumnResult;
			case FUNCTION_COLUMN_UNKNOWN:
				return functionColumnUnknown;
			case FUNCTION_NO_NULLS:
				return functionNoNulls;
			case FUNCTION_NULLABLE:
				return functionNullable;
			case FUNCTION_NULLABLE_UNKNOWN:
				return functionNullableUnknown;
			default:
				return -1;
		}
	}

	/**
	 * Overrides DatabaseMetaDataUsingInfoSchema#getJDBC4FunctionNoTableConstant.
	 * 
	 * @return java.sql.DatabaseMetaData#functionNoTable.
	 */
	protected int getJDBC4FunctionNoTableConstant() {
		return functionNoTable;
	}
	
	/**
	 * Overrides DatabaseMetaData#getColumnType(boolean, boolean, boolean, boolean).
	 * 
	 * @see JDBC4DatabaseMetaData#getProcedureOrFunctionColumnType(boolean, boolean, boolean, boolean)
	 */
	protected int getColumnType(boolean isOutParam, boolean isInParam, boolean isReturnParam,
			boolean forGetFunctionColumns) {
		return JDBC4DatabaseMetaData.getProcedureOrFunctionColumnType(isOutParam, isInParam, isReturnParam,
				forGetFunctionColumns);
	}
}
