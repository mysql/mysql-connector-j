/*
  Copyright (c) 2002, 2014, Oracle and/or its affiliates. All rights reserved.

  The MySQL Connector/J is licensed under the terms of the GPLv2
  <http://www.gnu.org/licenses/old-licenses/gpl-2.0.html>, like most MySQL Connectors.
  There are special exceptions to the terms and conditions of the GPLv2 as it is applied to
  this software, see the FOSS License Exception
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

public class JDBC4DatabaseMetaData extends DatabaseMetaData {
    public JDBC4DatabaseMetaData(MySQLConnection connToSet, String databaseToSet) {
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
     * @param interfaces
     *            a Class defining an interface.
     * @return true if this implements the interface or directly or indirectly wraps an object that does.
     * @throws java.sql.SQLException
     *             if an error occurs while determining whether this is a wrapper
     *             for an object with the given interface.
     * @since 1.6
     */
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        // This works for classes that aren't actually wrapping anything
        return iface.isInstance(this);
    }

    /**
     * Returns an object that implements the given interface to allow access to non-standard methods,
     * or standard methods not exposed by the proxy.
     * The result may be either the object found to implement the interface or a proxy for that object.
     * If the receiver implements the interface then that is the object. If the receiver is a wrapper
     * and the wrapped object implements the interface then that is the object. Otherwise the object is
     * the result of calling <code>unwrap</code> recursively on the wrapped object. If the receiver is not a
     * wrapper and does not implement the interface, then an <code>SQLException</code> is thrown.
     * 
     * @param iface
     *            A Class defining an interface that the result must implement.
     * @return an object that implements the interface. May be a proxy for the actual implementing object.
     * @throws java.sql.SQLException
     *             If no object found that implements the interface
     * @since 1.6
     */
    public <T> T unwrap(java.lang.Class<T> iface) throws java.sql.SQLException {
        try {
            // This works for classes that aren't actually wrapping anything
            return iface.cast(this);
        } catch (ClassCastException cce) {
            throw SQLError.createSQLException("Unable to unwrap to " + iface.toString(), SQLError.SQL_STATE_ILLEGAL_ARGUMENT,
                    this.conn.getExceptionInterceptor());
        }
    }

    public boolean autoCommitFailureClosesAllResultSets() throws SQLException {
        return false;
    }

    /**
     * Changes in behavior introduced in JDBC4 when #getFunctionColumns became available. Overrides
     * DatabaseMetaData#getProcedureColumns
     * 
     * @see DatabaseMetaData#getProcedureColumns
     * @since 1.6
     */
    public java.sql.ResultSet getProcedureColumns(String catalog, String schemaPattern, String procedureNamePattern, String columnNamePattern)
            throws SQLException {
        Field[] fields = createProcedureColumnsFields();

        return getProcedureOrFunctionColumns(fields, catalog, schemaPattern, procedureNamePattern, columnNamePattern, true,
                conn.getGetProceduresReturnsFunctions());
    }

    /**
     * Changes in behavior introduced in JDBC4 when #getFunctions became available. Overrides
     * DatabaseMetaData#getProcedures.
     * 
     * @see DatabaseMetaData#getProcedures
     * @since 1.6
     */
    public java.sql.ResultSet getProcedures(String catalog, String schemaPattern, String procedureNamePattern) throws SQLException {
        Field[] fields = createFieldMetadataForGetProcedures();

        return getProceduresAndOrFunctions(fields, catalog, schemaPattern, procedureNamePattern, true, conn.getGetProceduresReturnsFunctions());
    }

    /**
     * Overrides DatabaseMetaData#getJDBC4FunctionNoTableConstant.
     * 
     * @return java.sql.DatabaseMetaData#functionNoTable
     */
    protected int getJDBC4FunctionNoTableConstant() {
        return functionNoTable;
    }

    /**
     * This method overrides DatabaseMetaData#getColumnType(boolean, boolean, boolean, boolean).
     * 
     * @see JDBC4DatabaseMetaData#getProcedureOrFunctionColumnType(boolean, boolean, boolean, boolean)
     */
    protected int getColumnType(boolean isOutParam, boolean isInParam, boolean isReturnParam, boolean forGetFunctionColumns) {
        return JDBC4DatabaseMetaData.getProcedureOrFunctionColumnType(isOutParam, isInParam, isReturnParam, forGetFunctionColumns);
    }

    /**
     * Determines the COLUMN_TYPE information based on parameter type (IN, OUT or INOUT) or function return parameter.
     * 
     * @param isOutParam
     *            Indicates whether it's an output parameter.
     * @param isInParam
     *            Indicates whether it's an input parameter.
     * @param isReturnParam
     *            Indicates whether it's a function return parameter.
     * @param forGetFunctionColumns
     *            Indicates whether the column belong to a function.
     * 
     * @return The corresponding COLUMN_TYPE as in java.sql.getProcedureColumns API.
     */
    protected static int getProcedureOrFunctionColumnType(boolean isOutParam, boolean isInParam, boolean isReturnParam, boolean forGetFunctionColumns) {

        if (isInParam && isOutParam) {
            return forGetFunctionColumns ? functionColumnInOut : procedureColumnInOut;
        } else if (isInParam) {
            return forGetFunctionColumns ? functionColumnIn : procedureColumnIn;
        } else if (isOutParam) {
            return forGetFunctionColumns ? functionColumnOut : procedureColumnOut;
        } else if (isReturnParam) {
            return forGetFunctionColumns ? functionReturn : procedureColumnReturn;
        } else {
            return forGetFunctionColumns ? functionColumnUnknown : procedureColumnUnknown;
        }
    }
}
