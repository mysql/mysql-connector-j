/*
  Copyright (c) 2005, 2016, Oracle and/or its affiliates. All rights reserved.

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

import java.sql.ParameterMetaData;
import java.sql.SQLException;
import java.sql.Types;

public class MysqlParameterMetadata implements ParameterMetaData {
    boolean returnSimpleMetadata = false;

    ResultSetMetaData metadata = null;

    int parameterCount = 0;

    private ExceptionInterceptor exceptionInterceptor;

    MysqlParameterMetadata(Field[] fieldInfo, int parameterCount, ExceptionInterceptor exceptionInterceptor) {
        this.metadata = new ResultSetMetaData(fieldInfo, false, true, exceptionInterceptor);

        this.parameterCount = parameterCount;
        this.exceptionInterceptor = exceptionInterceptor;
    }

    /**
     * Used for "fake" basic metadata for client-side prepared statements when
     * we don't know the parameter types.
     * 
     * @param parameterCount
     */
    MysqlParameterMetadata(int count) {
        this.parameterCount = count;
        this.returnSimpleMetadata = true;
    }

    public int getParameterCount() throws SQLException {
        return this.parameterCount;
    }

    public int isNullable(int arg0) throws SQLException {
        checkAvailable();

        return this.metadata.isNullable(arg0);
    }

    private void checkAvailable() throws SQLException {
        if (this.metadata == null || this.metadata.fields == null) {
            throw SQLError.createSQLException("Parameter metadata not available for the given statement", SQLError.SQL_STATE_DRIVER_NOT_CAPABLE,
                    this.exceptionInterceptor);
        }
    }

    public boolean isSigned(int arg0) throws SQLException {
        if (this.returnSimpleMetadata) {
            checkBounds(arg0);

            return false;
        }

        checkAvailable();

        return (this.metadata.isSigned(arg0));
    }

    public int getPrecision(int arg0) throws SQLException {
        if (this.returnSimpleMetadata) {
            checkBounds(arg0);

            return 0;
        }

        checkAvailable();

        return (this.metadata.getPrecision(arg0));
    }

    public int getScale(int arg0) throws SQLException {
        if (this.returnSimpleMetadata) {
            checkBounds(arg0);

            return 0;
        }

        checkAvailable();

        return (this.metadata.getScale(arg0));
    }

    public int getParameterType(int arg0) throws SQLException {
        if (this.returnSimpleMetadata) {
            checkBounds(arg0);

            return Types.VARCHAR;
        }

        checkAvailable();

        return (this.metadata.getColumnType(arg0));
    }

    public String getParameterTypeName(int arg0) throws SQLException {
        if (this.returnSimpleMetadata) {
            checkBounds(arg0);

            return "VARCHAR";
        }

        checkAvailable();

        return (this.metadata.getColumnTypeName(arg0));
    }

    public String getParameterClassName(int arg0) throws SQLException {
        if (this.returnSimpleMetadata) {
            checkBounds(arg0);

            return "java.lang.String";
        }

        checkAvailable();

        return (this.metadata.getColumnClassName(arg0));
    }

    public int getParameterMode(int arg0) throws SQLException {
        return parameterModeIn;
    }

    private void checkBounds(int paramNumber) throws SQLException {
        if (paramNumber < 1) {
            throw SQLError.createSQLException("Parameter index of '" + paramNumber + "' is invalid.", SQLError.SQL_STATE_ILLEGAL_ARGUMENT,
                    this.exceptionInterceptor);
        }

        if (paramNumber > this.parameterCount) {
            throw SQLError.createSQLException(
                    "Parameter index of '" + paramNumber + "' is greater than number of parameters, which is '" + this.parameterCount + "'.",
                    SQLError.SQL_STATE_ILLEGAL_ARGUMENT, this.exceptionInterceptor);

        }
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
    public <T> T unwrap(Class<T> iface) throws java.sql.SQLException {
        try {
            // This works for classes that aren't actually wrapping anything
            return iface.cast(this);
        } catch (ClassCastException cce) {
            throw SQLError.createSQLException("Unable to unwrap to " + iface.toString(), SQLError.SQL_STATE_ILLEGAL_ARGUMENT, this.exceptionInterceptor);
        }
    }
}
