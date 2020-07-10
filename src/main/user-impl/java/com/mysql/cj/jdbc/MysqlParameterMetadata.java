/*
 * Copyright (c) 2005, 2020, Oracle and/or its affiliates.
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

package com.mysql.cj.jdbc;

import java.sql.ParameterMetaData;
import java.sql.SQLException;

import com.mysql.cj.Messages;
import com.mysql.cj.MysqlType;
import com.mysql.cj.Session;
import com.mysql.cj.exceptions.ExceptionInterceptor;
import com.mysql.cj.exceptions.MysqlErrorNumbers;
import com.mysql.cj.jdbc.exceptions.SQLError;
import com.mysql.cj.jdbc.result.ResultSetMetaData;
import com.mysql.cj.result.Field;

public class MysqlParameterMetadata implements ParameterMetaData {
    boolean returnSimpleMetadata = false;

    ResultSetMetaData metadata = null;

    int parameterCount = 0;

    private ExceptionInterceptor exceptionInterceptor;

    public MysqlParameterMetadata(Session session, Field[] fieldInfo, int parameterCount, ExceptionInterceptor exceptionInterceptor) {
        this.metadata = new ResultSetMetaData(session, fieldInfo, false, true, exceptionInterceptor);

        this.parameterCount = parameterCount;
        this.exceptionInterceptor = exceptionInterceptor;
    }

    /**
     * Used for "fake" basic metadata for client-side prepared statements when
     * we don't know the parameter types.
     * 
     * @param count
     *            parameters number
     */
    MysqlParameterMetadata(int count) {
        this.parameterCount = count;
        this.returnSimpleMetadata = true;
    }

    @Override
    public int getParameterCount() throws SQLException {
        return this.parameterCount;
    }

    @Override
    public int isNullable(int arg0) throws SQLException {
        checkAvailable();

        return this.metadata.isNullable(arg0);
    }

    private void checkAvailable() throws SQLException {
        if (this.metadata == null || this.metadata.getFields() == null) {
            throw SQLError.createSQLException(Messages.getString("MysqlParameterMetadata.0"), MysqlErrorNumbers.SQL_STATE_DRIVER_NOT_CAPABLE,
                    this.exceptionInterceptor);
        }
    }

    @Override
    public boolean isSigned(int arg0) throws SQLException {
        if (this.returnSimpleMetadata) {
            checkBounds(arg0);

            return false;
        }

        checkAvailable();

        return (this.metadata.isSigned(arg0));
    }

    @Override
    public int getPrecision(int arg0) throws SQLException {
        if (this.returnSimpleMetadata) {
            checkBounds(arg0);

            return 0;
        }

        checkAvailable();

        return (this.metadata.getPrecision(arg0));
    }

    @Override
    public int getScale(int arg0) throws SQLException {
        if (this.returnSimpleMetadata) {
            checkBounds(arg0);

            return 0;
        }

        checkAvailable();

        return (this.metadata.getScale(arg0));
    }

    @Override
    public int getParameterType(int arg0) throws SQLException {
        if (this.returnSimpleMetadata) {
            checkBounds(arg0);

            return MysqlType.VARCHAR.getJdbcType();
        }

        checkAvailable();

        return (this.metadata.getColumnType(arg0));
    }

    @Override
    public String getParameterTypeName(int arg0) throws SQLException {
        if (this.returnSimpleMetadata) {
            checkBounds(arg0);

            return MysqlType.VARCHAR.getName();
        }

        checkAvailable();

        return (this.metadata.getColumnTypeName(arg0));
    }

    @Override
    public String getParameterClassName(int arg0) throws SQLException {
        if (this.returnSimpleMetadata) {
            checkBounds(arg0);

            return "java.lang.String";
        }

        checkAvailable();

        return (this.metadata.getColumnClassName(arg0));
    }

    @Override
    public int getParameterMode(int arg0) throws SQLException {
        return parameterModeIn;
    }

    private void checkBounds(int paramNumber) throws SQLException {
        if (paramNumber < 1) {
            throw SQLError.createSQLException(Messages.getString("MysqlParameterMetadata.1", new Object[] { paramNumber }),
                    MysqlErrorNumbers.SQL_STATE_ILLEGAL_ARGUMENT, this.exceptionInterceptor);
        }

        if (paramNumber > this.parameterCount) {
            throw SQLError.createSQLException(Messages.getString("MysqlParameterMetadata.2", new Object[] { paramNumber, this.parameterCount }),
                    MysqlErrorNumbers.SQL_STATE_ILLEGAL_ARGUMENT, this.exceptionInterceptor);

        }
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
}
