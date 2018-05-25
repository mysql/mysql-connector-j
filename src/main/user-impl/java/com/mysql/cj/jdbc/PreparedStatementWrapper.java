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

package com.mysql.cj.jdbc;

import java.io.InputStream;
import java.io.Reader;
import java.lang.reflect.Proxy;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLType;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.HashMap;

import javax.sql.StatementEvent;

import com.mysql.cj.Messages;
import com.mysql.cj.exceptions.MysqlErrorNumbers;
import com.mysql.cj.jdbc.exceptions.SQLError;

/**
 * Wraps prepared statements so that errors can be reported correctly to ConnectionEventListeners.
 */
public class PreparedStatementWrapper extends StatementWrapper implements PreparedStatement {

    protected static PreparedStatementWrapper getInstance(ConnectionWrapper c, MysqlPooledConnection conn, PreparedStatement toWrap) throws SQLException {
        return new PreparedStatementWrapper(c, conn, toWrap);
    }

    PreparedStatementWrapper(ConnectionWrapper c, MysqlPooledConnection conn, PreparedStatement toWrap) {
        super(c, conn, toWrap);
    }

    @Override
    public void setArray(int parameterIndex, Array x) throws SQLException {
        try {
            if (this.wrappedStmt != null) {
                ((PreparedStatement) this.wrappedStmt).setArray(parameterIndex, x);
            } else {
                throw SQLError.createSQLException(Messages.getString("Statement.AlreadyClosed"), MysqlErrorNumbers.SQL_STATE_GENERAL_ERROR,
                        this.exceptionInterceptor);
            }
        } catch (SQLException sqlEx) {
            checkAndFireConnectionError(sqlEx);
        }
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, int length) throws SQLException {
        try {
            if (this.wrappedStmt != null) {
                ((PreparedStatement) this.wrappedStmt).setAsciiStream(parameterIndex, x, length);
            } else {
                throw SQLError.createSQLException(Messages.getString("Statement.AlreadyClosed"), MysqlErrorNumbers.SQL_STATE_GENERAL_ERROR,
                        this.exceptionInterceptor);
            }
        } catch (SQLException sqlEx) {
            checkAndFireConnectionError(sqlEx);
        }
    }

    @Override
    public void setBigDecimal(int parameterIndex, BigDecimal x) throws SQLException {
        try {
            if (this.wrappedStmt != null) {
                ((PreparedStatement) this.wrappedStmt).setBigDecimal(parameterIndex, x);
            } else {
                throw SQLError.createSQLException(Messages.getString("Statement.AlreadyClosed"), MysqlErrorNumbers.SQL_STATE_GENERAL_ERROR,
                        this.exceptionInterceptor);
            }
        } catch (SQLException sqlEx) {
            checkAndFireConnectionError(sqlEx);
        }
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, int length) throws SQLException {
        try {
            if (this.wrappedStmt != null) {
                ((PreparedStatement) this.wrappedStmt).setBinaryStream(parameterIndex, x, length);
            } else {
                throw SQLError.createSQLException(Messages.getString("Statement.AlreadyClosed"), MysqlErrorNumbers.SQL_STATE_GENERAL_ERROR,
                        this.exceptionInterceptor);
            }
        } catch (SQLException sqlEx) {
            checkAndFireConnectionError(sqlEx);
        }
    }

    @Override
    public void setBlob(int parameterIndex, Blob x) throws SQLException {
        try {
            if (this.wrappedStmt != null) {
                ((PreparedStatement) this.wrappedStmt).setBlob(parameterIndex, x);
            } else {
                throw SQLError.createSQLException(Messages.getString("Statement.AlreadyClosed"), MysqlErrorNumbers.SQL_STATE_GENERAL_ERROR,
                        this.exceptionInterceptor);
            }
        } catch (SQLException sqlEx) {
            checkAndFireConnectionError(sqlEx);
        }
    }

    @Override
    public void setBoolean(int parameterIndex, boolean x) throws SQLException {
        try {
            if (this.wrappedStmt != null) {
                ((PreparedStatement) this.wrappedStmt).setBoolean(parameterIndex, x);
            } else {
                throw SQLError.createSQLException(Messages.getString("Statement.AlreadyClosed"), MysqlErrorNumbers.SQL_STATE_GENERAL_ERROR,
                        this.exceptionInterceptor);
            }
        } catch (SQLException sqlEx) {
            checkAndFireConnectionError(sqlEx);
        }
    }

    @Override
    public void setByte(int parameterIndex, byte x) throws SQLException {
        try {
            if (this.wrappedStmt != null) {
                ((PreparedStatement) this.wrappedStmt).setByte(parameterIndex, x);
            } else {
                throw SQLError.createSQLException(Messages.getString("Statement.AlreadyClosed"), MysqlErrorNumbers.SQL_STATE_GENERAL_ERROR,
                        this.exceptionInterceptor);
            }
        } catch (SQLException sqlEx) {
            checkAndFireConnectionError(sqlEx);
        }
    }

    @Override
    public void setBytes(int parameterIndex, byte[] x) throws SQLException {
        try {
            if (this.wrappedStmt != null) {
                ((PreparedStatement) this.wrappedStmt).setBytes(parameterIndex, x);
            } else {
                throw SQLError.createSQLException(Messages.getString("Statement.AlreadyClosed"), MysqlErrorNumbers.SQL_STATE_GENERAL_ERROR,
                        this.exceptionInterceptor);
            }
        } catch (SQLException sqlEx) {
            checkAndFireConnectionError(sqlEx);
        }
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, int length) throws SQLException {
        try {
            if (this.wrappedStmt != null) {
                ((PreparedStatement) this.wrappedStmt).setCharacterStream(parameterIndex, reader, length);
            } else {
                throw SQLError.createSQLException(Messages.getString("Statement.AlreadyClosed"), MysqlErrorNumbers.SQL_STATE_GENERAL_ERROR,
                        this.exceptionInterceptor);
            }
        } catch (SQLException sqlEx) {
            checkAndFireConnectionError(sqlEx);
        }
    }

    @Override
    public void setClob(int parameterIndex, Clob x) throws SQLException {
        try {
            if (this.wrappedStmt != null) {
                ((PreparedStatement) this.wrappedStmt).setClob(parameterIndex, x);
            } else {
                throw SQLError.createSQLException(Messages.getString("Statement.AlreadyClosed"), MysqlErrorNumbers.SQL_STATE_GENERAL_ERROR,
                        this.exceptionInterceptor);
            }
        } catch (SQLException sqlEx) {
            checkAndFireConnectionError(sqlEx);
        }
    }

    @Override
    public void setDate(int parameterIndex, Date x) throws SQLException {
        try {
            if (this.wrappedStmt != null) {
                ((PreparedStatement) this.wrappedStmt).setDate(parameterIndex, x);
            } else {
                throw SQLError.createSQLException(Messages.getString("Statement.AlreadyClosed"), MysqlErrorNumbers.SQL_STATE_GENERAL_ERROR,
                        this.exceptionInterceptor);
            }
        } catch (SQLException sqlEx) {
            checkAndFireConnectionError(sqlEx);
        }
    }

    @Override
    public void setDate(int parameterIndex, Date x, Calendar cal) throws SQLException {
        try {
            if (this.wrappedStmt != null) {
                ((PreparedStatement) this.wrappedStmt).setDate(parameterIndex, x, cal);
            } else {
                throw SQLError.createSQLException(Messages.getString("Statement.AlreadyClosed"), MysqlErrorNumbers.SQL_STATE_GENERAL_ERROR,
                        this.exceptionInterceptor);
            }
        } catch (SQLException sqlEx) {
            checkAndFireConnectionError(sqlEx);
        }
    }

    @Override
    public void setDouble(int parameterIndex, double x) throws SQLException {
        try {
            if (this.wrappedStmt != null) {
                ((PreparedStatement) this.wrappedStmt).setDouble(parameterIndex, x);
            } else {
                throw SQLError.createSQLException(Messages.getString("Statement.AlreadyClosed"), MysqlErrorNumbers.SQL_STATE_GENERAL_ERROR,
                        this.exceptionInterceptor);
            }
        } catch (SQLException sqlEx) {
            checkAndFireConnectionError(sqlEx);
        }
    }

    @Override
    public void setFloat(int parameterIndex, float x) throws SQLException {
        try {
            if (this.wrappedStmt != null) {
                ((PreparedStatement) this.wrappedStmt).setFloat(parameterIndex, x);
            } else {
                throw SQLError.createSQLException(Messages.getString("Statement.AlreadyClosed"), MysqlErrorNumbers.SQL_STATE_GENERAL_ERROR,
                        this.exceptionInterceptor);
            }
        } catch (SQLException sqlEx) {
            checkAndFireConnectionError(sqlEx);
        }
    }

    @Override
    public void setInt(int parameterIndex, int x) throws SQLException {
        try {
            if (this.wrappedStmt != null) {
                ((PreparedStatement) this.wrappedStmt).setInt(parameterIndex, x);
            } else {
                throw SQLError.createSQLException(Messages.getString("Statement.AlreadyClosed"), MysqlErrorNumbers.SQL_STATE_GENERAL_ERROR,
                        this.exceptionInterceptor);
            }
        } catch (SQLException sqlEx) {
            checkAndFireConnectionError(sqlEx);
        }
    }

    @Override
    public void setLong(int parameterIndex, long x) throws SQLException {
        try {
            if (this.wrappedStmt != null) {
                ((PreparedStatement) this.wrappedStmt).setLong(parameterIndex, x);
            } else {
                throw SQLError.createSQLException(Messages.getString("Statement.AlreadyClosed"), MysqlErrorNumbers.SQL_STATE_GENERAL_ERROR,
                        this.exceptionInterceptor);
            }
        } catch (SQLException sqlEx) {
            checkAndFireConnectionError(sqlEx);
        }
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        try {
            if (this.wrappedStmt != null) {
                return ((PreparedStatement) this.wrappedStmt).getMetaData();
            }

            throw SQLError.createSQLException(Messages.getString("Statement.AlreadyClosed"), MysqlErrorNumbers.SQL_STATE_GENERAL_ERROR,
                    this.exceptionInterceptor);
        } catch (SQLException sqlEx) {
            checkAndFireConnectionError(sqlEx);
        }

        return null;
    }

    @Override
    public void setNull(int parameterIndex, int sqlType) throws SQLException {
        try {
            if (this.wrappedStmt != null) {
                ((PreparedStatement) this.wrappedStmt).setNull(parameterIndex, sqlType);
            } else {
                throw SQLError.createSQLException(Messages.getString("Statement.AlreadyClosed"), MysqlErrorNumbers.SQL_STATE_GENERAL_ERROR,
                        this.exceptionInterceptor);
            }
        } catch (SQLException sqlEx) {
            checkAndFireConnectionError(sqlEx);
        }
    }

    @Override
    public void setNull(int parameterIndex, int sqlType, String typeName) throws SQLException {
        try {
            if (this.wrappedStmt != null) {
                ((PreparedStatement) this.wrappedStmt).setNull(parameterIndex, sqlType, typeName);
            } else {
                throw SQLError.createSQLException(Messages.getString("Statement.AlreadyClosed"), MysqlErrorNumbers.SQL_STATE_GENERAL_ERROR,
                        this.exceptionInterceptor);
            }
        } catch (SQLException sqlEx) {
            checkAndFireConnectionError(sqlEx);
        }
    }

    @Override
    public void setObject(int parameterIndex, Object x) throws SQLException {
        try {
            if (this.wrappedStmt != null) {
                ((PreparedStatement) this.wrappedStmt).setObject(parameterIndex, x);
            } else {
                throw SQLError.createSQLException(Messages.getString("Statement.AlreadyClosed"), MysqlErrorNumbers.SQL_STATE_GENERAL_ERROR,
                        this.exceptionInterceptor);
            }
        } catch (SQLException sqlEx) {
            checkAndFireConnectionError(sqlEx);
        }
    }

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType) throws SQLException {
        try {
            if (this.wrappedStmt != null) {
                ((PreparedStatement) this.wrappedStmt).setObject(parameterIndex, x, targetSqlType);
            } else {
                throw SQLError.createSQLException(Messages.getString("Statement.AlreadyClosed"), MysqlErrorNumbers.SQL_STATE_GENERAL_ERROR,
                        this.exceptionInterceptor);
            }
        } catch (SQLException sqlEx) {
            checkAndFireConnectionError(sqlEx);
        }
    }

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType, int scale) throws SQLException {
        try {
            if (this.wrappedStmt != null) {
                ((PreparedStatement) this.wrappedStmt).setObject(parameterIndex, x, targetSqlType, scale);
            } else {
                throw SQLError.createSQLException(Messages.getString("Statement.AlreadyClosed"), MysqlErrorNumbers.SQL_STATE_GENERAL_ERROR,
                        this.exceptionInterceptor);
            }
        } catch (SQLException sqlEx) {
            checkAndFireConnectionError(sqlEx);
        }
    }

    @Override
    public ParameterMetaData getParameterMetaData() throws SQLException {
        try {
            if (this.wrappedStmt != null) {
                return ((PreparedStatement) this.wrappedStmt).getParameterMetaData();
            }

            throw SQLError.createSQLException(Messages.getString("Statement.AlreadyClosed"), MysqlErrorNumbers.SQL_STATE_GENERAL_ERROR,
                    this.exceptionInterceptor);
        } catch (SQLException sqlEx) {
            checkAndFireConnectionError(sqlEx);
        }

        return null;
    }

    @Override
    public void setRef(int parameterIndex, Ref x) throws SQLException {
        try {
            if (this.wrappedStmt != null) {
                ((PreparedStatement) this.wrappedStmt).setRef(parameterIndex, x);
            } else {
                throw SQLError.createSQLException(Messages.getString("Statement.AlreadyClosed"), MysqlErrorNumbers.SQL_STATE_GENERAL_ERROR,
                        this.exceptionInterceptor);
            }
        } catch (SQLException sqlEx) {
            checkAndFireConnectionError(sqlEx);
        }
    }

    @Override
    public void setShort(int parameterIndex, short x) throws SQLException {
        try {
            if (this.wrappedStmt != null) {
                ((PreparedStatement) this.wrappedStmt).setShort(parameterIndex, x);
            } else {
                throw SQLError.createSQLException(Messages.getString("Statement.AlreadyClosed"), MysqlErrorNumbers.SQL_STATE_GENERAL_ERROR,
                        this.exceptionInterceptor);
            }
        } catch (SQLException sqlEx) {
            checkAndFireConnectionError(sqlEx);
        }
    }

    @Override
    public void setString(int parameterIndex, String x) throws SQLException {
        try {
            if (this.wrappedStmt != null) {
                ((PreparedStatement) this.wrappedStmt).setString(parameterIndex, x);
            } else {
                throw SQLError.createSQLException(Messages.getString("Statement.AlreadyClosed"), MysqlErrorNumbers.SQL_STATE_GENERAL_ERROR,
                        this.exceptionInterceptor);
            }
        } catch (SQLException sqlEx) {
            checkAndFireConnectionError(sqlEx);
        }
    }

    @Override
    public void setTime(int parameterIndex, Time x) throws SQLException {
        try {
            if (this.wrappedStmt != null) {
                ((PreparedStatement) this.wrappedStmt).setTime(parameterIndex, x);
            } else {
                throw SQLError.createSQLException(Messages.getString("Statement.AlreadyClosed"), MysqlErrorNumbers.SQL_STATE_GENERAL_ERROR,
                        this.exceptionInterceptor);
            }
        } catch (SQLException sqlEx) {
            checkAndFireConnectionError(sqlEx);
        }
    }

    @Override
    public void setTime(int parameterIndex, Time x, Calendar cal) throws SQLException {
        try {
            if (this.wrappedStmt != null) {
                ((PreparedStatement) this.wrappedStmt).setTime(parameterIndex, x, cal);
            } else {
                throw SQLError.createSQLException(Messages.getString("Statement.AlreadyClosed"), MysqlErrorNumbers.SQL_STATE_GENERAL_ERROR,
                        this.exceptionInterceptor);
            }
        } catch (SQLException sqlEx) {
            checkAndFireConnectionError(sqlEx);
        }
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x) throws SQLException {
        try {
            if (this.wrappedStmt != null) {
                ((PreparedStatement) this.wrappedStmt).setTimestamp(parameterIndex, x);
            } else {
                throw SQLError.createSQLException(Messages.getString("Statement.AlreadyClosed"), MysqlErrorNumbers.SQL_STATE_GENERAL_ERROR,
                        this.exceptionInterceptor);
            }
        } catch (SQLException sqlEx) {
            checkAndFireConnectionError(sqlEx);
        }
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal) throws SQLException {
        try {
            if (this.wrappedStmt != null) {
                ((PreparedStatement) this.wrappedStmt).setTimestamp(parameterIndex, x, cal);
            } else {
                throw SQLError.createSQLException(Messages.getString("Statement.AlreadyClosed"), MysqlErrorNumbers.SQL_STATE_GENERAL_ERROR,
                        this.exceptionInterceptor);
            }
        } catch (SQLException sqlEx) {
            checkAndFireConnectionError(sqlEx);
        }
    }

    @Override
    public void setURL(int parameterIndex, URL x) throws SQLException {
        try {
            if (this.wrappedStmt != null) {
                ((PreparedStatement) this.wrappedStmt).setURL(parameterIndex, x);
            } else {
                throw SQLError.createSQLException(Messages.getString("Statement.AlreadyClosed"), MysqlErrorNumbers.SQL_STATE_GENERAL_ERROR,
                        this.exceptionInterceptor);
            }
        } catch (SQLException sqlEx) {
            checkAndFireConnectionError(sqlEx);
        }
    }

    @Override
    @Deprecated
    public void setUnicodeStream(int parameterIndex, InputStream x, int length) throws SQLException {
        try {
            if (this.wrappedStmt != null) {
                ((PreparedStatement) this.wrappedStmt).setUnicodeStream(parameterIndex, x, length);
            } else {
                throw SQLError.createSQLException(Messages.getString("Statement.AlreadyClosed"), MysqlErrorNumbers.SQL_STATE_GENERAL_ERROR,
                        this.exceptionInterceptor);
            }
        } catch (SQLException sqlEx) {
            checkAndFireConnectionError(sqlEx);
        }
    }

    @Override
    public void addBatch() throws SQLException {
        try {
            if (this.wrappedStmt != null) {
                ((PreparedStatement) this.wrappedStmt).addBatch();
            } else {
                throw SQLError.createSQLException(Messages.getString("Statement.AlreadyClosed"), MysqlErrorNumbers.SQL_STATE_GENERAL_ERROR,
                        this.exceptionInterceptor);
            }
        } catch (SQLException sqlEx) {
            checkAndFireConnectionError(sqlEx);
        }
    }

    @Override
    public void clearParameters() throws SQLException {
        try {
            if (this.wrappedStmt != null) {
                ((PreparedStatement) this.wrappedStmt).clearParameters();
            } else {
                throw SQLError.createSQLException(Messages.getString("Statement.AlreadyClosed"), MysqlErrorNumbers.SQL_STATE_GENERAL_ERROR,
                        this.exceptionInterceptor);
            }
        } catch (SQLException sqlEx) {
            checkAndFireConnectionError(sqlEx);
        }
    }

    @Override
    public boolean execute() throws SQLException {
        try {
            if (this.wrappedStmt != null) {
                return ((PreparedStatement) this.wrappedStmt).execute();
            }

            throw SQLError.createSQLException(Messages.getString("Statement.AlreadyClosed"), MysqlErrorNumbers.SQL_STATE_GENERAL_ERROR,
                    this.exceptionInterceptor);
        } catch (SQLException sqlEx) {
            checkAndFireConnectionError(sqlEx);
        }

        return false; // we actually never get here, but the compiler can't figure that out
    }

    @Override
    public ResultSet executeQuery() throws SQLException {
        ResultSet rs = null;
        try {
            if (this.wrappedStmt == null) {
                throw SQLError.createSQLException(Messages.getString("Statement.AlreadyClosed"), MysqlErrorNumbers.SQL_STATE_GENERAL_ERROR,
                        this.exceptionInterceptor);
            }

            rs = ((PreparedStatement) this.wrappedStmt).executeQuery();
            ((com.mysql.cj.jdbc.result.ResultSetInternalMethods) rs).setWrapperStatement(this);

        } catch (SQLException sqlEx) {
            checkAndFireConnectionError(sqlEx);
        }

        return rs;
    }

    @Override
    public int executeUpdate() throws SQLException {
        try {
            if (this.wrappedStmt != null) {
                return ((PreparedStatement) this.wrappedStmt).executeUpdate();
            }

            throw SQLError.createSQLException(Messages.getString("Statement.AlreadyClosed"), MysqlErrorNumbers.SQL_STATE_GENERAL_ERROR,
                    this.exceptionInterceptor);
        } catch (SQLException sqlEx) {
            checkAndFireConnectionError(sqlEx);
        }

        return -1; // we actually never get here, but the compiler can't figure that out
    }

    @Override
    public String toString() {
        StringBuilder buf = new StringBuilder(super.toString());

        if (this.wrappedStmt != null) {
            buf.append(": ");
            try {
                buf.append(((ClientPreparedStatement) this.wrappedStmt).asSql());
            } catch (SQLException sqlEx) {
                buf.append("EXCEPTION: " + sqlEx.toString());
            }
        }

        return buf.toString();
    }

    @Override
    public void setRowId(int parameterIndex, RowId x) throws SQLException {
        try {
            if (this.wrappedStmt != null) {
                ((PreparedStatement) this.wrappedStmt).setRowId(parameterIndex, x);
            } else {
                throw SQLError.createSQLException(Messages.getString("Statement.AlreadyClosed"), MysqlErrorNumbers.SQL_STATE_GENERAL_ERROR,
                        this.exceptionInterceptor);
            }
        } catch (SQLException sqlEx) {
            checkAndFireConnectionError(sqlEx);
        }
    }

    @Override
    public void setNString(int parameterIndex, String value) throws SQLException {
        try {
            if (this.wrappedStmt != null) {
                ((PreparedStatement) this.wrappedStmt).setNString(parameterIndex, value);
            } else {
                throw SQLError.createSQLException(Messages.getString("Statement.AlreadyClosed"), MysqlErrorNumbers.SQL_STATE_GENERAL_ERROR,
                        this.exceptionInterceptor);
            }
        } catch (SQLException sqlEx) {
            checkAndFireConnectionError(sqlEx);
        }
    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader value, long length) throws SQLException {
        try {
            if (this.wrappedStmt != null) {
                ((PreparedStatement) this.wrappedStmt).setNCharacterStream(parameterIndex, value, length);
            } else {
                throw SQLError.createSQLException(Messages.getString("Statement.AlreadyClosed"), MysqlErrorNumbers.SQL_STATE_GENERAL_ERROR,
                        this.exceptionInterceptor);
            }
        } catch (SQLException sqlEx) {
            checkAndFireConnectionError(sqlEx);
        }
    }

    @Override
    public void setNClob(int parameterIndex, NClob value) throws SQLException {
        try {
            if (this.wrappedStmt != null) {
                ((PreparedStatement) this.wrappedStmt).setNClob(parameterIndex, value);
            } else {
                throw SQLError.createSQLException(Messages.getString("Statement.AlreadyClosed"), MysqlErrorNumbers.SQL_STATE_GENERAL_ERROR,
                        this.exceptionInterceptor);
            }
        } catch (SQLException sqlEx) {
            checkAndFireConnectionError(sqlEx);
        }
    }

    @Override
    public void setClob(int parameterIndex, Reader reader, long length) throws SQLException {
        try {
            if (this.wrappedStmt != null) {
                ((PreparedStatement) this.wrappedStmt).setClob(parameterIndex, reader, length);
            } else {
                throw SQLError.createSQLException(Messages.getString("Statement.AlreadyClosed"), MysqlErrorNumbers.SQL_STATE_GENERAL_ERROR,
                        this.exceptionInterceptor);
            }
        } catch (SQLException sqlEx) {
            checkAndFireConnectionError(sqlEx);
        }
    }

    @Override
    public void setBlob(int parameterIndex, InputStream inputStream, long length) throws SQLException {
        try {
            if (this.wrappedStmt != null) {
                ((PreparedStatement) this.wrappedStmt).setBlob(parameterIndex, inputStream, length);
            } else {
                throw SQLError.createSQLException(Messages.getString("Statement.AlreadyClosed"), MysqlErrorNumbers.SQL_STATE_GENERAL_ERROR,
                        this.exceptionInterceptor);
            }
        } catch (SQLException sqlEx) {
            checkAndFireConnectionError(sqlEx);
        }
    }

    @Override
    public void setNClob(int parameterIndex, Reader reader, long length) throws SQLException {
        try {
            if (this.wrappedStmt != null) {
                ((PreparedStatement) this.wrappedStmt).setNClob(parameterIndex, reader, length);
            } else {
                throw SQLError.createSQLException(Messages.getString("Statement.AlreadyClosed"), MysqlErrorNumbers.SQL_STATE_GENERAL_ERROR,
                        this.exceptionInterceptor);
            }
        } catch (SQLException sqlEx) {
            checkAndFireConnectionError(sqlEx);
        }
    }

    @Override
    public void setSQLXML(int parameterIndex, SQLXML xmlObject) throws SQLException {
        try {
            if (this.wrappedStmt != null) {
                ((PreparedStatement) this.wrappedStmt).setSQLXML(parameterIndex, xmlObject);
            } else {
                throw SQLError.createSQLException(Messages.getString("Statement.AlreadyClosed"), MysqlErrorNumbers.SQL_STATE_GENERAL_ERROR,
                        this.exceptionInterceptor);
            }
        } catch (SQLException sqlEx) {
            checkAndFireConnectionError(sqlEx);
        }
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, long length) throws SQLException {
        try {
            if (this.wrappedStmt != null) {
                ((PreparedStatement) this.wrappedStmt).setAsciiStream(parameterIndex, x, length);
            } else {
                throw SQLError.createSQLException(Messages.getString("Statement.AlreadyClosed"), MysqlErrorNumbers.SQL_STATE_GENERAL_ERROR,
                        this.exceptionInterceptor);
            }
        } catch (SQLException sqlEx) {
            checkAndFireConnectionError(sqlEx);
        }
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, long length) throws SQLException {
        try {
            if (this.wrappedStmt != null) {
                ((PreparedStatement) this.wrappedStmt).setBinaryStream(parameterIndex, x, length);
            } else {
                throw SQLError.createSQLException(Messages.getString("Statement.AlreadyClosed"), MysqlErrorNumbers.SQL_STATE_GENERAL_ERROR,
                        this.exceptionInterceptor);
            }
        } catch (SQLException sqlEx) {
            checkAndFireConnectionError(sqlEx);
        }
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, long length) throws SQLException {
        try {
            if (this.wrappedStmt != null) {
                ((PreparedStatement) this.wrappedStmt).setCharacterStream(parameterIndex, reader, length);
            } else {
                throw SQLError.createSQLException(Messages.getString("Statement.AlreadyClosed"), MysqlErrorNumbers.SQL_STATE_GENERAL_ERROR,
                        this.exceptionInterceptor);
            }
        } catch (SQLException sqlEx) {
            checkAndFireConnectionError(sqlEx);
        }
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x) throws SQLException {
        try {
            if (this.wrappedStmt != null) {
                ((PreparedStatement) this.wrappedStmt).setAsciiStream(parameterIndex, x);
            } else {
                throw SQLError.createSQLException(Messages.getString("Statement.AlreadyClosed"), MysqlErrorNumbers.SQL_STATE_GENERAL_ERROR,
                        this.exceptionInterceptor);
            }
        } catch (SQLException sqlEx) {
            checkAndFireConnectionError(sqlEx);
        }
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x) throws SQLException {
        try {
            if (this.wrappedStmt != null) {
                ((PreparedStatement) this.wrappedStmt).setBinaryStream(parameterIndex, x);
            } else {
                throw SQLError.createSQLException(Messages.getString("Statement.AlreadyClosed"), MysqlErrorNumbers.SQL_STATE_GENERAL_ERROR,
                        this.exceptionInterceptor);
            }
        } catch (SQLException sqlEx) {
            checkAndFireConnectionError(sqlEx);
        }
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader) throws SQLException {
        try {
            if (this.wrappedStmt != null) {
                ((PreparedStatement) this.wrappedStmt).setCharacterStream(parameterIndex, reader);
            } else {
                throw SQLError.createSQLException(Messages.getString("Statement.AlreadyClosed"), MysqlErrorNumbers.SQL_STATE_GENERAL_ERROR,
                        this.exceptionInterceptor);
            }
        } catch (SQLException sqlEx) {
            checkAndFireConnectionError(sqlEx);
        }

    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader value) throws SQLException {
        try {
            if (this.wrappedStmt != null) {
                ((PreparedStatement) this.wrappedStmt).setNCharacterStream(parameterIndex, value);
            } else {
                throw SQLError.createSQLException(Messages.getString("Statement.AlreadyClosed"), MysqlErrorNumbers.SQL_STATE_GENERAL_ERROR,
                        this.exceptionInterceptor);
            }
        } catch (SQLException sqlEx) {
            checkAndFireConnectionError(sqlEx);
        }

    }

    @Override
    public void setClob(int parameterIndex, Reader reader) throws SQLException {
        try {
            if (this.wrappedStmt != null) {
                ((PreparedStatement) this.wrappedStmt).setClob(parameterIndex, reader);
            } else {
                throw SQLError.createSQLException(Messages.getString("Statement.AlreadyClosed"), MysqlErrorNumbers.SQL_STATE_GENERAL_ERROR,
                        this.exceptionInterceptor);
            }
        } catch (SQLException sqlEx) {
            checkAndFireConnectionError(sqlEx);
        }

    }

    @Override
    public void setBlob(int parameterIndex, InputStream inputStream) throws SQLException {
        try {
            if (this.wrappedStmt != null) {
                ((PreparedStatement) this.wrappedStmt).setBlob(parameterIndex, inputStream);
            } else {
                throw SQLError.createSQLException(Messages.getString("Statement.AlreadyClosed"), MysqlErrorNumbers.SQL_STATE_GENERAL_ERROR,
                        this.exceptionInterceptor);
            }
        } catch (SQLException sqlEx) {
            checkAndFireConnectionError(sqlEx);
        }
    }

    @Override
    public void setNClob(int parameterIndex, Reader reader) throws SQLException {
        try {
            if (this.wrappedStmt != null) {
                ((PreparedStatement) this.wrappedStmt).setNClob(parameterIndex, reader);
            } else {
                throw SQLError.createSQLException(Messages.getString("Statement.AlreadyClosed"), MysqlErrorNumbers.SQL_STATE_GENERAL_ERROR,
                        this.exceptionInterceptor);
            }
        } catch (SQLException sqlEx) {
            checkAndFireConnectionError(sqlEx);
        }
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {

        boolean isInstance = iface.isInstance(this);

        if (isInstance) {
            return true;
        }

        String interfaceClassName = iface.getName();

        return (interfaceClassName.equals("com.mysql.cj.jdbc.Statement") || interfaceClassName.equals("java.sql.Statement")
                || interfaceClassName.equals("java.sql.Wrapper") || interfaceClassName.equals("java.sql.PreparedStatement")); // TODO check other interfaces
    }

    @Override
    public synchronized <T> T unwrap(java.lang.Class<T> iface) throws java.sql.SQLException {
        try {
            if ("java.sql.Statement".equals(iface.getName()) || "java.sql.PreparedStatement".equals(iface.getName())
                    || "java.sql.Wrapper.class".equals(iface.getName())) {
                return iface.cast(this);
            }

            if (this.unwrappedInterfaces == null) {
                this.unwrappedInterfaces = new HashMap<>();
            }

            Object cachedUnwrapped = this.unwrappedInterfaces.get(iface);

            if (cachedUnwrapped == null) {
                cachedUnwrapped = Proxy.newProxyInstance(this.wrappedStmt.getClass().getClassLoader(), new Class<?>[] { iface },
                        new ConnectionErrorFiringInvocationHandler(this.wrappedStmt));
                this.unwrappedInterfaces.put(iface, cachedUnwrapped);
            }

            return iface.cast(cachedUnwrapped);
        } catch (ClassCastException cce) {
            throw SQLError.createSQLException(Messages.getString("Common.UnableToUnwrap", new Object[] { iface.toString() }),
                    MysqlErrorNumbers.SQL_STATE_ILLEGAL_ARGUMENT, this.exceptionInterceptor);
        }
    }

    @Override
    public synchronized void close() throws SQLException {
        if (this.pooledConnection == null) {
            // no-op
            return;
        }

        MysqlPooledConnection con = this.pooledConnection; // we need this later...

        try {
            super.close();
        } finally {
            try {
                StatementEvent e = new StatementEvent(con, this);
                con.fireStatementEvent(e);
            } finally {
                this.unwrappedInterfaces = null;
            }
        }
    }

    @Override
    public long executeLargeUpdate() throws SQLException {
        try {
            if (this.wrappedStmt != null) {
                return ((ClientPreparedStatement) this.wrappedStmt).executeLargeUpdate();
            }

            throw SQLError.createSQLException(Messages.getString("Statement.AlreadyClosed"), MysqlErrorNumbers.SQL_STATE_ILLEGAL_ARGUMENT,
                    this.exceptionInterceptor);
        } catch (SQLException sqlEx) {
            checkAndFireConnectionError(sqlEx);
        }

        return -1; // we actually never get here, but the compiler can't figure that out
    }

    @Override
    public void setObject(int parameterIndex, Object x, SQLType targetSqlType) throws SQLException {
        try {
            if (this.wrappedStmt != null) {
                ((PreparedStatement) this.wrappedStmt).setObject(parameterIndex, x, targetSqlType);
            } else {
                throw SQLError.createSQLException("No operations allowed after statement closed", MysqlErrorNumbers.SQL_STATE_GENERAL_ERROR,
                        this.exceptionInterceptor);
            }
        } catch (SQLException sqlEx) {
            checkAndFireConnectionError(sqlEx);
        }
    }

    @Override
    public void setObject(int parameterIndex, Object x, SQLType targetSqlType, int scaleOrLength) throws SQLException {
        try {
            if (this.wrappedStmt != null) {
                ((PreparedStatement) this.wrappedStmt).setObject(parameterIndex, x, targetSqlType, scaleOrLength);
            } else {
                throw SQLError.createSQLException("No operations allowed after statement closed", MysqlErrorNumbers.SQL_STATE_GENERAL_ERROR,
                        this.exceptionInterceptor);
            }
        } catch (SQLException sqlEx) {
            checkAndFireConnectionError(sqlEx);
        }
    }
}
