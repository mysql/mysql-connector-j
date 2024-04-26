/*
 * Copyright (c) 2002, 2024, Oracle and/or its affiliates.
 *
 * This program is free software; you can redistribute it and/or modify it under the terms of the GNU General Public License, version 2.0, as published by
 * the Free Software Foundation.
 *
 * This program is designed to work with certain software that is licensed under separate terms, as designated in a particular file or component or in
 * included license documentation. The authors of MySQL hereby grant you an additional permission to link the program and your derivative works with the
 * separately licensed software that they have either included with the program or referenced in the documentation.
 *
 * Without limiting anything contained in the foregoing, this file, which is part of MySQL Connector/J, is also subject to the Universal FOSS Exception,
 * version 1.0, a copy of which can be found at http://oss.oracle.com/licenses/universal-foss-exception.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License, version 2.0, for more details.
 *
 * You should have received a copy of the GNU General Public License along with this program; if not, write to the Free Software Foundation, Inc.,
 * 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA
 */

package com.mysql.cj.jdbc;

import java.io.InputStream;
import java.io.Reader;
import java.lang.reflect.Proxy;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.Ref;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLType;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;

import com.mysql.cj.Messages;
import com.mysql.cj.exceptions.MysqlErrorNumbers;
import com.mysql.cj.jdbc.exceptions.SQLError;

/**
 * Wraps callable statements created by pooled connections.
 */
public class CallableStatementWrapper extends PreparedStatementWrapper implements CallableStatement {

    protected static CallableStatementWrapper getInstance(ConnectionWrapper c, MysqlPooledConnection conn, CallableStatement toWrap) throws SQLException {
        return new CallableStatementWrapper(c, conn, toWrap);
    }

    /**
     * @param c
     *            ConnectionWrapper
     * @param conn
     *            MysqlPooledConnection
     * @param toWrap
     *            CallableStatement
     */
    public CallableStatementWrapper(ConnectionWrapper c, MysqlPooledConnection conn, CallableStatement toWrap) {
        super(c, conn, toWrap);
    }

    @Override
    public void registerOutParameter(int parameterIndex, int sqlType) throws SQLException {
        try {
            if (this.wrappedStmt != null) {
                ((CallableStatement) this.wrappedStmt).registerOutParameter(parameterIndex, sqlType);
            } else {
                throw SQLError.createSQLException(Messages.getString("Statement.AlreadyClosed"), MysqlErrorNumbers.SQLSTATE_CONNJ_GENERAL_ERROR,
                        this.exceptionInterceptor);
            }
        } catch (SQLException sqlEx) {
            checkAndFireConnectionError(sqlEx);
        }
    }

    @Override
    public void registerOutParameter(int parameterIndex, int sqlType, int scale) throws SQLException {
        try {
            if (this.wrappedStmt != null) {
                ((CallableStatement) this.wrappedStmt).registerOutParameter(parameterIndex, sqlType, scale);
            } else {
                throw SQLError.createSQLException(Messages.getString("Statement.AlreadyClosed"), MysqlErrorNumbers.SQLSTATE_CONNJ_GENERAL_ERROR,
                        this.exceptionInterceptor);
            }
        } catch (SQLException sqlEx) {
            checkAndFireConnectionError(sqlEx);
        }
    }

    @Override
    public boolean wasNull() throws SQLException {
        try {
            if (this.wrappedStmt != null) {
                return ((CallableStatement) this.wrappedStmt).wasNull();
            }
            throw SQLError.createSQLException(Messages.getString("Statement.AlreadyClosed"), MysqlErrorNumbers.SQLSTATE_CONNJ_GENERAL_ERROR,
                    this.exceptionInterceptor);

        } catch (SQLException sqlEx) {
            checkAndFireConnectionError(sqlEx);
        }

        return false;
    }

    @Override
    public String getString(int parameterIndex) throws SQLException {
        try {
            if (this.wrappedStmt != null) {
                return ((CallableStatement) this.wrappedStmt).getString(parameterIndex);
            }
            throw SQLError.createSQLException(Messages.getString("Statement.AlreadyClosed"), MysqlErrorNumbers.SQLSTATE_CONNJ_GENERAL_ERROR,
                    this.exceptionInterceptor);

        } catch (SQLException sqlEx) {
            checkAndFireConnectionError(sqlEx);
        }
        return null;
    }

    @Override
    public boolean getBoolean(int parameterIndex) throws SQLException {
        try {
            if (this.wrappedStmt != null) {
                return ((CallableStatement) this.wrappedStmt).getBoolean(parameterIndex);
            }
            throw SQLError.createSQLException(Messages.getString("Statement.AlreadyClosed"), MysqlErrorNumbers.SQLSTATE_CONNJ_GENERAL_ERROR,
                    this.exceptionInterceptor);

        } catch (SQLException sqlEx) {
            checkAndFireConnectionError(sqlEx);
        }

        return false;
    }

    @Override
    public byte getByte(int parameterIndex) throws SQLException {
        try {
            if (this.wrappedStmt != null) {
                return ((CallableStatement) this.wrappedStmt).getByte(parameterIndex);
            }
            throw SQLError.createSQLException(Messages.getString("Statement.AlreadyClosed"), MysqlErrorNumbers.SQLSTATE_CONNJ_GENERAL_ERROR,
                    this.exceptionInterceptor);

        } catch (SQLException sqlEx) {
            checkAndFireConnectionError(sqlEx);
        }

        return 0;
    }

    @Override
    public short getShort(int parameterIndex) throws SQLException {
        try {
            if (this.wrappedStmt != null) {
                return ((CallableStatement) this.wrappedStmt).getShort(parameterIndex);
            }
            throw SQLError.createSQLException(Messages.getString("Statement.AlreadyClosed"), MysqlErrorNumbers.SQLSTATE_CONNJ_GENERAL_ERROR,
                    this.exceptionInterceptor);

        } catch (SQLException sqlEx) {
            checkAndFireConnectionError(sqlEx);
        }

        return 0;
    }

    @Override
    public int getInt(int parameterIndex) throws SQLException {
        try {
            if (this.wrappedStmt != null) {
                return ((CallableStatement) this.wrappedStmt).getInt(parameterIndex);
            }
            throw SQLError.createSQLException(Messages.getString("Statement.AlreadyClosed"), MysqlErrorNumbers.SQLSTATE_CONNJ_GENERAL_ERROR,
                    this.exceptionInterceptor);

        } catch (SQLException sqlEx) {
            checkAndFireConnectionError(sqlEx);
        }

        return 0;
    }

    @Override
    public long getLong(int parameterIndex) throws SQLException {
        try {
            if (this.wrappedStmt != null) {
                return ((CallableStatement) this.wrappedStmt).getLong(parameterIndex);
            }
            throw SQLError.createSQLException(Messages.getString("Statement.AlreadyClosed"), MysqlErrorNumbers.SQLSTATE_CONNJ_GENERAL_ERROR,
                    this.exceptionInterceptor);

        } catch (SQLException sqlEx) {
            checkAndFireConnectionError(sqlEx);
        }

        return 0;
    }

    @Override
    public float getFloat(int parameterIndex) throws SQLException {
        try {
            if (this.wrappedStmt != null) {
                return ((CallableStatement) this.wrappedStmt).getFloat(parameterIndex);
            }
            throw SQLError.createSQLException(Messages.getString("Statement.AlreadyClosed"), MysqlErrorNumbers.SQLSTATE_CONNJ_GENERAL_ERROR,
                    this.exceptionInterceptor);

        } catch (SQLException sqlEx) {
            checkAndFireConnectionError(sqlEx);
        }

        return 0;
    }

    @Override
    public double getDouble(int parameterIndex) throws SQLException {
        try {
            if (this.wrappedStmt != null) {
                return ((CallableStatement) this.wrappedStmt).getDouble(parameterIndex);
            }
            throw SQLError.createSQLException(Messages.getString("Statement.AlreadyClosed"), MysqlErrorNumbers.SQLSTATE_CONNJ_GENERAL_ERROR,
                    this.exceptionInterceptor);

        } catch (SQLException sqlEx) {
            checkAndFireConnectionError(sqlEx);
        }

        return 0;
    }

    @Override
    @Deprecated
    public BigDecimal getBigDecimal(int parameterIndex, int scale) throws SQLException {
        try {
            if (this.wrappedStmt != null) {
                return ((CallableStatement) this.wrappedStmt).getBigDecimal(parameterIndex, scale);
            }
            throw SQLError.createSQLException(Messages.getString("Statement.AlreadyClosed"), MysqlErrorNumbers.SQLSTATE_CONNJ_GENERAL_ERROR,
                    this.exceptionInterceptor);

        } catch (SQLException sqlEx) {
            checkAndFireConnectionError(sqlEx);
        }

        return null;
    }

    @Override
    public byte[] getBytes(int parameterIndex) throws SQLException {
        try {
            if (this.wrappedStmt != null) {
                return ((CallableStatement) this.wrappedStmt).getBytes(parameterIndex);
            }
            throw SQLError.createSQLException(Messages.getString("Statement.AlreadyClosed"), MysqlErrorNumbers.SQLSTATE_CONNJ_GENERAL_ERROR,
                    this.exceptionInterceptor);

        } catch (SQLException sqlEx) {
            checkAndFireConnectionError(sqlEx);
        }

        return null;
    }

    @Override
    public Date getDate(int parameterIndex) throws SQLException {
        try {
            if (this.wrappedStmt != null) {
                return ((CallableStatement) this.wrappedStmt).getDate(parameterIndex);
            }
            throw SQLError.createSQLException(Messages.getString("Statement.AlreadyClosed"), MysqlErrorNumbers.SQLSTATE_CONNJ_GENERAL_ERROR,
                    this.exceptionInterceptor);

        } catch (SQLException sqlEx) {
            checkAndFireConnectionError(sqlEx);
        }

        return null;
    }

    @Override
    public Time getTime(int parameterIndex) throws SQLException {
        try {
            if (this.wrappedStmt != null) {
                return ((CallableStatement) this.wrappedStmt).getTime(parameterIndex);
            }
            throw SQLError.createSQLException(Messages.getString("Statement.AlreadyClosed"), MysqlErrorNumbers.SQLSTATE_CONNJ_GENERAL_ERROR,
                    this.exceptionInterceptor);

        } catch (SQLException sqlEx) {
            checkAndFireConnectionError(sqlEx);
        }

        return null;
    }

    @Override
    public Timestamp getTimestamp(int parameterIndex) throws SQLException {
        try {
            if (this.wrappedStmt != null) {
                return ((CallableStatement) this.wrappedStmt).getTimestamp(parameterIndex);
            }
            throw SQLError.createSQLException(Messages.getString("Statement.AlreadyClosed"), MysqlErrorNumbers.SQLSTATE_CONNJ_GENERAL_ERROR,
                    this.exceptionInterceptor);

        } catch (SQLException sqlEx) {
            checkAndFireConnectionError(sqlEx);
        }

        return null;
    }

    @Override
    public Object getObject(int parameterIndex) throws SQLException {
        try {
            if (this.wrappedStmt != null) {
                return ((CallableStatement) this.wrappedStmt).getObject(parameterIndex);
            }
            throw SQLError.createSQLException(Messages.getString("Statement.AlreadyClosed"), MysqlErrorNumbers.SQLSTATE_CONNJ_GENERAL_ERROR,
                    this.exceptionInterceptor);

        } catch (SQLException sqlEx) {
            checkAndFireConnectionError(sqlEx);
        }

        return null;
    }

    @Override
    public BigDecimal getBigDecimal(int parameterIndex) throws SQLException {
        try {
            if (this.wrappedStmt != null) {
                return ((CallableStatement) this.wrappedStmt).getBigDecimal(parameterIndex);
            }
            throw SQLError.createSQLException(Messages.getString("Statement.AlreadyClosed"), MysqlErrorNumbers.SQLSTATE_CONNJ_GENERAL_ERROR,
                    this.exceptionInterceptor);

        } catch (SQLException sqlEx) {
            checkAndFireConnectionError(sqlEx);
        }

        return null;
    }

    @Override
    public Object getObject(int parameterIndex, Map<String, Class<?>> typeMap) throws SQLException {
        try {
            if (this.wrappedStmt != null) {
                return ((CallableStatement) this.wrappedStmt).getObject(parameterIndex, typeMap);
            }
            throw SQLError.createSQLException(Messages.getString("Statement.AlreadyClosed"), MysqlErrorNumbers.SQLSTATE_CONNJ_GENERAL_ERROR,
                    this.exceptionInterceptor);

        } catch (SQLException sqlEx) {
            checkAndFireConnectionError(sqlEx);
        }
        return null;
    }

    @Override
    public Ref getRef(int parameterIndex) throws SQLException {
        try {
            if (this.wrappedStmt != null) {
                return ((CallableStatement) this.wrappedStmt).getRef(parameterIndex);
            }
            throw SQLError.createSQLException(Messages.getString("Statement.AlreadyClosed"), MysqlErrorNumbers.SQLSTATE_CONNJ_GENERAL_ERROR,
                    this.exceptionInterceptor);

        } catch (SQLException sqlEx) {
            checkAndFireConnectionError(sqlEx);
        }

        return null;
    }

    @Override
    public Blob getBlob(int parameterIndex) throws SQLException {
        try {
            if (this.wrappedStmt != null) {
                return ((CallableStatement) this.wrappedStmt).getBlob(parameterIndex);
            }
            throw SQLError.createSQLException(Messages.getString("Statement.AlreadyClosed"), MysqlErrorNumbers.SQLSTATE_CONNJ_GENERAL_ERROR,
                    this.exceptionInterceptor);

        } catch (SQLException sqlEx) {
            checkAndFireConnectionError(sqlEx);
        }

        return null;
    }

    @Override
    public Clob getClob(int parameterIndex) throws SQLException {
        try {
            if (this.wrappedStmt != null) {
                return ((CallableStatement) this.wrappedStmt).getClob(parameterIndex);
            }
            throw SQLError.createSQLException(Messages.getString("Statement.AlreadyClosed"), MysqlErrorNumbers.SQLSTATE_CONNJ_GENERAL_ERROR,
                    this.exceptionInterceptor);

        } catch (SQLException sqlEx) {
            checkAndFireConnectionError(sqlEx);
        }
        return null;
    }

    @Override
    public Array getArray(int parameterIndex) throws SQLException {
        try {
            if (this.wrappedStmt != null) {
                return ((CallableStatement) this.wrappedStmt).getArray(parameterIndex);
            }
            throw SQLError.createSQLException(Messages.getString("Statement.AlreadyClosed"), MysqlErrorNumbers.SQLSTATE_CONNJ_GENERAL_ERROR,
                    this.exceptionInterceptor);

        } catch (SQLException sqlEx) {
            checkAndFireConnectionError(sqlEx);
        }
        return null;
    }

    @Override
    public Date getDate(int parameterIndex, Calendar cal) throws SQLException {
        try {
            if (this.wrappedStmt != null) {
                return ((CallableStatement) this.wrappedStmt).getDate(parameterIndex, cal);
            }
            throw SQLError.createSQLException(Messages.getString("Statement.AlreadyClosed"), MysqlErrorNumbers.SQLSTATE_CONNJ_GENERAL_ERROR,
                    this.exceptionInterceptor);

        } catch (SQLException sqlEx) {
            checkAndFireConnectionError(sqlEx);
        }
        return null;
    }

    @Override
    public Time getTime(int parameterIndex, Calendar cal) throws SQLException {
        try {
            if (this.wrappedStmt != null) {
                return ((CallableStatement) this.wrappedStmt).getTime(parameterIndex, cal);
            }
            throw SQLError.createSQLException(Messages.getString("Statement.AlreadyClosed"), MysqlErrorNumbers.SQLSTATE_CONNJ_GENERAL_ERROR,
                    this.exceptionInterceptor);

        } catch (SQLException sqlEx) {
            checkAndFireConnectionError(sqlEx);
        }
        return null;
    }

    @Override
    public Timestamp getTimestamp(int parameterIndex, Calendar cal) throws SQLException {
        try {
            if (this.wrappedStmt != null) {
                return ((CallableStatement) this.wrappedStmt).getTimestamp(parameterIndex, cal);
            }
            throw SQLError.createSQLException(Messages.getString("Statement.AlreadyClosed"), MysqlErrorNumbers.SQLSTATE_CONNJ_GENERAL_ERROR,
                    this.exceptionInterceptor);

        } catch (SQLException sqlEx) {
            checkAndFireConnectionError(sqlEx);
        }
        return null;
    }

    @Override
    public void registerOutParameter(int paramIndex, int sqlType, String typeName) throws SQLException {
        try {
            if (this.wrappedStmt != null) {
                ((CallableStatement) this.wrappedStmt).registerOutParameter(paramIndex, sqlType, typeName);
            } else {
                throw SQLError.createSQLException(Messages.getString("Statement.AlreadyClosed"), MysqlErrorNumbers.SQLSTATE_CONNJ_GENERAL_ERROR,
                        this.exceptionInterceptor);
            }
        } catch (SQLException sqlEx) {
            checkAndFireConnectionError(sqlEx);
        }
    }

    @Override
    public void registerOutParameter(String parameterName, int sqlType) throws SQLException {
        try {
            if (this.wrappedStmt != null) {
                ((CallableStatement) this.wrappedStmt).registerOutParameter(parameterName, sqlType);
            } else {
                throw SQLError.createSQLException(Messages.getString("Statement.AlreadyClosed"), MysqlErrorNumbers.SQLSTATE_CONNJ_GENERAL_ERROR,
                        this.exceptionInterceptor);
            }
        } catch (SQLException sqlEx) {
            checkAndFireConnectionError(sqlEx);
        }
    }

    @Override
    public void registerOutParameter(String parameterName, int sqlType, int scale) throws SQLException {
        try {
            if (this.wrappedStmt != null) {
                ((CallableStatement) this.wrappedStmt).registerOutParameter(parameterName, sqlType, scale);
            } else {
                throw SQLError.createSQLException(Messages.getString("Statement.AlreadyClosed"), MysqlErrorNumbers.SQLSTATE_CONNJ_GENERAL_ERROR,
                        this.exceptionInterceptor);
            }
        } catch (SQLException sqlEx) {
            checkAndFireConnectionError(sqlEx);
        }
    }

    @Override
    public void registerOutParameter(String parameterName, int sqlType, String typeName) throws SQLException {
        try {
            if (this.wrappedStmt != null) {
                ((CallableStatement) this.wrappedStmt).registerOutParameter(parameterName, sqlType, typeName);
            } else {
                throw SQLError.createSQLException(Messages.getString("Statement.AlreadyClosed"), MysqlErrorNumbers.SQLSTATE_CONNJ_GENERAL_ERROR,
                        this.exceptionInterceptor);
            }
        } catch (SQLException sqlEx) {
            checkAndFireConnectionError(sqlEx);
        }
    }

    @Override
    public URL getURL(int parameterIndex) throws SQLException {
        try {
            if (this.wrappedStmt != null) {
                return ((CallableStatement) this.wrappedStmt).getURL(parameterIndex);
            }
            throw SQLError.createSQLException(Messages.getString("Statement.AlreadyClosed"), MysqlErrorNumbers.SQLSTATE_CONNJ_GENERAL_ERROR,
                    this.exceptionInterceptor);

        } catch (SQLException sqlEx) {
            checkAndFireConnectionError(sqlEx);
        }

        return null;
    }

    @Override
    public void setURL(String parameterName, URL val) throws SQLException {
        try {
            if (this.wrappedStmt != null) {
                ((CallableStatement) this.wrappedStmt).setURL(parameterName, val);
            } else {
                throw SQLError.createSQLException(Messages.getString("Statement.AlreadyClosed"), MysqlErrorNumbers.SQLSTATE_CONNJ_GENERAL_ERROR,
                        this.exceptionInterceptor);
            }
        } catch (SQLException sqlEx) {
            checkAndFireConnectionError(sqlEx);
        }
    }

    @Override
    public void setNull(String parameterName, int sqlType) throws SQLException {
        try {
            if (this.wrappedStmt != null) {
                ((CallableStatement) this.wrappedStmt).setNull(parameterName, sqlType);
            } else {
                throw SQLError.createSQLException(Messages.getString("Statement.AlreadyClosed"), MysqlErrorNumbers.SQLSTATE_CONNJ_GENERAL_ERROR,
                        this.exceptionInterceptor);
            }
        } catch (SQLException sqlEx) {
            checkAndFireConnectionError(sqlEx);
        }
    }

    @Override
    public void setBoolean(String parameterName, boolean x) throws SQLException {
        try {
            if (this.wrappedStmt != null) {
                ((CallableStatement) this.wrappedStmt).setBoolean(parameterName, x);
            } else {
                throw SQLError.createSQLException(Messages.getString("Statement.AlreadyClosed"), MysqlErrorNumbers.SQLSTATE_CONNJ_GENERAL_ERROR,
                        this.exceptionInterceptor);
            }
        } catch (SQLException sqlEx) {
            checkAndFireConnectionError(sqlEx);
        }
    }

    @Override
    public void setByte(String parameterName, byte x) throws SQLException {
        try {
            if (this.wrappedStmt != null) {
                ((CallableStatement) this.wrappedStmt).setByte(parameterName, x);
            } else {
                throw SQLError.createSQLException(Messages.getString("Statement.AlreadyClosed"), MysqlErrorNumbers.SQLSTATE_CONNJ_GENERAL_ERROR,
                        this.exceptionInterceptor);
            }
        } catch (SQLException sqlEx) {
            checkAndFireConnectionError(sqlEx);
        }
    }

    @Override
    public void setShort(String parameterName, short x) throws SQLException {
        try {
            if (this.wrappedStmt != null) {
                ((CallableStatement) this.wrappedStmt).setShort(parameterName, x);
            } else {
                throw SQLError.createSQLException(Messages.getString("Statement.AlreadyClosed"), MysqlErrorNumbers.SQLSTATE_CONNJ_GENERAL_ERROR,
                        this.exceptionInterceptor);
            }
        } catch (SQLException sqlEx) {
            checkAndFireConnectionError(sqlEx);
        }
    }

    @Override
    public void setInt(String parameterName, int x) throws SQLException {
        try {
            if (this.wrappedStmt != null) {
                ((CallableStatement) this.wrappedStmt).setInt(parameterName, x);
            } else {
                throw SQLError.createSQLException(Messages.getString("Statement.AlreadyClosed"), MysqlErrorNumbers.SQLSTATE_CONNJ_GENERAL_ERROR,
                        this.exceptionInterceptor);
            }
        } catch (SQLException sqlEx) {
            checkAndFireConnectionError(sqlEx);
        }
    }

    @Override
    public void setLong(String parameterName, long x) throws SQLException {
        try {
            if (this.wrappedStmt != null) {
                ((CallableStatement) this.wrappedStmt).setLong(parameterName, x);
            } else {
                throw SQLError.createSQLException(Messages.getString("Statement.AlreadyClosed"), MysqlErrorNumbers.SQLSTATE_CONNJ_GENERAL_ERROR,
                        this.exceptionInterceptor);
            }
        } catch (SQLException sqlEx) {
            checkAndFireConnectionError(sqlEx);
        }
    }

    @Override
    public void setFloat(String parameterName, float x) throws SQLException {
        try {
            if (this.wrappedStmt != null) {
                ((CallableStatement) this.wrappedStmt).setFloat(parameterName, x);
            } else {
                throw SQLError.createSQLException(Messages.getString("Statement.AlreadyClosed"), MysqlErrorNumbers.SQLSTATE_CONNJ_GENERAL_ERROR,
                        this.exceptionInterceptor);
            }
        } catch (SQLException sqlEx) {
            checkAndFireConnectionError(sqlEx);
        }
    }

    @Override
    public void setDouble(String parameterName, double x) throws SQLException {
        try {
            if (this.wrappedStmt != null) {
                ((CallableStatement) this.wrappedStmt).setDouble(parameterName, x);
            } else {
                throw SQLError.createSQLException(Messages.getString("Statement.AlreadyClosed"), MysqlErrorNumbers.SQLSTATE_CONNJ_GENERAL_ERROR,
                        this.exceptionInterceptor);
            }
        } catch (SQLException sqlEx) {
            checkAndFireConnectionError(sqlEx);
        }
    }

    @Override
    public void setBigDecimal(String parameterName, BigDecimal x) throws SQLException {
        try {
            if (this.wrappedStmt != null) {
                ((CallableStatement) this.wrappedStmt).setBigDecimal(parameterName, x);
            } else {
                throw SQLError.createSQLException(Messages.getString("Statement.AlreadyClosed"), MysqlErrorNumbers.SQLSTATE_CONNJ_GENERAL_ERROR,
                        this.exceptionInterceptor);
            }
        } catch (SQLException sqlEx) {
            checkAndFireConnectionError(sqlEx);
        }
    }

    @Override
    public void setString(String parameterName, String x) throws SQLException {
        try {
            if (this.wrappedStmt != null) {
                ((CallableStatement) this.wrappedStmt).setString(parameterName, x);
            } else {
                throw SQLError.createSQLException(Messages.getString("Statement.AlreadyClosed"), MysqlErrorNumbers.SQLSTATE_CONNJ_GENERAL_ERROR,
                        this.exceptionInterceptor);
            }
        } catch (SQLException sqlEx) {
            checkAndFireConnectionError(sqlEx);
        }
    }

    @Override
    public void setBytes(String parameterName, byte[] x) throws SQLException {
        try {
            if (this.wrappedStmt != null) {
                ((CallableStatement) this.wrappedStmt).setBytes(parameterName, x);
            } else {
                throw SQLError.createSQLException(Messages.getString("Statement.AlreadyClosed"), MysqlErrorNumbers.SQLSTATE_CONNJ_GENERAL_ERROR,
                        this.exceptionInterceptor);
            }
        } catch (SQLException sqlEx) {
            checkAndFireConnectionError(sqlEx);
        }
    }

    @Override
    public void setDate(String parameterName, Date x) throws SQLException {
        try {
            if (this.wrappedStmt != null) {
                ((CallableStatement) this.wrappedStmt).setDate(parameterName, x);
            } else {
                throw SQLError.createSQLException(Messages.getString("Statement.AlreadyClosed"), MysqlErrorNumbers.SQLSTATE_CONNJ_GENERAL_ERROR,
                        this.exceptionInterceptor);
            }
        } catch (SQLException sqlEx) {
            checkAndFireConnectionError(sqlEx);
        }
    }

    @Override
    public void setTime(String parameterName, Time x) throws SQLException {
        try {
            if (this.wrappedStmt != null) {
                ((CallableStatement) this.wrappedStmt).setTime(parameterName, x);
            } else {
                throw SQLError.createSQLException(Messages.getString("Statement.AlreadyClosed"), MysqlErrorNumbers.SQLSTATE_CONNJ_GENERAL_ERROR,
                        this.exceptionInterceptor);
            }
        } catch (SQLException sqlEx) {
            checkAndFireConnectionError(sqlEx);
        }
    }

    @Override
    public void setTimestamp(String parameterName, Timestamp x) throws SQLException {
        try {
            if (this.wrappedStmt != null) {
                ((CallableStatement) this.wrappedStmt).setTimestamp(parameterName, x);
            } else {
                throw SQLError.createSQLException(Messages.getString("Statement.AlreadyClosed"), MysqlErrorNumbers.SQLSTATE_CONNJ_GENERAL_ERROR,
                        this.exceptionInterceptor);
            }
        } catch (SQLException sqlEx) {
            checkAndFireConnectionError(sqlEx);
        }
    }

    @Override
    public void setAsciiStream(String parameterName, InputStream x, int length) throws SQLException {
        try {
            if (this.wrappedStmt != null) {
                ((CallableStatement) this.wrappedStmt).setAsciiStream(parameterName, x, length);
            } else {
                throw SQLError.createSQLException(Messages.getString("Statement.AlreadyClosed"), MysqlErrorNumbers.SQLSTATE_CONNJ_GENERAL_ERROR,
                        this.exceptionInterceptor);
            }
        } catch (SQLException sqlEx) {
            checkAndFireConnectionError(sqlEx);
        }
    }

    @Override
    public void setBinaryStream(String parameterName, InputStream x, int length) throws SQLException {
        try {
            if (this.wrappedStmt != null) {
                ((CallableStatement) this.wrappedStmt).setBinaryStream(parameterName, x, length);
            } else {
                throw SQLError.createSQLException(Messages.getString("Statement.AlreadyClosed"), MysqlErrorNumbers.SQLSTATE_CONNJ_GENERAL_ERROR,
                        this.exceptionInterceptor);
            }
        } catch (SQLException sqlEx) {
            checkAndFireConnectionError(sqlEx);
        }
    }

    @Override
    public void setObject(String parameterName, Object x, int targetSqlType, int scale) throws SQLException {
        try {
            if (this.wrappedStmt != null) {
                ((CallableStatement) this.wrappedStmt).setObject(parameterName, x, targetSqlType, scale);
            } else {
                throw SQLError.createSQLException(Messages.getString("Statement.AlreadyClosed"), MysqlErrorNumbers.SQLSTATE_CONNJ_GENERAL_ERROR,
                        this.exceptionInterceptor);
            }
        } catch (SQLException sqlEx) {
            checkAndFireConnectionError(sqlEx);
        }
    }

    @Override
    public void setObject(String parameterName, Object x, int targetSqlType) throws SQLException {
        try {
            if (this.wrappedStmt != null) {
                ((CallableStatement) this.wrappedStmt).setObject(parameterName, x, targetSqlType);
            } else {
                throw SQLError.createSQLException(Messages.getString("Statement.AlreadyClosed"), MysqlErrorNumbers.SQLSTATE_CONNJ_GENERAL_ERROR,
                        this.exceptionInterceptor);
            }
        } catch (SQLException sqlEx) {
            checkAndFireConnectionError(sqlEx);
        }
    }

    @Override
    public void setObject(String parameterName, Object x) throws SQLException {
        try {
            if (this.wrappedStmt != null) {
                ((CallableStatement) this.wrappedStmt).setObject(parameterName, x);
            } else {
                throw SQLError.createSQLException(Messages.getString("Statement.AlreadyClosed"), MysqlErrorNumbers.SQLSTATE_CONNJ_GENERAL_ERROR,
                        this.exceptionInterceptor);
            }
        } catch (SQLException sqlEx) {
            checkAndFireConnectionError(sqlEx);
        }
    }

    @Override
    public void setCharacterStream(String parameterName, Reader reader, int length) throws SQLException {
        try {
            if (this.wrappedStmt != null) {
                ((CallableStatement) this.wrappedStmt).setCharacterStream(parameterName, reader, length);
            } else {
                throw SQLError.createSQLException(Messages.getString("Statement.AlreadyClosed"), MysqlErrorNumbers.SQLSTATE_CONNJ_GENERAL_ERROR,
                        this.exceptionInterceptor);
            }
        } catch (SQLException sqlEx) {
            checkAndFireConnectionError(sqlEx);
        }
    }

    @Override
    public void setDate(String parameterName, Date x, Calendar cal) throws SQLException {
        try {
            if (this.wrappedStmt != null) {
                ((CallableStatement) this.wrappedStmt).setDate(parameterName, x, cal);
            } else {
                throw SQLError.createSQLException(Messages.getString("Statement.AlreadyClosed"), MysqlErrorNumbers.SQLSTATE_CONNJ_GENERAL_ERROR,
                        this.exceptionInterceptor);
            }
        } catch (SQLException sqlEx) {
            checkAndFireConnectionError(sqlEx);
        }
    }

    @Override
    public void setTime(String parameterName, Time x, Calendar cal) throws SQLException {
        try {
            if (this.wrappedStmt != null) {
                ((CallableStatement) this.wrappedStmt).setTime(parameterName, x, cal);
            } else {
                throw SQLError.createSQLException(Messages.getString("Statement.AlreadyClosed"), MysqlErrorNumbers.SQLSTATE_CONNJ_GENERAL_ERROR,
                        this.exceptionInterceptor);
            }
        } catch (SQLException sqlEx) {
            checkAndFireConnectionError(sqlEx);
        }
    }

    @Override
    public void setTimestamp(String parameterName, Timestamp x, Calendar cal) throws SQLException {
        try {
            if (this.wrappedStmt != null) {
                ((CallableStatement) this.wrappedStmt).setTimestamp(parameterName, x, cal);
            } else {
                throw SQLError.createSQLException(Messages.getString("Statement.AlreadyClosed"), MysqlErrorNumbers.SQLSTATE_CONNJ_GENERAL_ERROR,
                        this.exceptionInterceptor);
            }
        } catch (SQLException sqlEx) {
            checkAndFireConnectionError(sqlEx);
        }
    }

    @Override
    public void setNull(String parameterName, int sqlType, String typeName) throws SQLException {
        try {
            if (this.wrappedStmt != null) {
                ((CallableStatement) this.wrappedStmt).setNull(parameterName, sqlType, typeName);
            } else {
                throw SQLError.createSQLException(Messages.getString("Statement.AlreadyClosed"), MysqlErrorNumbers.SQLSTATE_CONNJ_GENERAL_ERROR,
                        this.exceptionInterceptor);
            }
        } catch (SQLException sqlEx) {
            checkAndFireConnectionError(sqlEx);
        }
    }

    @Override
    public String getString(String parameterName) throws SQLException {
        try {
            if (this.wrappedStmt != null) {
                return ((CallableStatement) this.wrappedStmt).getString(parameterName);
            }
            throw SQLError.createSQLException(Messages.getString("Statement.AlreadyClosed"), MysqlErrorNumbers.SQLSTATE_CONNJ_GENERAL_ERROR,
                    this.exceptionInterceptor);

        } catch (SQLException sqlEx) {
            checkAndFireConnectionError(sqlEx);
        }
        return null;
    }

    @Override
    public boolean getBoolean(String parameterName) throws SQLException {
        try {
            if (this.wrappedStmt != null) {
                return ((CallableStatement) this.wrappedStmt).getBoolean(parameterName);
            }
            throw SQLError.createSQLException(Messages.getString("Statement.AlreadyClosed"), MysqlErrorNumbers.SQLSTATE_CONNJ_GENERAL_ERROR,
                    this.exceptionInterceptor);

        } catch (SQLException sqlEx) {
            checkAndFireConnectionError(sqlEx);
        }

        return false;
    }

    @Override
    public byte getByte(String parameterName) throws SQLException {
        try {
            if (this.wrappedStmt != null) {
                return ((CallableStatement) this.wrappedStmt).getByte(parameterName);
            }
            throw SQLError.createSQLException(Messages.getString("Statement.AlreadyClosed"), MysqlErrorNumbers.SQLSTATE_CONNJ_GENERAL_ERROR,
                    this.exceptionInterceptor);

        } catch (SQLException sqlEx) {
            checkAndFireConnectionError(sqlEx);
        }

        return 0;
    }

    @Override
    public short getShort(String parameterName) throws SQLException {
        try {
            if (this.wrappedStmt != null) {
                return ((CallableStatement) this.wrappedStmt).getShort(parameterName);
            }
            throw SQLError.createSQLException(Messages.getString("Statement.AlreadyClosed"), MysqlErrorNumbers.SQLSTATE_CONNJ_GENERAL_ERROR,
                    this.exceptionInterceptor);

        } catch (SQLException sqlEx) {
            checkAndFireConnectionError(sqlEx);
        }

        return 0;
    }

    @Override
    public int getInt(String parameterName) throws SQLException {
        try {
            if (this.wrappedStmt != null) {
                return ((CallableStatement) this.wrappedStmt).getInt(parameterName);
            }
            throw SQLError.createSQLException(Messages.getString("Statement.AlreadyClosed"), MysqlErrorNumbers.SQLSTATE_CONNJ_GENERAL_ERROR,
                    this.exceptionInterceptor);

        } catch (SQLException sqlEx) {
            checkAndFireConnectionError(sqlEx);
        }

        return 0;
    }

    @Override
    public long getLong(String parameterName) throws SQLException {
        try {
            if (this.wrappedStmt != null) {
                return ((CallableStatement) this.wrappedStmt).getLong(parameterName);
            }
            throw SQLError.createSQLException(Messages.getString("Statement.AlreadyClosed"), MysqlErrorNumbers.SQLSTATE_CONNJ_GENERAL_ERROR,
                    this.exceptionInterceptor);

        } catch (SQLException sqlEx) {
            checkAndFireConnectionError(sqlEx);
        }

        return 0;
    }

    @Override
    public float getFloat(String parameterName) throws SQLException {
        try {
            if (this.wrappedStmt != null) {
                return ((CallableStatement) this.wrappedStmt).getFloat(parameterName);
            }
            throw SQLError.createSQLException(Messages.getString("Statement.AlreadyClosed"), MysqlErrorNumbers.SQLSTATE_CONNJ_GENERAL_ERROR,
                    this.exceptionInterceptor);

        } catch (SQLException sqlEx) {
            checkAndFireConnectionError(sqlEx);
        }

        return 0;
    }

    @Override
    public double getDouble(String parameterName) throws SQLException {
        try {
            if (this.wrappedStmt != null) {
                return ((CallableStatement) this.wrappedStmt).getDouble(parameterName);
            }
            throw SQLError.createSQLException(Messages.getString("Statement.AlreadyClosed"), MysqlErrorNumbers.SQLSTATE_CONNJ_GENERAL_ERROR,
                    this.exceptionInterceptor);

        } catch (SQLException sqlEx) {
            checkAndFireConnectionError(sqlEx);
        }

        return 0;
    }

    @Override
    public byte[] getBytes(String parameterName) throws SQLException {
        try {
            if (this.wrappedStmt != null) {
                return ((CallableStatement) this.wrappedStmt).getBytes(parameterName);
            }
            throw SQLError.createSQLException(Messages.getString("Statement.AlreadyClosed"), MysqlErrorNumbers.SQLSTATE_CONNJ_GENERAL_ERROR,
                    this.exceptionInterceptor);

        } catch (SQLException sqlEx) {
            checkAndFireConnectionError(sqlEx);
        }

        return null;
    }

    @Override
    public Date getDate(String parameterName) throws SQLException {
        try {
            if (this.wrappedStmt != null) {
                return ((CallableStatement) this.wrappedStmt).getDate(parameterName);
            }
            throw SQLError.createSQLException(Messages.getString("Statement.AlreadyClosed"), MysqlErrorNumbers.SQLSTATE_CONNJ_GENERAL_ERROR,
                    this.exceptionInterceptor);

        } catch (SQLException sqlEx) {
            checkAndFireConnectionError(sqlEx);
        }

        return null;
    }

    @Override
    public Time getTime(String parameterName) throws SQLException {
        try {
            if (this.wrappedStmt != null) {
                return ((CallableStatement) this.wrappedStmt).getTime(parameterName);
            }
            throw SQLError.createSQLException(Messages.getString("Statement.AlreadyClosed"), MysqlErrorNumbers.SQLSTATE_CONNJ_GENERAL_ERROR,
                    this.exceptionInterceptor);

        } catch (SQLException sqlEx) {
            checkAndFireConnectionError(sqlEx);
        }

        return null;
    }

    @Override
    public Timestamp getTimestamp(String parameterName) throws SQLException {
        try {
            if (this.wrappedStmt != null) {
                return ((CallableStatement) this.wrappedStmt).getTimestamp(parameterName);
            }
            throw SQLError.createSQLException(Messages.getString("Statement.AlreadyClosed"), MysqlErrorNumbers.SQLSTATE_CONNJ_GENERAL_ERROR,
                    this.exceptionInterceptor);

        } catch (SQLException sqlEx) {
            checkAndFireConnectionError(sqlEx);
        }

        return null;
    }

    @Override
    public Object getObject(String parameterName) throws SQLException {
        try {
            if (this.wrappedStmt != null) {
                return ((CallableStatement) this.wrappedStmt).getObject(parameterName);
            }
            throw SQLError.createSQLException(Messages.getString("Statement.AlreadyClosed"), MysqlErrorNumbers.SQLSTATE_CONNJ_GENERAL_ERROR,
                    this.exceptionInterceptor);

        } catch (SQLException sqlEx) {
            checkAndFireConnectionError(sqlEx);
        }

        return null;
    }

    @Override
    public BigDecimal getBigDecimal(String parameterName) throws SQLException {
        try {
            if (this.wrappedStmt != null) {
                return ((CallableStatement) this.wrappedStmt).getBigDecimal(parameterName);
            }
            throw SQLError.createSQLException(Messages.getString("Statement.AlreadyClosed"), MysqlErrorNumbers.SQLSTATE_CONNJ_GENERAL_ERROR,
                    this.exceptionInterceptor);

        } catch (SQLException sqlEx) {
            checkAndFireConnectionError(sqlEx);
        }

        return null;
    }

    @Override
    public Object getObject(String parameterName, Map<String, Class<?>> typeMap) throws SQLException {
        try {
            if (this.wrappedStmt != null) {
                return ((CallableStatement) this.wrappedStmt).getObject(parameterName, typeMap);
            }
            throw SQLError.createSQLException(Messages.getString("Statement.AlreadyClosed"), MysqlErrorNumbers.SQLSTATE_CONNJ_GENERAL_ERROR,
                    this.exceptionInterceptor);

        } catch (SQLException sqlEx) {
            checkAndFireConnectionError(sqlEx);
        }
        return null;
    }

    @Override
    public Ref getRef(String parameterName) throws SQLException {
        try {
            if (this.wrappedStmt != null) {
                return ((CallableStatement) this.wrappedStmt).getRef(parameterName);
            }
            throw SQLError.createSQLException(Messages.getString("Statement.AlreadyClosed"), MysqlErrorNumbers.SQLSTATE_CONNJ_GENERAL_ERROR,
                    this.exceptionInterceptor);

        } catch (SQLException sqlEx) {
            checkAndFireConnectionError(sqlEx);
        }

        return null;
    }

    @Override
    public Blob getBlob(String parameterName) throws SQLException {
        try {
            if (this.wrappedStmt != null) {
                return ((CallableStatement) this.wrappedStmt).getBlob(parameterName);
            }
            throw SQLError.createSQLException(Messages.getString("Statement.AlreadyClosed"), MysqlErrorNumbers.SQLSTATE_CONNJ_GENERAL_ERROR,
                    this.exceptionInterceptor);

        } catch (SQLException sqlEx) {
            checkAndFireConnectionError(sqlEx);
        }

        return null;
    }

    @Override
    public Clob getClob(String parameterName) throws SQLException {
        try {
            if (this.wrappedStmt != null) {
                return ((CallableStatement) this.wrappedStmt).getClob(parameterName);
            }
            throw SQLError.createSQLException(Messages.getString("Statement.AlreadyClosed"), MysqlErrorNumbers.SQLSTATE_CONNJ_GENERAL_ERROR,
                    this.exceptionInterceptor);

        } catch (SQLException sqlEx) {
            checkAndFireConnectionError(sqlEx);
        }
        return null;
    }

    @Override
    public Array getArray(String parameterName) throws SQLException {
        try {
            if (this.wrappedStmt != null) {
                return ((CallableStatement) this.wrappedStmt).getArray(parameterName);
            }
            throw SQLError.createSQLException(Messages.getString("Statement.AlreadyClosed"), MysqlErrorNumbers.SQLSTATE_CONNJ_GENERAL_ERROR,
                    this.exceptionInterceptor);

        } catch (SQLException sqlEx) {
            checkAndFireConnectionError(sqlEx);
        }
        return null;
    }

    @Override
    public Date getDate(String parameterName, Calendar cal) throws SQLException {
        try {
            if (this.wrappedStmt != null) {
                return ((CallableStatement) this.wrappedStmt).getDate(parameterName, cal);
            }
            throw SQLError.createSQLException(Messages.getString("Statement.AlreadyClosed"), MysqlErrorNumbers.SQLSTATE_CONNJ_GENERAL_ERROR,
                    this.exceptionInterceptor);

        } catch (SQLException sqlEx) {
            checkAndFireConnectionError(sqlEx);
        }
        return null;
    }

    @Override
    public Time getTime(String parameterName, Calendar cal) throws SQLException {
        try {
            if (this.wrappedStmt != null) {
                return ((CallableStatement) this.wrappedStmt).getTime(parameterName, cal);
            }
            throw SQLError.createSQLException(Messages.getString("Statement.AlreadyClosed"), MysqlErrorNumbers.SQLSTATE_CONNJ_GENERAL_ERROR,
                    this.exceptionInterceptor);

        } catch (SQLException sqlEx) {
            checkAndFireConnectionError(sqlEx);
        }
        return null;
    }

    @Override
    public Timestamp getTimestamp(String parameterName, Calendar cal) throws SQLException {
        try {
            if (this.wrappedStmt != null) {
                return ((CallableStatement) this.wrappedStmt).getTimestamp(parameterName, cal);
            }
            throw SQLError.createSQLException(Messages.getString("Statement.AlreadyClosed"), MysqlErrorNumbers.SQLSTATE_CONNJ_GENERAL_ERROR,
                    this.exceptionInterceptor);

        } catch (SQLException sqlEx) {
            checkAndFireConnectionError(sqlEx);
        }
        return null;
    }

    @Override
    public URL getURL(String parameterName) throws SQLException {
        try {
            if (this.wrappedStmt != null) {
                return ((CallableStatement) this.wrappedStmt).getURL(parameterName);
            }
            throw SQLError.createSQLException(Messages.getString("Statement.AlreadyClosed"), MysqlErrorNumbers.SQLSTATE_CONNJ_GENERAL_ERROR,
                    this.exceptionInterceptor);

        } catch (SQLException sqlEx) {
            checkAndFireConnectionError(sqlEx);
        }

        return null;
    }

    @Override
    public RowId getRowId(String parameterName) throws SQLException {
        try {
            if (this.wrappedStmt != null) {
                return ((CallableStatement) this.wrappedStmt).getRowId(parameterName);
            }
            throw SQLError.createSQLException(Messages.getString("Statement.AlreadyClosed"), MysqlErrorNumbers.SQLSTATE_CONNJ_GENERAL_ERROR,
                    this.exceptionInterceptor);
        } catch (SQLException sqlEx) {
            checkAndFireConnectionError(sqlEx);
        }

        return null;
    }

    @Override
    public RowId getRowId(int parameterIndex) throws SQLException {
        try {
            if (this.wrappedStmt != null) {
                return ((CallableStatement) this.wrappedStmt).getRowId(parameterIndex);
            }
            throw SQLError.createSQLException(Messages.getString("Statement.AlreadyClosed"), MysqlErrorNumbers.SQLSTATE_CONNJ_GENERAL_ERROR,
                    this.exceptionInterceptor);
        } catch (SQLException sqlEx) {
            checkAndFireConnectionError(sqlEx);
        }

        return null;
    }

    @Override
    public void setRowId(String parameterName, RowId x) throws SQLException {
        try {
            if (this.wrappedStmt != null) {
                ((CallableStatement) this.wrappedStmt).setRowId(parameterName, x);
            } else {
                throw SQLError.createSQLException(Messages.getString("Statement.AlreadyClosed"), MysqlErrorNumbers.SQLSTATE_CONNJ_GENERAL_ERROR,
                        this.exceptionInterceptor);
            }
        } catch (SQLException sqlEx) {
            checkAndFireConnectionError(sqlEx);
        }
    }

    @Override
    public void setNString(String parameterName, String value) throws SQLException {
        try {
            if (this.wrappedStmt != null) {
                ((CallableStatement) this.wrappedStmt).setNString(parameterName, value);
            } else {
                throw SQLError.createSQLException(Messages.getString("Statement.AlreadyClosed"), MysqlErrorNumbers.SQLSTATE_CONNJ_GENERAL_ERROR,
                        this.exceptionInterceptor);
            }
        } catch (SQLException sqlEx) {
            checkAndFireConnectionError(sqlEx);
        }
    }

    @Override
    public void setNCharacterStream(String parameterName, Reader reader, long length) throws SQLException {
        try {
            if (this.wrappedStmt != null) {
                ((CallableStatement) this.wrappedStmt).setNCharacterStream(parameterName, reader, length);
            } else {
                throw SQLError.createSQLException(Messages.getString("Statement.AlreadyClosed"), MysqlErrorNumbers.SQLSTATE_CONNJ_GENERAL_ERROR,
                        this.exceptionInterceptor);
            }
        } catch (SQLException sqlEx) {
            checkAndFireConnectionError(sqlEx);
        }
    }

    @Override
    public void setNClob(String parameterName, NClob value) throws SQLException {
        try {
            if (this.wrappedStmt != null) {
                ((CallableStatement) this.wrappedStmt).setNClob(parameterName, value);
            } else {
                throw SQLError.createSQLException(Messages.getString("Statement.AlreadyClosed"), MysqlErrorNumbers.SQLSTATE_CONNJ_GENERAL_ERROR,
                        this.exceptionInterceptor);
            }
        } catch (SQLException sqlEx) {
            checkAndFireConnectionError(sqlEx);
        }
    }

    @Override
    public void setClob(String parameterName, Reader reader, long length) throws SQLException {
        try {
            if (this.wrappedStmt != null) {
                ((CallableStatement) this.wrappedStmt).setClob(parameterName, reader, length);
            } else {
                throw SQLError.createSQLException(Messages.getString("Statement.AlreadyClosed"), MysqlErrorNumbers.SQLSTATE_CONNJ_GENERAL_ERROR,
                        this.exceptionInterceptor);
            }
        } catch (SQLException sqlEx) {
            checkAndFireConnectionError(sqlEx);
        }
    }

    @Override
    public void setBlob(String parameterName, InputStream x, long length) throws SQLException {
        try {
            if (this.wrappedStmt != null) {
                ((CallableStatement) this.wrappedStmt).setBlob(parameterName, x, length);
            } else {
                throw SQLError.createSQLException(Messages.getString("Statement.AlreadyClosed"), MysqlErrorNumbers.SQLSTATE_CONNJ_GENERAL_ERROR,
                        this.exceptionInterceptor);
            }
        } catch (SQLException sqlEx) {
            checkAndFireConnectionError(sqlEx);
        }
    }

    @Override
    public void setNClob(String parameterName, Reader reader, long length) throws SQLException {
        try {
            if (this.wrappedStmt != null) {
                ((CallableStatement) this.wrappedStmt).setNClob(parameterName, reader, length);
            } else {
                throw SQLError.createSQLException(Messages.getString("Statement.AlreadyClosed"), MysqlErrorNumbers.SQLSTATE_CONNJ_GENERAL_ERROR,
                        this.exceptionInterceptor);
            }
        } catch (SQLException sqlEx) {
            checkAndFireConnectionError(sqlEx);
        }
    }

    @Override
    public NClob getNClob(String parameterName) throws SQLException {
        try {
            if (this.wrappedStmt != null) {
                return ((CallableStatement) this.wrappedStmt).getNClob(parameterName);
            }
            throw SQLError.createSQLException(Messages.getString("Statement.AlreadyClosed"), MysqlErrorNumbers.SQLSTATE_CONNJ_GENERAL_ERROR,
                    this.exceptionInterceptor);
        } catch (SQLException sqlEx) {
            checkAndFireConnectionError(sqlEx);
        }

        return null;
    }

    @Override
    public NClob getNClob(int parameterIndex) throws SQLException {
        try {
            if (this.wrappedStmt != null) {
                return ((CallableStatement) this.wrappedStmt).getNClob(parameterIndex);
            }
            throw SQLError.createSQLException(Messages.getString("Statement.AlreadyClosed"), MysqlErrorNumbers.SQLSTATE_CONNJ_GENERAL_ERROR,
                    this.exceptionInterceptor);
        } catch (SQLException sqlEx) {
            checkAndFireConnectionError(sqlEx);
        }

        return null;
    }

    @Override
    public void setSQLXML(String parameterName, SQLXML xmlObject) throws SQLException {
        try {
            if (this.wrappedStmt != null) {
                ((CallableStatement) this.wrappedStmt).setSQLXML(parameterName, xmlObject);
            } else {
                throw SQLError.createSQLException(Messages.getString("Statement.AlreadyClosed"), MysqlErrorNumbers.SQLSTATE_CONNJ_GENERAL_ERROR,
                        this.exceptionInterceptor);
            }
        } catch (SQLException sqlEx) {
            checkAndFireConnectionError(sqlEx);
        }
    }

    @Override
    public SQLXML getSQLXML(int parameterIndex) throws SQLException {
        try {
            if (this.wrappedStmt != null) {
                return ((CallableStatement) this.wrappedStmt).getSQLXML(parameterIndex);
            }
            throw SQLError.createSQLException(Messages.getString("Statement.AlreadyClosed"), MysqlErrorNumbers.SQLSTATE_CONNJ_GENERAL_ERROR,
                    this.exceptionInterceptor);
        } catch (SQLException sqlEx) {
            checkAndFireConnectionError(sqlEx);
        }
        return null;
    }

    @Override
    public SQLXML getSQLXML(String parameterName) throws SQLException {
        try {
            if (this.wrappedStmt != null) {
                return ((CallableStatement) this.wrappedStmt).getSQLXML(parameterName);
            }
            throw SQLError.createSQLException(Messages.getString("Statement.AlreadyClosed"), MysqlErrorNumbers.SQLSTATE_CONNJ_GENERAL_ERROR,
                    this.exceptionInterceptor);
        } catch (SQLException sqlEx) {
            checkAndFireConnectionError(sqlEx);
        }
        return null;
    }

    @Override
    public String getNString(int parameterIndex) throws SQLException {
        try {
            if (this.wrappedStmt != null) {
                return ((CallableStatement) this.wrappedStmt).getNString(parameterIndex);
            }
            throw SQLError.createSQLException(Messages.getString("Statement.AlreadyClosed"), MysqlErrorNumbers.SQLSTATE_CONNJ_GENERAL_ERROR,
                    this.exceptionInterceptor);
        } catch (SQLException sqlEx) {
            checkAndFireConnectionError(sqlEx);
        }

        return null;
    }

    @Override
    public String getNString(String parameterName) throws SQLException {
        try {
            if (this.wrappedStmt != null) {
                return ((CallableStatement) this.wrappedStmt).getNString(parameterName);
            }
            throw SQLError.createSQLException(Messages.getString("Statement.AlreadyClosed"), MysqlErrorNumbers.SQLSTATE_CONNJ_GENERAL_ERROR,
                    this.exceptionInterceptor);
        } catch (SQLException sqlEx) {
            checkAndFireConnectionError(sqlEx);
        }

        return null;
    }

    @Override
    public Reader getNCharacterStream(int parameterIndex) throws SQLException {
        try {
            if (this.wrappedStmt != null) {
                return ((CallableStatement) this.wrappedStmt).getNCharacterStream(parameterIndex);
            }
            throw SQLError.createSQLException(Messages.getString("Statement.AlreadyClosed"), MysqlErrorNumbers.SQLSTATE_CONNJ_GENERAL_ERROR,
                    this.exceptionInterceptor);
        } catch (SQLException sqlEx) {
            checkAndFireConnectionError(sqlEx);
        }

        return null;
    }

    @Override
    public Reader getNCharacterStream(String parameterName) throws SQLException {
        try {
            if (this.wrappedStmt != null) {
                return ((CallableStatement) this.wrappedStmt).getNCharacterStream(parameterName);
            }
            throw SQLError.createSQLException(Messages.getString("Statement.AlreadyClosed"), MysqlErrorNumbers.SQLSTATE_CONNJ_GENERAL_ERROR,
                    this.exceptionInterceptor);
        } catch (SQLException sqlEx) {
            checkAndFireConnectionError(sqlEx);
        }

        return null;
    }

    @Override
    public Reader getCharacterStream(int parameterIndex) throws SQLException {
        try {
            if (this.wrappedStmt != null) {
                return ((CallableStatement) this.wrappedStmt).getCharacterStream(parameterIndex);
            }
            throw SQLError.createSQLException(Messages.getString("Statement.AlreadyClosed"), MysqlErrorNumbers.SQLSTATE_CONNJ_GENERAL_ERROR,
                    this.exceptionInterceptor);
        } catch (SQLException sqlEx) {
            checkAndFireConnectionError(sqlEx);
        }

        return null;
    }

    @Override
    public Reader getCharacterStream(String parameterName) throws SQLException {
        try {
            if (this.wrappedStmt != null) {
                return ((CallableStatement) this.wrappedStmt).getCharacterStream(parameterName);
            }
            throw SQLError.createSQLException(Messages.getString("Statement.AlreadyClosed"), MysqlErrorNumbers.SQLSTATE_CONNJ_GENERAL_ERROR,
                    this.exceptionInterceptor);
        } catch (SQLException sqlEx) {
            checkAndFireConnectionError(sqlEx);
        }

        return null;
    }

    @Override
    public void setBlob(String parameterName, Blob x) throws SQLException {
        try {
            if (this.wrappedStmt != null) {
                ((CallableStatement) this.wrappedStmt).setBlob(parameterName, x);
            } else {
                throw SQLError.createSQLException(Messages.getString("Statement.AlreadyClosed"), MysqlErrorNumbers.SQLSTATE_CONNJ_GENERAL_ERROR,
                        this.exceptionInterceptor);
            }
        } catch (SQLException sqlEx) {
            checkAndFireConnectionError(sqlEx);
        }
    }

    @Override
    public void setClob(String parameterName, Clob x) throws SQLException {
        try {
            if (this.wrappedStmt != null) {
                ((CallableStatement) this.wrappedStmt).setClob(parameterName, x);
            } else {
                throw SQLError.createSQLException(Messages.getString("Statement.AlreadyClosed"), MysqlErrorNumbers.SQLSTATE_CONNJ_GENERAL_ERROR,
                        this.exceptionInterceptor);
            }
        } catch (SQLException sqlEx) {
            checkAndFireConnectionError(sqlEx);
        }
    }

    @Override
    public void setAsciiStream(String parameterName, InputStream x, long length) throws SQLException {
        try {
            if (this.wrappedStmt != null) {
                ((CallableStatement) this.wrappedStmt).setAsciiStream(parameterName, x, length);
            } else {
                throw SQLError.createSQLException(Messages.getString("Statement.AlreadyClosed"), MysqlErrorNumbers.SQLSTATE_CONNJ_GENERAL_ERROR,
                        this.exceptionInterceptor);
            }
        } catch (SQLException sqlEx) {
            checkAndFireConnectionError(sqlEx);
        }
    }

    @Override
    public void setBinaryStream(String parameterName, InputStream x, long length) throws SQLException {
        try {
            if (this.wrappedStmt != null) {
                ((CallableStatement) this.wrappedStmt).setBinaryStream(parameterName, x, length);
            } else {
                throw SQLError.createSQLException(Messages.getString("Statement.AlreadyClosed"), MysqlErrorNumbers.SQLSTATE_CONNJ_GENERAL_ERROR,
                        this.exceptionInterceptor);
            }
        } catch (SQLException sqlEx) {
            checkAndFireConnectionError(sqlEx);
        }
    }

    @Override
    public void setCharacterStream(String parameterName, Reader reader, long length) throws SQLException {
        try {
            if (this.wrappedStmt != null) {
                ((CallableStatement) this.wrappedStmt).setCharacterStream(parameterName, reader, length);
            } else {
                throw SQLError.createSQLException(Messages.getString("Statement.AlreadyClosed"), MysqlErrorNumbers.SQLSTATE_CONNJ_GENERAL_ERROR,
                        this.exceptionInterceptor);
            }
        } catch (SQLException sqlEx) {
            checkAndFireConnectionError(sqlEx);
        }
    }

    @Override
    public void setAsciiStream(String parameterName, InputStream x) throws SQLException {
        try {
            if (this.wrappedStmt != null) {
                ((CallableStatement) this.wrappedStmt).setAsciiStream(parameterName, x);
            } else {
                throw SQLError.createSQLException(Messages.getString("Statement.AlreadyClosed"), MysqlErrorNumbers.SQLSTATE_CONNJ_GENERAL_ERROR,
                        this.exceptionInterceptor);
            }
        } catch (SQLException sqlEx) {
            checkAndFireConnectionError(sqlEx);
        }
    }

    @Override
    public void setBinaryStream(String parameterName, InputStream x) throws SQLException {
        try {
            if (this.wrappedStmt != null) {
                ((CallableStatement) this.wrappedStmt).setBinaryStream(parameterName, x);
            } else {
                throw SQLError.createSQLException(Messages.getString("Statement.AlreadyClosed"), MysqlErrorNumbers.SQLSTATE_CONNJ_GENERAL_ERROR,
                        this.exceptionInterceptor);
            }
        } catch (SQLException sqlEx) {
            checkAndFireConnectionError(sqlEx);
        }
    }

    @Override
    public void setCharacterStream(String parameterName, Reader reader) throws SQLException {
        try {
            if (this.wrappedStmt != null) {
                ((CallableStatement) this.wrappedStmt).setCharacterStream(parameterName, reader);
            } else {
                throw SQLError.createSQLException(Messages.getString("Statement.AlreadyClosed"), MysqlErrorNumbers.SQLSTATE_CONNJ_GENERAL_ERROR,
                        this.exceptionInterceptor);
            }
        } catch (SQLException sqlEx) {
            checkAndFireConnectionError(sqlEx);
        }
    }

    @Override
    public void setNCharacterStream(String parameterName, Reader reader) throws SQLException {
        try {
            if (this.wrappedStmt != null) {
                ((CallableStatement) this.wrappedStmt).setNCharacterStream(parameterName, reader);
            } else {
                throw SQLError.createSQLException(Messages.getString("Statement.AlreadyClosed"), MysqlErrorNumbers.SQLSTATE_CONNJ_GENERAL_ERROR,
                        this.exceptionInterceptor);
            }
        } catch (SQLException sqlEx) {
            checkAndFireConnectionError(sqlEx);
        }
    }

    @Override
    public void setClob(String parameterName, Reader reader) throws SQLException {
        try {
            if (this.wrappedStmt != null) {
                ((CallableStatement) this.wrappedStmt).setClob(parameterName, reader);
            } else {
                throw SQLError.createSQLException(Messages.getString("Statement.AlreadyClosed"), MysqlErrorNumbers.SQLSTATE_CONNJ_GENERAL_ERROR,
                        this.exceptionInterceptor);
            }
        } catch (SQLException sqlEx) {
            checkAndFireConnectionError(sqlEx);
        }
    }

    @Override
    public void setBlob(String parameterName, InputStream x) throws SQLException {
        try {
            if (this.wrappedStmt != null) {
                ((CallableStatement) this.wrappedStmt).setBlob(parameterName, x);
            } else {
                throw SQLError.createSQLException(Messages.getString("Statement.AlreadyClosed"), MysqlErrorNumbers.SQLSTATE_CONNJ_GENERAL_ERROR,
                        this.exceptionInterceptor);
            }
        } catch (SQLException sqlEx) {
            checkAndFireConnectionError(sqlEx);
        }
    }

    @Override
    public void setNClob(String parameterName, Reader reader) throws SQLException {
        try {
            if (this.wrappedStmt != null) {
                ((CallableStatement) this.wrappedStmt).setNClob(parameterName, reader);
            } else {
                throw SQLError.createSQLException(Messages.getString("Statement.AlreadyClosed"), MysqlErrorNumbers.SQLSTATE_CONNJ_GENERAL_ERROR,
                        this.exceptionInterceptor);
            }
        } catch (SQLException sqlEx) {
            checkAndFireConnectionError(sqlEx);
        }
    }

    @Override
    public <T> T getObject(int parameterIndex, Class<T> type) throws SQLException {
        if (this.wrappedStmt != null) {
            return null; // TODO
        }
        throw SQLError.createSQLException(Messages.getString("Statement.AlreadyClosed"), MysqlErrorNumbers.SQLSTATE_CONNJ_GENERAL_ERROR,
                this.exceptionInterceptor);
    }

    @Override
    public <T> T getObject(String parameterName, Class<T> type) throws SQLException {
        if (this.wrappedStmt != null) {
            return null; // TODO
        }
        throw SQLError.createSQLException(Messages.getString("Statement.AlreadyClosed"), MysqlErrorNumbers.SQLSTATE_CONNJ_GENERAL_ERROR,
                this.exceptionInterceptor);
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        boolean isInstance = iface.isInstance(this);

        if (isInstance) {
            return true;
        }

        String interfaceClassName = iface.getName();

        return interfaceClassName.equals("com.mysql.cj.jdbc.Statement") || interfaceClassName.equals("java.sql.Statement")
                || interfaceClassName.equals("java.sql.Wrapper") || interfaceClassName.equals("java.sql.PreparedStatement")
                || interfaceClassName.equals("java.sql.CallableStatement");
    }

    @Override
    public void close() throws SQLException {
        try {
            super.close();
        } finally {
            this.unwrappedInterfaces = null;
        }
    }

    @Override
    public <T> T unwrap(java.lang.Class<T> iface) throws java.sql.SQLException {
        this.lock.lock();
        try {
            if ("java.sql.Statement".equals(iface.getName()) || "java.sql.CallableStatement".equals(iface.getName())
                    || "java.sql.PreparedStatement".equals(iface.getName()) || "java.sql.Wrapper.class".equals(iface.getName())) {
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
                    MysqlErrorNumbers.SQLSTATE_CONNJ_ILLEGAL_ARGUMENT, this.exceptionInterceptor);
        } finally {
            this.lock.unlock();
        }
    }

    @Override
    public void registerOutParameter(int parameterIndex, SQLType sqlType) throws SQLException {
        try {
            if (this.wrappedStmt != null) {
                ((CallableStatement) this.wrappedStmt).registerOutParameter(parameterIndex, sqlType);
            } else {
                throw SQLError.createSQLException(Messages.getString("Statement.AlreadyClosed"), MysqlErrorNumbers.SQLSTATE_CONNJ_GENERAL_ERROR,
                        this.exceptionInterceptor);
            }
        } catch (SQLException sqlEx) {
            checkAndFireConnectionError(sqlEx);
        }
    }

    @Override
    public void registerOutParameter(int parameterIndex, SQLType sqlType, int scale) throws SQLException {
        try {
            if (this.wrappedStmt != null) {
                ((CallableStatement) this.wrappedStmt).registerOutParameter(parameterIndex, sqlType, scale);
            } else {
                throw SQLError.createSQLException(Messages.getString("Statement.AlreadyClosed"), MysqlErrorNumbers.SQLSTATE_CONNJ_GENERAL_ERROR,
                        this.exceptionInterceptor);
            }
        } catch (SQLException sqlEx) {
            checkAndFireConnectionError(sqlEx);
        }
    }

    @Override
    public void registerOutParameter(int parameterIndex, SQLType sqlType, String typeName) throws SQLException {
        try {
            if (this.wrappedStmt != null) {
                ((CallableStatement) this.wrappedStmt).registerOutParameter(parameterIndex, sqlType, typeName);
            } else {
                throw SQLError.createSQLException(Messages.getString("Statement.AlreadyClosed"), MysqlErrorNumbers.SQLSTATE_CONNJ_GENERAL_ERROR,
                        this.exceptionInterceptor);
            }
        } catch (SQLException sqlEx) {
            checkAndFireConnectionError(sqlEx);
        }
    }

    @Override
    public void registerOutParameter(String parameterName, SQLType sqlType) throws SQLException {
        try {
            if (this.wrappedStmt != null) {
                ((CallableStatement) this.wrappedStmt).registerOutParameter(parameterName, sqlType);
            } else {
                throw SQLError.createSQLException(Messages.getString("Statement.AlreadyClosed"), MysqlErrorNumbers.SQLSTATE_CONNJ_GENERAL_ERROR,
                        this.exceptionInterceptor);
            }
        } catch (SQLException sqlEx) {
            checkAndFireConnectionError(sqlEx);
        }
    }

    @Override
    public void registerOutParameter(String parameterName, SQLType sqlType, int scale) throws SQLException {
        try {
            if (this.wrappedStmt != null) {
                ((CallableStatement) this.wrappedStmt).registerOutParameter(parameterName, sqlType, scale);
            } else {
                throw SQLError.createSQLException(Messages.getString("Statement.AlreadyClosed"), MysqlErrorNumbers.SQLSTATE_CONNJ_GENERAL_ERROR,
                        this.exceptionInterceptor);
            }
        } catch (SQLException sqlEx) {
            checkAndFireConnectionError(sqlEx);
        }
    }

    @Override
    public void registerOutParameter(String parameterName, SQLType sqlType, String typeName) throws SQLException {
        try {
            if (this.wrappedStmt != null) {
                ((CallableStatement) this.wrappedStmt).registerOutParameter(parameterName, sqlType, typeName);
            } else {
                throw SQLError.createSQLException(Messages.getString("Statement.AlreadyClosed"), MysqlErrorNumbers.SQLSTATE_CONNJ_GENERAL_ERROR,
                        this.exceptionInterceptor);
            }
        } catch (SQLException sqlEx) {
            checkAndFireConnectionError(sqlEx);
        }
    }

    @Override
    public void setObject(int parameterIndex, Object x, SQLType targetSqlType) throws SQLException {
        try {
            if (this.wrappedStmt != null) {
                ((CallableStatement) this.wrappedStmt).setObject(parameterIndex, x, targetSqlType);
            } else {
                throw SQLError.createSQLException(Messages.getString("Statement.AlreadyClosed"), MysqlErrorNumbers.SQLSTATE_CONNJ_GENERAL_ERROR,
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
                ((CallableStatement) this.wrappedStmt).setObject(parameterIndex, x, targetSqlType, scaleOrLength);
            } else {
                throw SQLError.createSQLException(Messages.getString("Statement.AlreadyClosed"), MysqlErrorNumbers.SQLSTATE_CONNJ_GENERAL_ERROR,
                        this.exceptionInterceptor);
            }
        } catch (SQLException sqlEx) {
            checkAndFireConnectionError(sqlEx);
        }
    }

    @Override
    public void setObject(String parameterName, Object x, SQLType targetSqlType) throws SQLException {
        try {
            if (this.wrappedStmt != null) {
                ((CallableStatement) this.wrappedStmt).setObject(parameterName, x, targetSqlType);
            } else {
                throw SQLError.createSQLException(Messages.getString("Statement.AlreadyClosed"), MysqlErrorNumbers.SQLSTATE_CONNJ_GENERAL_ERROR,
                        this.exceptionInterceptor);
            }
        } catch (SQLException sqlEx) {
            checkAndFireConnectionError(sqlEx);
        }
    }

    @Override
    public void setObject(String parameterName, Object x, SQLType targetSqlType, int scaleOrLength) throws SQLException {
        try {
            if (this.wrappedStmt != null) {
                ((CallableStatement) this.wrappedStmt).setObject(parameterName, x, targetSqlType, scaleOrLength);
            } else {
                throw SQLError.createSQLException(Messages.getString("Statement.AlreadyClosed"), MysqlErrorNumbers.SQLSTATE_CONNJ_GENERAL_ERROR,
                        this.exceptionInterceptor);
            }
        } catch (SQLException sqlEx) {
            checkAndFireConnectionError(sqlEx);
        }
    }

}
