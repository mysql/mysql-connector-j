/*
 * Copyright (c) 2002, 2020, Oracle and/or its affiliates.
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

import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.util.HashMap;

import com.mysql.cj.Messages;
import com.mysql.cj.exceptions.MysqlErrorNumbers;
import com.mysql.cj.jdbc.exceptions.SQLError;

/**
 * Wraps statements so that errors can be reported correctly to ConnectionEventListeners.
 */
public class StatementWrapper extends WrapperBase implements Statement {

    protected static StatementWrapper getInstance(ConnectionWrapper c, MysqlPooledConnection conn, Statement toWrap) throws SQLException {
        return new StatementWrapper(c, conn, toWrap);
    }

    protected Statement wrappedStmt;

    protected ConnectionWrapper wrappedConn;

    public StatementWrapper(ConnectionWrapper c, MysqlPooledConnection conn, Statement toWrap) {
        super(conn);
        this.wrappedStmt = toWrap;
        this.wrappedConn = c;
    }

    @Override
    public Connection getConnection() throws SQLException {
        try {
            if (this.wrappedStmt != null) {
                return this.wrappedConn;
            }

            throw SQLError.createSQLException(Messages.getString("Statement.AlreadyClosed"), MysqlErrorNumbers.SQL_STATE_ILLEGAL_ARGUMENT,
                    this.exceptionInterceptor);
        } catch (SQLException sqlEx) {
            checkAndFireConnectionError(sqlEx);
        }

        return null; // we actually never get here, but the compiler can't figure that out
    }

    @Override
    public void setCursorName(String name) throws SQLException {
        try {
            if (this.wrappedStmt != null) {
                this.wrappedStmt.setCursorName(name);
            } else {
                throw SQLError.createSQLException(Messages.getString("Statement.AlreadyClosed"), MysqlErrorNumbers.SQL_STATE_ILLEGAL_ARGUMENT,
                        this.exceptionInterceptor);
            }
        } catch (SQLException sqlEx) {
            checkAndFireConnectionError(sqlEx);
        }
    }

    @Override
    public void setEscapeProcessing(boolean enable) throws SQLException {
        try {
            if (this.wrappedStmt != null) {
                this.wrappedStmt.setEscapeProcessing(enable);
            } else {
                throw SQLError.createSQLException(Messages.getString("Statement.AlreadyClosed"), MysqlErrorNumbers.SQL_STATE_ILLEGAL_ARGUMENT,
                        this.exceptionInterceptor);
            }
        } catch (SQLException sqlEx) {
            checkAndFireConnectionError(sqlEx);
        }
    }

    @Override
    public void setFetchDirection(int direction) throws SQLException {
        try {
            if (this.wrappedStmt != null) {
                this.wrappedStmt.setFetchDirection(direction);
            } else {
                throw SQLError.createSQLException(Messages.getString("Statement.AlreadyClosed"), MysqlErrorNumbers.SQL_STATE_ILLEGAL_ARGUMENT,
                        this.exceptionInterceptor);
            }
        } catch (SQLException sqlEx) {
            checkAndFireConnectionError(sqlEx);
        }
    }

    @Override
    public int getFetchDirection() throws SQLException {
        try {
            if (this.wrappedStmt != null) {
                return this.wrappedStmt.getFetchDirection();
            }

            throw SQLError.createSQLException(Messages.getString("Statement.AlreadyClosed"), MysqlErrorNumbers.SQL_STATE_ILLEGAL_ARGUMENT,
                    this.exceptionInterceptor);
        } catch (SQLException sqlEx) {
            checkAndFireConnectionError(sqlEx);
        }

        return ResultSet.FETCH_FORWARD; // we actually never get here, but the compiler can't figure that out
    }

    @Override
    public void setFetchSize(int rows) throws SQLException {
        try {
            if (this.wrappedStmt != null) {
                this.wrappedStmt.setFetchSize(rows);
            } else {
                throw SQLError.createSQLException(Messages.getString("Statement.AlreadyClosed"), MysqlErrorNumbers.SQL_STATE_ILLEGAL_ARGUMENT,
                        this.exceptionInterceptor);
            }
        } catch (SQLException sqlEx) {
            checkAndFireConnectionError(sqlEx);
        }
    }

    @Override
    public int getFetchSize() throws SQLException {
        try {
            if (this.wrappedStmt != null) {
                return this.wrappedStmt.getFetchSize();
            }

            throw SQLError.createSQLException(Messages.getString("Statement.AlreadyClosed"), MysqlErrorNumbers.SQL_STATE_ILLEGAL_ARGUMENT,
                    this.exceptionInterceptor);
        } catch (SQLException sqlEx) {
            checkAndFireConnectionError(sqlEx);
        }

        return 0; // we actually never get here, but the compiler can't figure that out
    }

    @Override
    public ResultSet getGeneratedKeys() throws SQLException {
        try {
            if (this.wrappedStmt != null) {
                return this.wrappedStmt.getGeneratedKeys();
            }

            throw SQLError.createSQLException(Messages.getString("Statement.AlreadyClosed"), MysqlErrorNumbers.SQL_STATE_ILLEGAL_ARGUMENT,
                    this.exceptionInterceptor);
        } catch (SQLException sqlEx) {
            checkAndFireConnectionError(sqlEx);
        }

        return null; // we actually never get here, but the compiler can't figure that out
    }

    @Override
    public void setMaxFieldSize(int max) throws SQLException {
        try {
            if (this.wrappedStmt != null) {
                this.wrappedStmt.setMaxFieldSize(max);
            } else {
                throw SQLError.createSQLException(Messages.getString("Statement.AlreadyClosed"), MysqlErrorNumbers.SQL_STATE_ILLEGAL_ARGUMENT,
                        this.exceptionInterceptor);
            }
        } catch (SQLException sqlEx) {
            checkAndFireConnectionError(sqlEx);
        }
    }

    @Override
    public int getMaxFieldSize() throws SQLException {
        try {
            if (this.wrappedStmt != null) {
                return this.wrappedStmt.getMaxFieldSize();
            }

            throw SQLError.createSQLException(Messages.getString("Statement.AlreadyClosed"), MysqlErrorNumbers.SQL_STATE_ILLEGAL_ARGUMENT,
                    this.exceptionInterceptor);
        } catch (SQLException sqlEx) {
            checkAndFireConnectionError(sqlEx);
        }

        return 0; // we actually never get here, but the compiler can't figure that out
    }

    @Override
    public void setMaxRows(int max) throws SQLException {
        try {
            if (this.wrappedStmt != null) {
                this.wrappedStmt.setMaxRows(max);
            } else {
                throw SQLError.createSQLException(Messages.getString("Statement.AlreadyClosed"), MysqlErrorNumbers.SQL_STATE_ILLEGAL_ARGUMENT,
                        this.exceptionInterceptor);
            }
        } catch (SQLException sqlEx) {
            checkAndFireConnectionError(sqlEx);
        }
    }

    @Override
    public int getMaxRows() throws SQLException {
        try {
            if (this.wrappedStmt != null) {
                return this.wrappedStmt.getMaxRows();
            }

            throw SQLError.createSQLException(Messages.getString("Statement.AlreadyClosed"), MysqlErrorNumbers.SQL_STATE_ILLEGAL_ARGUMENT,
                    this.exceptionInterceptor);
        } catch (SQLException sqlEx) {
            checkAndFireConnectionError(sqlEx);
        }

        return 0; // we actually never get here, but the compiler can't figure that out
    }

    @Override
    public boolean getMoreResults() throws SQLException {
        try {
            if (this.wrappedStmt != null) {
                return this.wrappedStmt.getMoreResults();
            }

            throw SQLError.createSQLException(Messages.getString("Statement.AlreadyClosed"), MysqlErrorNumbers.SQL_STATE_ILLEGAL_ARGUMENT,
                    this.exceptionInterceptor);
        } catch (SQLException sqlEx) {
            checkAndFireConnectionError(sqlEx);
        }

        return false;
    }

    @Override
    public boolean getMoreResults(int current) throws SQLException {
        try {
            if (this.wrappedStmt != null) {
                return this.wrappedStmt.getMoreResults(current);
            }

            throw SQLError.createSQLException(Messages.getString("Statement.AlreadyClosed"), MysqlErrorNumbers.SQL_STATE_ILLEGAL_ARGUMENT,
                    this.exceptionInterceptor);
        } catch (SQLException sqlEx) {
            checkAndFireConnectionError(sqlEx);
        }

        return false;
    }

    @Override
    public void setQueryTimeout(int seconds) throws SQLException {
        try {
            if (this.wrappedStmt != null) {
                this.wrappedStmt.setQueryTimeout(seconds);
            } else {
                throw SQLError.createSQLException(Messages.getString("Statement.AlreadyClosed"), MysqlErrorNumbers.SQL_STATE_ILLEGAL_ARGUMENT,
                        this.exceptionInterceptor);
            }
        } catch (SQLException sqlEx) {
            checkAndFireConnectionError(sqlEx);
        }
    }

    @Override
    public int getQueryTimeout() throws SQLException {
        try {
            if (this.wrappedStmt != null) {
                return this.wrappedStmt.getQueryTimeout();
            }

            throw SQLError.createSQLException(Messages.getString("Statement.AlreadyClosed"), MysqlErrorNumbers.SQL_STATE_ILLEGAL_ARGUMENT,
                    this.exceptionInterceptor);
        } catch (SQLException sqlEx) {
            checkAndFireConnectionError(sqlEx);
        }

        return 0;
    }

    @Override
    public ResultSet getResultSet() throws SQLException {
        try {
            if (this.wrappedStmt != null) {
                ResultSet rs = this.wrappedStmt.getResultSet();

                if (rs != null) {
                    ((com.mysql.cj.jdbc.result.ResultSetInternalMethods) rs).setWrapperStatement(this);
                }
                return rs;
            }

            throw SQLError.createSQLException(Messages.getString("Statement.AlreadyClosed"), MysqlErrorNumbers.SQL_STATE_ILLEGAL_ARGUMENT,
                    this.exceptionInterceptor);
        } catch (SQLException sqlEx) {
            checkAndFireConnectionError(sqlEx);
        }

        return null;
    }

    @Override
    public int getResultSetConcurrency() throws SQLException {
        try {
            if (this.wrappedStmt != null) {
                return this.wrappedStmt.getResultSetConcurrency();
            }

            throw SQLError.createSQLException(Messages.getString("Statement.AlreadyClosed"), MysqlErrorNumbers.SQL_STATE_ILLEGAL_ARGUMENT,
                    this.exceptionInterceptor);
        } catch (SQLException sqlEx) {
            checkAndFireConnectionError(sqlEx);
        }

        return 0;
    }

    @Override
    public int getResultSetHoldability() throws SQLException {
        try {
            if (this.wrappedStmt != null) {
                return this.wrappedStmt.getResultSetHoldability();
            }

            throw SQLError.createSQLException(Messages.getString("Statement.AlreadyClosed"), MysqlErrorNumbers.SQL_STATE_ILLEGAL_ARGUMENT,
                    this.exceptionInterceptor);
        } catch (SQLException sqlEx) {
            checkAndFireConnectionError(sqlEx);
        }

        return Statement.CLOSE_CURRENT_RESULT;
    }

    @Override
    public int getResultSetType() throws SQLException {
        try {
            if (this.wrappedStmt != null) {
                return this.wrappedStmt.getResultSetType();
            }

            throw SQLError.createSQLException(Messages.getString("Statement.AlreadyClosed"), MysqlErrorNumbers.SQL_STATE_ILLEGAL_ARGUMENT,
                    this.exceptionInterceptor);
        } catch (SQLException sqlEx) {
            checkAndFireConnectionError(sqlEx);
        }

        return ResultSet.TYPE_FORWARD_ONLY;
    }

    @Override
    public int getUpdateCount() throws SQLException {
        try {
            if (this.wrappedStmt != null) {
                return this.wrappedStmt.getUpdateCount();
            }

            throw SQLError.createSQLException(Messages.getString("Statement.AlreadyClosed"), MysqlErrorNumbers.SQL_STATE_ILLEGAL_ARGUMENT,
                    this.exceptionInterceptor);
        } catch (SQLException sqlEx) {
            checkAndFireConnectionError(sqlEx);
        }

        return -1;
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        try {
            if (this.wrappedStmt != null) {
                return this.wrappedStmt.getWarnings();
            }

            throw SQLError.createSQLException(Messages.getString("Statement.AlreadyClosed"), MysqlErrorNumbers.SQL_STATE_ILLEGAL_ARGUMENT,
                    this.exceptionInterceptor);
        } catch (SQLException sqlEx) {
            checkAndFireConnectionError(sqlEx);
        }

        return null;
    }

    @Override
    public void addBatch(String sql) throws SQLException {
        try {
            if (this.wrappedStmt != null) {
                this.wrappedStmt.addBatch(sql);
            } else {
                throw SQLError.createSQLException(Messages.getString("Statement.AlreadyClosed"), MysqlErrorNumbers.SQL_STATE_ILLEGAL_ARGUMENT,
                        this.exceptionInterceptor);
            }
        } catch (SQLException sqlEx) {
            checkAndFireConnectionError(sqlEx);
        }
    }

    @Override
    public void cancel() throws SQLException {
        try {
            if (this.wrappedStmt != null) {
                this.wrappedStmt.cancel();
            } else {
                throw SQLError.createSQLException(Messages.getString("Statement.AlreadyClosed"), MysqlErrorNumbers.SQL_STATE_ILLEGAL_ARGUMENT,
                        this.exceptionInterceptor);
            }
        } catch (SQLException sqlEx) {
            checkAndFireConnectionError(sqlEx);
        }
    }

    @Override
    public void clearBatch() throws SQLException {
        try {
            if (this.wrappedStmt != null) {
                this.wrappedStmt.clearBatch();
            } else {
                throw SQLError.createSQLException(Messages.getString("Statement.AlreadyClosed"), MysqlErrorNumbers.SQL_STATE_ILLEGAL_ARGUMENT,
                        this.exceptionInterceptor);
            }
        } catch (SQLException sqlEx) {
            checkAndFireConnectionError(sqlEx);
        }
    }

    @Override
    public void clearWarnings() throws SQLException {
        try {
            if (this.wrappedStmt != null) {
                this.wrappedStmt.clearWarnings();
            } else {
                throw SQLError.createSQLException(Messages.getString("Statement.AlreadyClosed"), MysqlErrorNumbers.SQL_STATE_ILLEGAL_ARGUMENT,
                        this.exceptionInterceptor);
            }
        } catch (SQLException sqlEx) {
            checkAndFireConnectionError(sqlEx);
        }
    }

    @Override
    public void close() throws SQLException {
        try {
            if (this.wrappedStmt != null) {
                this.wrappedStmt.close();
            }
        } catch (SQLException sqlEx) {
            checkAndFireConnectionError(sqlEx);
        } finally {
            this.wrappedStmt = null;
            this.pooledConnection = null;
            this.unwrappedInterfaces = null;
        }
    }

    @Override
    public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
        try {
            if (this.wrappedStmt != null) {
                return this.wrappedStmt.execute(sql, autoGeneratedKeys);
            }

            throw SQLError.createSQLException(Messages.getString("Statement.AlreadyClosed"), MysqlErrorNumbers.SQL_STATE_ILLEGAL_ARGUMENT,
                    this.exceptionInterceptor);
        } catch (SQLException sqlEx) {
            checkAndFireConnectionError(sqlEx);
        }

        return false; // we actually never get here, but the compiler can't figure that out
    }

    @Override
    public boolean execute(String sql, int[] columnIndexes) throws SQLException {
        try {
            if (this.wrappedStmt != null) {
                return this.wrappedStmt.execute(sql, columnIndexes);
            }

            throw SQLError.createSQLException(Messages.getString("Statement.AlreadyClosed"), MysqlErrorNumbers.SQL_STATE_ILLEGAL_ARGUMENT,
                    this.exceptionInterceptor);
        } catch (SQLException sqlEx) {
            checkAndFireConnectionError(sqlEx);
        }

        return false; // we actually never get here, but the compiler can't figure that out
    }

    @Override
    public boolean execute(String sql, String[] columnNames) throws SQLException {
        try {
            if (this.wrappedStmt != null) {
                return this.wrappedStmt.execute(sql, columnNames);
            }

            throw SQLError.createSQLException(Messages.getString("Statement.AlreadyClosed"), MysqlErrorNumbers.SQL_STATE_ILLEGAL_ARGUMENT,
                    this.exceptionInterceptor);
        } catch (SQLException sqlEx) {
            checkAndFireConnectionError(sqlEx);
        }

        return false; // we actually never get here, but the compiler can't figure that out
    }

    @Override
    public boolean execute(String sql) throws SQLException {
        try {
            if (this.wrappedStmt != null) {
                return this.wrappedStmt.execute(sql);
            }

            throw SQLError.createSQLException(Messages.getString("Statement.AlreadyClosed"), MysqlErrorNumbers.SQL_STATE_ILLEGAL_ARGUMENT,
                    this.exceptionInterceptor);
        } catch (SQLException sqlEx) {
            checkAndFireConnectionError(sqlEx);
        }

        return false; // we actually never get here, but the compiler can't figure that out
    }

    @Override
    public int[] executeBatch() throws SQLException {
        try {
            if (this.wrappedStmt != null) {
                return this.wrappedStmt.executeBatch();
            }

            throw SQLError.createSQLException(Messages.getString("Statement.AlreadyClosed"), MysqlErrorNumbers.SQL_STATE_ILLEGAL_ARGUMENT,
                    this.exceptionInterceptor);
        } catch (SQLException sqlEx) {
            checkAndFireConnectionError(sqlEx);
        }

        return null; // we actually never get here, but the compiler can't figure that out
    }

    @Override
    public ResultSet executeQuery(String sql) throws SQLException {
        ResultSet rs = null;
        try {
            if (this.wrappedStmt == null) {
                throw SQLError.createSQLException(Messages.getString("Statement.AlreadyClosed"), MysqlErrorNumbers.SQL_STATE_ILLEGAL_ARGUMENT,
                        this.exceptionInterceptor);
            }

            rs = this.wrappedStmt.executeQuery(sql);
            ((com.mysql.cj.jdbc.result.ResultSetInternalMethods) rs).setWrapperStatement(this);

        } catch (SQLException sqlEx) {
            checkAndFireConnectionError(sqlEx);
        }

        return rs;
    }

    @Override
    public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
        try {
            if (this.wrappedStmt != null) {
                return this.wrappedStmt.executeUpdate(sql, autoGeneratedKeys);
            }

            throw SQLError.createSQLException(Messages.getString("Statement.AlreadyClosed"), MysqlErrorNumbers.SQL_STATE_ILLEGAL_ARGUMENT,
                    this.exceptionInterceptor);
        } catch (SQLException sqlEx) {
            checkAndFireConnectionError(sqlEx);
        }

        return -1; // we actually never get here, but the compiler can't figure that out
    }

    @Override
    public int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
        try {
            if (this.wrappedStmt != null) {
                return this.wrappedStmt.executeUpdate(sql, columnIndexes);
            }

            throw SQLError.createSQLException(Messages.getString("Statement.AlreadyClosed"), MysqlErrorNumbers.SQL_STATE_ILLEGAL_ARGUMENT,
                    this.exceptionInterceptor);
        } catch (SQLException sqlEx) {
            checkAndFireConnectionError(sqlEx);
        }

        return -1; // we actually never get here, but the compiler can't figure that out
    }

    @Override
    public int executeUpdate(String sql, String[] columnNames) throws SQLException {
        try {
            if (this.wrappedStmt != null) {
                return this.wrappedStmt.executeUpdate(sql, columnNames);
            }

            throw SQLError.createSQLException(Messages.getString("Statement.AlreadyClosed"), MysqlErrorNumbers.SQL_STATE_ILLEGAL_ARGUMENT,
                    this.exceptionInterceptor);
        } catch (SQLException sqlEx) {
            checkAndFireConnectionError(sqlEx);
        }

        return -1; // we actually never get here, but the compiler can't figure that out
    }

    @Override
    public int executeUpdate(String sql) throws SQLException {
        try {
            if (this.wrappedStmt != null) {
                return this.wrappedStmt.executeUpdate(sql);
            }

            throw SQLError.createSQLException(Messages.getString("Statement.AlreadyClosed"), MysqlErrorNumbers.SQL_STATE_ILLEGAL_ARGUMENT,
                    this.exceptionInterceptor);
        } catch (SQLException sqlEx) {
            checkAndFireConnectionError(sqlEx);
        }

        return -1; // we actually never get here, but the compiler can't figure that out
    }

    public void enableStreamingResults() throws SQLException {
        try {
            if (this.wrappedStmt != null) {
                ((com.mysql.cj.jdbc.JdbcStatement) this.wrappedStmt).enableStreamingResults();
            } else {
                throw SQLError.createSQLException(Messages.getString("Statement.AlreadyClosed"), MysqlErrorNumbers.SQL_STATE_GENERAL_ERROR,
                        this.exceptionInterceptor);
            }
        } catch (SQLException sqlEx) {
            checkAndFireConnectionError(sqlEx);
        }
    }

    @Override
    public synchronized <T> T unwrap(java.lang.Class<T> iface) throws java.sql.SQLException {
        try {
            if ("java.sql.Statement".equals(iface.getName()) || "java.sql.Wrapper.class".equals(iface.getName())) {
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
    public boolean isWrapperFor(Class<?> iface) throws SQLException {

        boolean isInstance = iface.isInstance(this);

        if (isInstance) {
            return true;
        }

        String interfaceClassName = iface.getName();

        return (interfaceClassName.equals("com.mysql.cj.jdbc.Statement") || interfaceClassName.equals("java.sql.Statement")
                || interfaceClassName.equals("java.sql.Wrapper")); // TODO check other interfaces
    }

    @Override
    public boolean isClosed() throws SQLException {
        try {
            if (this.wrappedStmt != null) {
                return this.wrappedStmt.isClosed();
            }
            throw SQLError.createSQLException(Messages.getString("Statement.AlreadyClosed"), MysqlErrorNumbers.SQL_STATE_ILLEGAL_ARGUMENT,
                    this.exceptionInterceptor);
        } catch (SQLException sqlEx) {
            checkAndFireConnectionError(sqlEx);
        }

        return false; // We never get here, compiler can't tell
    }

    @Override
    public void setPoolable(boolean poolable) throws SQLException {
        try {
            if (this.wrappedStmt != null) {
                this.wrappedStmt.setPoolable(poolable);
            } else {
                throw SQLError.createSQLException(Messages.getString("Statement.AlreadyClosed"), MysqlErrorNumbers.SQL_STATE_ILLEGAL_ARGUMENT,
                        this.exceptionInterceptor);
            }
        } catch (SQLException sqlEx) {
            checkAndFireConnectionError(sqlEx);
        }
    }

    @Override
    public boolean isPoolable() throws SQLException {
        try {
            if (this.wrappedStmt != null) {
                return this.wrappedStmt.isPoolable();
            }
            throw SQLError.createSQLException(Messages.getString("Statement.AlreadyClosed"), MysqlErrorNumbers.SQL_STATE_ILLEGAL_ARGUMENT,
                    this.exceptionInterceptor);
        } catch (SQLException sqlEx) {
            checkAndFireConnectionError(sqlEx);
        }

        return false; // We never get here, compiler can't tell
    }

    @Override
    public void closeOnCompletion() throws SQLException {
        if (this.wrappedStmt == null) {
            throw SQLError.createSQLException(Messages.getString("Statement.AlreadyClosed"), MysqlErrorNumbers.SQL_STATE_ILLEGAL_ARGUMENT,
                    this.exceptionInterceptor);
        }
    }

    @Override
    public boolean isCloseOnCompletion() throws SQLException {
        if (this.wrappedStmt == null) {
            throw SQLError.createSQLException(Messages.getString("Statement.AlreadyClosed"), MysqlErrorNumbers.SQL_STATE_ILLEGAL_ARGUMENT,
                    this.exceptionInterceptor);
        }
        return false;
    }

    @Override
    public long[] executeLargeBatch() throws SQLException {
        try {
            if (this.wrappedStmt != null) {
                return ((StatementImpl) this.wrappedStmt).executeLargeBatch();
            }

            throw SQLError.createSQLException(Messages.getString("Statement.AlreadyClosed"), MysqlErrorNumbers.SQL_STATE_ILLEGAL_ARGUMENT,
                    this.exceptionInterceptor);
        } catch (SQLException sqlEx) {
            checkAndFireConnectionError(sqlEx);
        }

        return null; // we actually never get here, but the compiler can't figure that out
    }

    @Override
    public long executeLargeUpdate(String sql) throws SQLException {
        try {
            if (this.wrappedStmt != null) {
                return ((StatementImpl) this.wrappedStmt).executeLargeUpdate(sql);
            }

            throw SQLError.createSQLException(Messages.getString("Statement.AlreadyClosed"), MysqlErrorNumbers.SQL_STATE_ILLEGAL_ARGUMENT,
                    this.exceptionInterceptor);
        } catch (SQLException sqlEx) {
            checkAndFireConnectionError(sqlEx);
        }

        return -1; // we actually never get here, but the compiler can't figure that out
    }

    @Override
    public long executeLargeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
        try {
            if (this.wrappedStmt != null) {
                return ((StatementImpl) this.wrappedStmt).executeLargeUpdate(sql, autoGeneratedKeys);
            }

            throw SQLError.createSQLException(Messages.getString("Statement.AlreadyClosed"), MysqlErrorNumbers.SQL_STATE_ILLEGAL_ARGUMENT,
                    this.exceptionInterceptor);
        } catch (SQLException sqlEx) {
            checkAndFireConnectionError(sqlEx);
        }

        return -1; // we actually never get here, but the compiler can't figure that out
    }

    @Override
    public long executeLargeUpdate(String sql, int[] columnIndexes) throws SQLException {
        try {
            if (this.wrappedStmt != null) {
                return ((StatementImpl) this.wrappedStmt).executeLargeUpdate(sql, columnIndexes);
            }

            throw SQLError.createSQLException(Messages.getString("Statement.AlreadyClosed"), MysqlErrorNumbers.SQL_STATE_ILLEGAL_ARGUMENT,
                    this.exceptionInterceptor);
        } catch (SQLException sqlEx) {
            checkAndFireConnectionError(sqlEx);
        }

        return -1; // we actually never get here, but the compiler can't figure that out
    }

    @Override
    public long executeLargeUpdate(String sql, String[] columnNames) throws SQLException {
        try {
            if (this.wrappedStmt != null) {
                return ((StatementImpl) this.wrappedStmt).executeLargeUpdate(sql, columnNames);
            }

            throw SQLError.createSQLException(Messages.getString("Statement.AlreadyClosed"), MysqlErrorNumbers.SQL_STATE_ILLEGAL_ARGUMENT,
                    this.exceptionInterceptor);
        } catch (SQLException sqlEx) {
            checkAndFireConnectionError(sqlEx);
        }

        return -1; // we actually never get here, but the compiler can't figure that out
    }

    @Override
    public long getLargeMaxRows() throws SQLException {
        try {
            if (this.wrappedStmt != null) {
                return ((StatementImpl) this.wrappedStmt).getLargeMaxRows();
            }

            throw SQLError.createSQLException(Messages.getString("Statement.AlreadyClosed"), MysqlErrorNumbers.SQL_STATE_ILLEGAL_ARGUMENT,
                    this.exceptionInterceptor);
        } catch (SQLException sqlEx) {
            checkAndFireConnectionError(sqlEx);
        }

        return 0; // we actually never get here, but the compiler can't figure that out
    }

    @Override
    public long getLargeUpdateCount() throws SQLException {
        try {
            if (this.wrappedStmt != null) {
                return ((StatementImpl) this.wrappedStmt).getLargeUpdateCount();
            }

            throw SQLError.createSQLException(Messages.getString("Statement.AlreadyClosed"), MysqlErrorNumbers.SQL_STATE_ILLEGAL_ARGUMENT,
                    this.exceptionInterceptor);
        } catch (SQLException sqlEx) {
            checkAndFireConnectionError(sqlEx);
        }

        return -1;
    }

    @Override
    public void setLargeMaxRows(long max) throws SQLException {
        try {
            if (this.wrappedStmt != null) {
                ((StatementImpl) this.wrappedStmt).setLargeMaxRows(max);
            } else {
                throw SQLError.createSQLException(Messages.getString("Statement.AlreadyClosed"), MysqlErrorNumbers.SQL_STATE_ILLEGAL_ARGUMENT,
                        this.exceptionInterceptor);
            }
        } catch (SQLException sqlEx) {
            checkAndFireConnectionError(sqlEx);
        }
    }
}
