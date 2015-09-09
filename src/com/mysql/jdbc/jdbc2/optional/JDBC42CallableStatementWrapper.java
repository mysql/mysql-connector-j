/*
 Copyright (c) 2015, Oracle and/or its affiliates. All rights reserved.

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

package com.mysql.jdbc.jdbc2.optional;

import java.sql.CallableStatement;
import java.sql.SQLException;
import java.sql.SQLType;

import com.mysql.jdbc.SQLError;
import com.mysql.jdbc.jdbc2.optional.ConnectionWrapper;
import com.mysql.jdbc.jdbc2.optional.MysqlPooledConnection;

public class JDBC42CallableStatementWrapper extends JDBC4CallableStatementWrapper {
    public JDBC42CallableStatementWrapper(ConnectionWrapper c, MysqlPooledConnection conn, CallableStatement toWrap) {
        super(c, conn, toWrap);
    }

    /**
     * Support for java.sql.JDBCType/java.sql.SQLType.
     * 
     * @param parameterIndex
     * @param sqlType
     * @throws SQLException
     */
    public void registerOutParameter(int parameterIndex, SQLType sqlType) throws SQLException {
        try {
            if (this.wrappedStmt != null) {
                ((CallableStatement) this.wrappedStmt).registerOutParameter(parameterIndex, sqlType);
            } else {
                throw SQLError.createSQLException("No operations allowed after statement closed", SQLError.SQL_STATE_GENERAL_ERROR, this.exceptionInterceptor);
            }
        } catch (SQLException sqlEx) {
            checkAndFireConnectionError(sqlEx);
        }
    }

    /**
     * Support for java.sql.JDBCType/java.sql.SQLType.
     * 
     * @param parameterIndex
     * @param sqlType
     * @param scale
     * @throws SQLException
     */
    public void registerOutParameter(int parameterIndex, SQLType sqlType, int scale) throws SQLException {
        try {
            if (this.wrappedStmt != null) {
                ((CallableStatement) this.wrappedStmt).registerOutParameter(parameterIndex, sqlType, scale);
            } else {
                throw SQLError.createSQLException("No operations allowed after statement closed", SQLError.SQL_STATE_GENERAL_ERROR, this.exceptionInterceptor);
            }
        } catch (SQLException sqlEx) {
            checkAndFireConnectionError(sqlEx);
        }
    }

    /**
     * Support for java.sql.JDBCType/java.sql.SQLType.
     * 
     * @param parameterIndex
     * @param sqlType
     * @param typeName
     * @throws SQLException
     */
    public void registerOutParameter(int parameterIndex, SQLType sqlType, String typeName) throws SQLException {
        try {
            if (this.wrappedStmt != null) {
                ((CallableStatement) this.wrappedStmt).registerOutParameter(parameterIndex, sqlType, typeName);
            } else {
                throw SQLError.createSQLException("No operations allowed after statement closed", SQLError.SQL_STATE_GENERAL_ERROR, this.exceptionInterceptor);
            }
        } catch (SQLException sqlEx) {
            checkAndFireConnectionError(sqlEx);
        }
    }

    /**
     * Support for java.sql.JDBCType/java.sql.SQLType.
     * 
     * @param parameterName
     * @param sqlType
     * @throws SQLException
     */
    public void registerOutParameter(String parameterName, SQLType sqlType) throws SQLException {
        try {
            if (this.wrappedStmt != null) {
                ((CallableStatement) this.wrappedStmt).registerOutParameter(parameterName, sqlType);
            } else {
                throw SQLError.createSQLException("No operations allowed after statement closed", SQLError.SQL_STATE_GENERAL_ERROR, this.exceptionInterceptor);
            }
        } catch (SQLException sqlEx) {
            checkAndFireConnectionError(sqlEx);
        }
    }

    /**
     * Support for java.sql.JDBCType/java.sql.SQLType.
     * 
     * @param parameterName
     * @param sqlType
     * @param scale
     * @throws SQLException
     */
    public void registerOutParameter(String parameterName, SQLType sqlType, int scale) throws SQLException {
        try {
            if (this.wrappedStmt != null) {
                ((CallableStatement) this.wrappedStmt).registerOutParameter(parameterName, sqlType, scale);
            } else {
                throw SQLError.createSQLException("No operations allowed after statement closed", SQLError.SQL_STATE_GENERAL_ERROR, this.exceptionInterceptor);
            }
        } catch (SQLException sqlEx) {
            checkAndFireConnectionError(sqlEx);
        }
    }

    /**
     * Support for java.sql.JDBCType/java.sql.SQLType.
     * 
     * @param parameterName
     * @param sqlType
     * @param typeName
     * @throws SQLException
     */
    public void registerOutParameter(String parameterName, SQLType sqlType, String typeName) throws SQLException {
        try {
            if (this.wrappedStmt != null) {
                ((CallableStatement) this.wrappedStmt).registerOutParameter(parameterName, sqlType, typeName);
            } else {
                throw SQLError.createSQLException("No operations allowed after statement closed", SQLError.SQL_STATE_GENERAL_ERROR, this.exceptionInterceptor);
            }
        } catch (SQLException sqlEx) {
            checkAndFireConnectionError(sqlEx);
        }
    }

    /**
     * Support for java.sql.JDBCType/java.sql.SQLType.
     * 
     * @param parameterIndex
     * @param x
     * @param targetSqlType
     * @throws SQLException
     */
    public void setObject(int parameterIndex, Object x, SQLType targetSqlType) throws SQLException {
        try {
            if (this.wrappedStmt != null) {
                ((CallableStatement) this.wrappedStmt).setObject(parameterIndex, x, targetSqlType);
            } else {
                throw SQLError.createSQLException("No operations allowed after statement closed", SQLError.SQL_STATE_GENERAL_ERROR, this.exceptionInterceptor);
            }
        } catch (SQLException sqlEx) {
            checkAndFireConnectionError(sqlEx);
        }
    }

    /**
     * Support for java.sql.JDBCType/java.sql.SQLType.
     * 
     * @param parameterIndex
     * @param x
     * @param targetSqlType
     * @param scaleOrLength
     * @throws SQLException
     */
    public void setObject(int parameterIndex, Object x, SQLType targetSqlType, int scaleOrLength) throws SQLException {
        try {
            if (this.wrappedStmt != null) {
                ((CallableStatement) this.wrappedStmt).setObject(parameterIndex, x, targetSqlType, scaleOrLength);
            } else {
                throw SQLError.createSQLException("No operations allowed after statement closed", SQLError.SQL_STATE_GENERAL_ERROR, this.exceptionInterceptor);
            }
        } catch (SQLException sqlEx) {
            checkAndFireConnectionError(sqlEx);
        }
    }

    /**
     * Support for java.sql.JDBCType/java.sql.SQLType.
     * 
     * @param parameterName
     * @param x
     * @param targetSqlType
     * @throws SQLException
     */
    public void setObject(String parameterName, Object x, SQLType targetSqlType) throws SQLException {
        try {
            if (this.wrappedStmt != null) {
                ((CallableStatement) this.wrappedStmt).setObject(parameterName, x, targetSqlType);
            } else {
                throw SQLError.createSQLException("No operations allowed after statement closed", SQLError.SQL_STATE_GENERAL_ERROR, this.exceptionInterceptor);
            }
        } catch (SQLException sqlEx) {
            checkAndFireConnectionError(sqlEx);
        }
    }

    /**
     * Support for java.sql.JDBCType/java.sql.SQLType.
     * 
     * @param parameterName
     * @param x
     * @param targetSqlType
     * @param scaleOrLength
     * @throws SQLException
     */
    public void setObject(String parameterName, Object x, SQLType targetSqlType, int scaleOrLength) throws SQLException {
        try {
            if (this.wrappedStmt != null) {
                ((CallableStatement) this.wrappedStmt).setObject(parameterName, x, targetSqlType, scaleOrLength);
            } else {
                throw SQLError.createSQLException("No operations allowed after statement closed", SQLError.SQL_STATE_GENERAL_ERROR, this.exceptionInterceptor);
            }
        } catch (SQLException sqlEx) {
            checkAndFireConnectionError(sqlEx);
        }
    }
}
