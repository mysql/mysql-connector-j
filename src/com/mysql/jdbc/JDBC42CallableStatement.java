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

package com.mysql.jdbc;

import java.sql.SQLException;
import java.sql.SQLType;

public class JDBC42CallableStatement extends JDBC4CallableStatement {
    public JDBC42CallableStatement(MySQLConnection conn, CallableStatementParamInfo paramInfo) throws SQLException {
        super(conn, paramInfo);
    }

    public JDBC42CallableStatement(MySQLConnection conn, String sql, String catalog, boolean isFunctionCall) throws SQLException {
        super(conn, sql, catalog, isFunctionCall);
    }

    /**
     * Helper methods.
     */

    private int checkSqlType(int sqlType) throws SQLException {
        return JDBC42Helper.checkSqlType(sqlType, getExceptionInterceptor());
    }

    private int translateAndCheckSqlType(SQLType sqlType) throws SQLException {
        return JDBC42Helper.translateAndCheckSqlType(sqlType, getExceptionInterceptor());
    }

    /**
     * Support for java.sql.JDBCType/java.sql.SQLType.
     * 
     * @param parameterIndex
     * @param sqlType
     * @throws SQLException
     */
    public void registerOutParameter(int parameterIndex, SQLType sqlType) throws SQLException {
        super.registerOutParameter(parameterIndex, translateAndCheckSqlType(sqlType));
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
        super.registerOutParameter(parameterIndex, translateAndCheckSqlType(sqlType), scale);
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
        super.registerOutParameter(parameterIndex, translateAndCheckSqlType(sqlType), typeName);
    }

    /**
     * Support for java.sql.JDBCType/java.sql.SQLType.
     * 
     * @param parameterName
     * @param sqlType
     * @throws SQLException
     */
    public void registerOutParameter(String parameterName, SQLType sqlType) throws SQLException {
        super.registerOutParameter(parameterName, translateAndCheckSqlType(sqlType));
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
        super.registerOutParameter(parameterName, translateAndCheckSqlType(sqlType), scale);
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
        super.registerOutParameter(parameterName, translateAndCheckSqlType(sqlType), typeName);
    }

    /**
     * Support for java.time.LocalDate, java.time.LocalTime, java.time.LocalDateTime, java.time.OffsetTime and java.time.OffsetDateTime.
     * 
     * @param parameterIndex
     * @param x
     * @throws SQLException
     */
    @Override
    public void setObject(int parameterIndex, Object x) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            super.setObject(parameterIndex, JDBC42Helper.convertJavaTimeToJavaSql(x));
        }
    }

    /**
     * Support for java.time.LocalDate, java.time.LocalTime, java.time.LocalDateTime, java.time.OffsetTime and java.time.OffsetDateTime.
     * 
     * @param parameterIndex
     * @param x
     * @param targetSqlType
     * @throws SQLException
     */
    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            super.setObject(parameterIndex, JDBC42Helper.convertJavaTimeToJavaSql(x), checkSqlType(targetSqlType));
        }
    }

    /**
     * Support for java.time.LocalDate, java.time.LocalTime, java.time.LocalDateTime, java.time.OffsetTime and java.time.OffsetDateTime.
     * 
     * @param parameterIndex
     * @param x
     * @param targetSqlType
     * @param scaleOrLength
     * @throws SQLException
     */
    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType, int scaleOrLength) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            super.setObject(parameterIndex, JDBC42Helper.convertJavaTimeToJavaSql(x), checkSqlType(targetSqlType), scaleOrLength);
        }
    }

    /**
     * Support for java.sql.JDBCType/java.sql.SQLType.
     * Support for java.time.LocalDate, java.time.LocalTime, java.time.LocalDateTime, java.time.OffsetTime and java.time.OffsetDateTime.
     * 
     * @param parameterIndex
     * @param x
     * @param targetSqlType
     * @throws SQLException
     */
    public void setObject(int parameterIndex, Object x, SQLType targetSqlType) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            super.setObject(parameterIndex, JDBC42Helper.convertJavaTimeToJavaSql(x), translateAndCheckSqlType(targetSqlType));
        }
    }

    /**
     * Support for java.sql.JDBCType/java.sql.SQLType.
     * Support for java.time.LocalDate, java.time.LocalTime, java.time.LocalDateTime, java.time.OffsetTime and java.time.OffsetDateTime.
     * 
     * @param parameterIndex
     * @param x
     * @param targetSqlType
     * @param scaleOrLength
     * @throws SQLException
     */
    public void setObject(int parameterIndex, Object x, SQLType targetSqlType, int scaleOrLength) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            super.setObject(parameterIndex, JDBC42Helper.convertJavaTimeToJavaSql(x), translateAndCheckSqlType(targetSqlType), scaleOrLength);
        }
    }

    /**
     * Support for java.sql.JDBCType/java.sql.SQLType.
     * Support for java.time.LocalDate, java.time.LocalTime, java.time.LocalDateTime, java.time.OffsetTime and java.time.OffsetDateTime.
     * 
     * @param parameterName
     * @param x
     * @param targetSqlType
     * @throws SQLException
     */
    public void setObject(String parameterName, Object x, SQLType targetSqlType) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            super.setObject(parameterName, JDBC42Helper.convertJavaTimeToJavaSql(x), translateAndCheckSqlType(targetSqlType));
        }
    }

    /**
     * Support for java.sql.JDBCType/java.sql.SQLType.
     * Support for java.time.LocalDate, java.time.LocalTime, java.time.LocalDateTime, java.time.OffsetTime and java.time.OffsetDateTime.
     * 
     * @param parameterName
     * @param x
     * @param targetSqlType
     * @param scaleOrLength
     * @throws SQLException
     */
    public void setObject(String parameterName, Object x, SQLType targetSqlType, int scaleOrLength) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            super.setObject(parameterName, JDBC42Helper.convertJavaTimeToJavaSql(x), translateAndCheckSqlType(targetSqlType), scaleOrLength);
        }
    }
}
