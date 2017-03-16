/*
  Copyright (c) 2015, 2017, Oracle and/or its affiliates. All rights reserved.

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

import java.sql.Date;
import java.sql.SQLException;
import java.sql.SQLType;
import java.sql.Struct;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.format.DateTimeParseException;

import com.mysql.jdbc.Field;
import com.mysql.jdbc.RowData;
import com.mysql.jdbc.SQLError;

public class JDBC42ResultSet extends JDBC4ResultSet {

    public JDBC42ResultSet(long updateCount, long updateID, MySQLConnection conn, StatementImpl creatorStmt) {
        super(updateCount, updateID, conn, creatorStmt);
    }

    public JDBC42ResultSet(String catalog, Field[] fields, RowData tuples, MySQLConnection conn, StatementImpl creatorStmt) throws SQLException {
        super(catalog, fields, tuples, conn, creatorStmt);
    }

    /**
     * Support for java.time.LocalDate, java.time.LocalTime, java.time.LocalDateTime, java.time.OffsetTime and java.time.OffsetDateTime.
     * 
     * @param columnIndex
     * @param type
     * @return
     * @throws SQLException
     */
    public <T> T getObject(int columnIndex, Class<T> type) throws SQLException {
        if (type == null) {
            throw SQLError.createSQLException("Type parameter can not be null", SQLError.SQL_STATE_ILLEGAL_ARGUMENT, getExceptionInterceptor());
        }

        if (type.equals(LocalDate.class)) {
            final Date date = getDate(columnIndex);
            return date == null ? null : type.cast(date.toLocalDate());
        } else if (type.equals(LocalDateTime.class)) {
            final Timestamp timestamp = getTimestamp(columnIndex);
            return timestamp == null ? null : type.cast(timestamp.toLocalDateTime());
        } else if (type.equals(LocalTime.class)) {
            final Time time = getTime(columnIndex);
            return time == null ? null : type.cast(time.toLocalTime());
        } else if (type.equals(OffsetDateTime.class)) {
            try {
                final String string = getString(columnIndex);
                return string == null ? null : type.cast(OffsetDateTime.parse(string));
            } catch (DateTimeParseException e) {
                // Let it continue and try by object deserialization.
            }
        } else if (type.equals(OffsetTime.class)) {
            try {
                final String string = getString(columnIndex);
                return string == null? null : type.cast(OffsetTime.parse(string));
            } catch (DateTimeParseException e) {
                // Let it continue and try by object deserialization.
            }
        }

        return super.getObject(columnIndex, type);
    }

    /**
     * Support for java.sql.JDBCType/java.sql.SQLType. (Not updatable)
     * 
     * @param columnIndex
     * @param x
     * @param targetSqlType
     * @throws SQLException
     */
    public void updateObject(int columnIndex, Object x, SQLType targetSqlType) throws SQLException {
        throw new NotUpdatable();
    }

    /**
     * Support for java.sql.JDBCType/java.sql.SQLType.
     * 
     * @param columnIndex
     * @param x
     * @param targetSqlType
     * @param scaleOrLength
     * @throws SQLException
     */
    public void updateObject(int columnIndex, Object x, SQLType targetSqlType, int scaleOrLength) throws SQLException {
        throw new NotUpdatable();
    }

    /**
     * Support for java.sql.JDBCType/java.sql.SQLType.
     * 
     * @param columnLabel
     * @param x
     * @param targetSqlType
     * @throws SQLException
     */
    public void updateObject(String columnLabel, Object x, SQLType targetSqlType) throws SQLException {
        throw new NotUpdatable();
    }

    /**
     * Support for java.sql.JDBCType/java.sql.SQLType.
     * 
     * @param columnLabel
     * @param x
     * @param targetSqlType
     * @param scaleOrLength
     * @throws SQLException
     */
    public void updateObject(String columnLabel, Object x, SQLType targetSqlType, int scaleOrLength) throws SQLException {
        throw new NotUpdatable();
    }
}
