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

import java.sql.Date;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.JDBCType;
import java.sql.SQLException;
import java.sql.SQLType;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.OffsetTime;
import java.time.OffsetDateTime;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;

import com.mysql.jdbc.ExceptionInterceptor;
import com.mysql.jdbc.Messages;
import com.mysql.jdbc.SQLError;

public class JDBC42Helper {
    /**
     * JDBC 4.2 Helper methods.
     */
    static Object convertJavaTimeToJavaSql(Object x) {
        if (x instanceof LocalDate) {
            return Date.valueOf((LocalDate) x);
        } else if (x instanceof LocalDateTime) {
            return Timestamp.valueOf((LocalDateTime) x);
        } else if (x instanceof LocalTime) {
            return Time.valueOf((LocalTime) x);
        }
        return x;
    }

    static boolean isSqlTypeSupported(int sqlType) {
        return sqlType != Types.REF_CURSOR && sqlType != Types.TIME_WITH_TIMEZONE && sqlType != Types.TIMESTAMP_WITH_TIMEZONE;
    }

    static int checkSqlType(int sqlType, ExceptionInterceptor exceptionInterceptor) throws SQLException {
        if (JDBC42Helper.isSqlTypeSupported(sqlType)) {
            return sqlType;
        }
        throw SQLError.createSQLFeatureNotSupportedException(Messages.getString("UnsupportedSQLType.0") + JDBCType.valueOf(sqlType),
                SQLError.SQL_STATE_DRIVER_NOT_CAPABLE, exceptionInterceptor);
    }

    static int translateAndCheckSqlType(SQLType sqlType, ExceptionInterceptor exceptionInterceptor) throws SQLException {
        return JDBC42Helper.checkSqlType(sqlType.getVendorTypeNumber(), exceptionInterceptor);
    }
}
