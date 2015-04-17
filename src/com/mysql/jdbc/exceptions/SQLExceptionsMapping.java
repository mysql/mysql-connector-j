/*
  Copyright (c) 2015, Oracle and/or its affiliates. All rights reserved.

  The MySQL Connector/J is licensed under the terms of the GPLv2
  <http://www.gnu.org/licenses/old-licenses/gpl-2.0.html>, like most MySQL Connectors.
  There are special exceptions to the terms and conditions of the GPLv2 as it is applied to
  this software, see the FLOSS License Exception
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

package com.mysql.jdbc.exceptions;

import java.sql.SQLException;

import com.mysql.cj.api.exception.ExceptionInterceptor;
import com.mysql.cj.core.exception.ConnectionClosedException;
import com.mysql.cj.core.exception.InvalidConnectionAttributeException;
import com.mysql.cj.core.exception.StatementClosedException;
import com.mysql.cj.core.exception.UnableToConnectException;
import com.mysql.cj.core.exception.WrongArgumentException;

public class SQLExceptionsMapping {

    public static SQLException translateException(Throwable ex, ExceptionInterceptor interceptor) {
        if (ex instanceof SQLException) {
            return (SQLException) ex;

        } else if (ex.getCause() != null && ex.getCause() instanceof SQLException) {
            return (SQLException) ex.getCause();

        } else if (ex instanceof ConnectionClosedException) {
            return SQLError.createSQLException(ex.getMessage(), SQLError.SQL_STATE_CONNECTION_NOT_OPEN, ex, interceptor);

        } else if (ex instanceof InvalidConnectionAttributeException) {
            return SQLError.createSQLException(ex.getMessage(), SQLError.SQL_STATE_INVALID_CONNECTION_ATTRIBUTE, ex, interceptor);

        } else if (ex instanceof UnableToConnectException) {
            return SQLError.createSQLException(ex.getMessage(), SQLError.SQL_STATE_UNABLE_TO_CONNECT_TO_DATASOURCE, ex, interceptor);

        } else if (ex instanceof StatementClosedException) {
            return SQLError.createSQLException(ex.getMessage(), SQLError.SQL_STATE_ILLEGAL_ARGUMENT, ex, interceptor);

        } else if (ex instanceof WrongArgumentException) {
            return SQLError.createSQLException(ex.getMessage(), SQLError.SQL_STATE_ILLEGAL_ARGUMENT, ex, interceptor);

        } else if (ex instanceof StringIndexOutOfBoundsException) {
            return SQLError.createSQLException(ex.getMessage(), SQLError.SQL_STATE_ILLEGAL_ARGUMENT, ex, interceptor);

        } else {
            return SQLError.createSQLException(ex.getMessage(), SQLError.SQL_STATE_GENERAL_ERROR, ex, interceptor);
        }
    }

    public static SQLException translateException(Throwable ex) {
        return translateException(ex, null);
    }
}
