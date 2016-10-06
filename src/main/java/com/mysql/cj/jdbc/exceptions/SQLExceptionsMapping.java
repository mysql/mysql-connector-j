/*
  Copyright (c) 2015, 2016, Oracle and/or its affiliates. All rights reserved.

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

package com.mysql.cj.jdbc.exceptions;

import java.sql.SQLException;

import com.mysql.cj.api.exceptions.ExceptionInterceptor;
import com.mysql.cj.core.exceptions.CJCommunicationsException;
import com.mysql.cj.core.exceptions.CJConnectionFeatureNotAvailableException;
import com.mysql.cj.core.exceptions.CJException;
import com.mysql.cj.core.exceptions.CJOperationNotSupportedException;
import com.mysql.cj.core.exceptions.CJPacketTooBigException;
import com.mysql.cj.core.exceptions.CJTimeoutException;
import com.mysql.cj.core.exceptions.ConnectionIsClosedException;
import com.mysql.cj.core.exceptions.DataConversionException;
import com.mysql.cj.core.exceptions.DataReadException;
import com.mysql.cj.core.exceptions.DataTruncationException;
import com.mysql.cj.core.exceptions.InvalidConnectionAttributeException;
import com.mysql.cj.core.exceptions.NumberOutOfRange;
import com.mysql.cj.core.exceptions.OperationCancelledException;
import com.mysql.cj.core.exceptions.SSLParamsException;
import com.mysql.cj.core.exceptions.StatementIsClosedException;
import com.mysql.cj.core.exceptions.UnableToConnectException;
import com.mysql.cj.core.exceptions.WrongArgumentException;

public class SQLExceptionsMapping {

    public static SQLException translateException(Throwable ex, ExceptionInterceptor interceptor) {
        if (ex instanceof SQLException) {
            return (SQLException) ex;

        } else if (ex.getCause() != null && ex.getCause() instanceof SQLException) {
            return (SQLException) ex.getCause();

        } else if (ex instanceof CJCommunicationsException) {
            return SQLError.createCommunicationsException(ex.getMessage(), ex, interceptor);

        } else if (ex instanceof CJConnectionFeatureNotAvailableException) {
            return new ConnectionFeatureNotAvailableException(ex.getMessage(), ex);

        } else if (ex instanceof SSLParamsException) {
            return SQLError.createSQLException(ex.getMessage(), SQLError.SQL_STATE_BAD_SSL_PARAMS, 0, false, ex, interceptor);

        } else if (ex instanceof ConnectionIsClosedException) {
            return SQLError.createSQLException(ex.getMessage(), SQLError.SQL_STATE_CONNECTION_NOT_OPEN, ex, interceptor);

        } else if (ex instanceof InvalidConnectionAttributeException) {
            return SQLError.createSQLException(ex.getMessage(), SQLError.SQL_STATE_INVALID_CONNECTION_ATTRIBUTE, ex, interceptor);

        } else if (ex instanceof UnableToConnectException) {
            return SQLError.createSQLException(ex.getMessage(), SQLError.SQL_STATE_UNABLE_TO_CONNECT_TO_DATASOURCE, ex, interceptor);

        } else if (ex instanceof StatementIsClosedException) {
            return SQLError.createSQLException(ex.getMessage(), SQLError.SQL_STATE_ILLEGAL_ARGUMENT, ex, interceptor);

        } else if (ex instanceof WrongArgumentException) {
            return SQLError.createSQLException(ex.getMessage(), SQLError.SQL_STATE_ILLEGAL_ARGUMENT, ex, interceptor);

        } else if (ex instanceof StringIndexOutOfBoundsException) {
            return SQLError.createSQLException(ex.getMessage(), SQLError.SQL_STATE_ILLEGAL_ARGUMENT, ex, interceptor);

        } else if (ex instanceof NumberOutOfRange) {
            // must come before DataReadException as it's more specific
            return SQLError.createSQLException(ex.getMessage(), SQLError.SQL_STATE_NUMERIC_VALUE_OUT_OF_RANGE, ex, interceptor);

        } else if (ex instanceof DataConversionException) {
            // must come before DataReadException as it's more specific
            return SQLError.createSQLException(ex.getMessage(), SQLError.SQL_STATE_INVALID_CHARACTER_VALUE_FOR_CAST, ex, interceptor);

        } else if (ex instanceof DataReadException) {
            return SQLError.createSQLException(ex.getMessage(), SQLError.SQL_STATE_ILLEGAL_ARGUMENT, ex, interceptor);

        } else if (ex instanceof DataTruncationException) {
            return new MysqlDataTruncation(((DataTruncationException) ex).getMessage(), ((DataTruncationException) ex).getIndex(),
                    ((DataTruncationException) ex).isParameter(), ((DataTruncationException) ex).isRead(), ((DataTruncationException) ex).getDataSize(),
                    ((DataTruncationException) ex).getTransferSize(), ((DataTruncationException) ex).getVendorCode());

        } else if (ex instanceof CJPacketTooBigException) {
            return new PacketTooBigException(ex.getMessage());

        } else if (ex instanceof OperationCancelledException) {
            return new MySQLStatementCancelledException(ex.getMessage());

        } else if (ex instanceof CJTimeoutException) {
            return new MySQLTimeoutException(ex.getMessage());

        } else if (ex instanceof CJOperationNotSupportedException) {
            return new OperationNotSupportedException(ex.getMessage());

        } else if (ex instanceof UnsupportedOperationException) {
            return new OperationNotSupportedException(ex.getMessage());

        } else if (ex instanceof CJException) {
            return SQLError.createSQLException(ex.getMessage(), ((CJException) ex).getSQLState(), ((CJException) ex).getVendorCode(),
                    ((CJException) ex).isTransient(), interceptor);

        } else {
            return SQLError.createSQLException(ex.getMessage(), SQLError.SQL_STATE_GENERAL_ERROR, ex, interceptor);
        }
    }

    public static SQLException translateException(Throwable ex) {
        return translateException(ex, null);
    }
}
