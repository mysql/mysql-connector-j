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

package com.mysql.cj.jdbc.exceptions;

import java.sql.SQLDataException;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLIntegrityConstraintViolationException;
import java.sql.SQLNonTransientConnectionException;
import java.sql.SQLSyntaxErrorException;
import java.sql.SQLTransientConnectionException;

import com.mysql.cj.exceptions.ExceptionInterceptor;
import com.mysql.cj.exceptions.MysqlErrorNumbers;
import com.mysql.cj.jdbc.JdbcConnection;
import com.mysql.cj.protocol.PacketReceivedTimeHolder;
import com.mysql.cj.protocol.PacketSentTimeHolder;
import com.mysql.cj.util.Util;

/**
 * SQLError is a utility class that maps MySQL error codes to SQL error codes as is required by the JDBC spec.
 */
public class SQLError {
    /*
     * SQL State Class SQLNonTransientException Subclass 08
     * SQLNonTransientConnectionException 22 SQLDataException 23
     * SQLIntegrityConstraintViolationException N/A
     * SQLInvalidAuthorizationException 42 SQLSyntaxErrorException
     * 
     * SQL State Class SQLTransientException Subclass 08
     * SQLTransientConnectionException 40 SQLTransactionRollbackException N/A
     * SQLTimeoutException
     */

    public static SQLException createSQLException(String message, String sqlState, ExceptionInterceptor interceptor) {
        return createSQLException(message, sqlState, 0, interceptor);
    }

    public static SQLException createSQLException(String message, ExceptionInterceptor interceptor) {
        SQLException sqlEx = new SQLException(message);

        return runThroughExceptionInterceptor(interceptor, sqlEx);
    }

    public static SQLException createSQLException(String message, String sqlState, Throwable cause, ExceptionInterceptor interceptor) {
        SQLException sqlEx = createSQLException(message, sqlState, null);

        if (sqlEx.getCause() == null) {
            if (cause != null) {
                try {
                    sqlEx.initCause(cause);
                } catch (Throwable t) {
                    // we're not going to muck with that here, since it's an error condition anyway!
                }
            }
        }
        // Run through the exception interceptor after setting the init cause.
        return runThroughExceptionInterceptor(interceptor, sqlEx);
    }

    public static SQLException createSQLException(String message, String sqlState, int vendorErrorCode, ExceptionInterceptor interceptor) {
        return createSQLException(message, sqlState, vendorErrorCode, false, interceptor);
    }

    public static SQLException createSQLException(String message, String sqlState, int vendorErrorCode, Throwable cause, ExceptionInterceptor interceptor) {
        return createSQLException(message, sqlState, vendorErrorCode, false, cause, interceptor);
    }

    public static SQLException createSQLException(String message, String sqlState, int vendorErrorCode, boolean isTransient, ExceptionInterceptor interceptor) {
        return createSQLException(message, sqlState, vendorErrorCode, isTransient, null, interceptor);
    }

    public static SQLException createSQLException(String message, String sqlState, int vendorErrorCode, boolean isTransient, Throwable cause,
            ExceptionInterceptor interceptor) {
        try {
            SQLException sqlEx = null;

            if (sqlState != null) {
                if (sqlState.startsWith("08")) {
                    if (isTransient) {
                        sqlEx = new SQLTransientConnectionException(message, sqlState, vendorErrorCode);
                    } else {
                        sqlEx = new SQLNonTransientConnectionException(message, sqlState, vendorErrorCode);
                    }

                } else if (sqlState.startsWith("22")) {
                    sqlEx = new SQLDataException(message, sqlState, vendorErrorCode);

                } else if (sqlState.startsWith("23")) {
                    sqlEx = new SQLIntegrityConstraintViolationException(message, sqlState, vendorErrorCode);

                } else if (sqlState.startsWith("42")) {
                    sqlEx = new SQLSyntaxErrorException(message, sqlState, vendorErrorCode);

                } else if (sqlState.startsWith("40")) {
                    sqlEx = new MySQLTransactionRollbackException(message, sqlState, vendorErrorCode);

                } else if (sqlState.startsWith("70100")) {
                    sqlEx = new MySQLQueryInterruptedException(message, sqlState, vendorErrorCode);

                } else {
                    sqlEx = new SQLException(message, sqlState, vendorErrorCode);
                }
            } else {
                sqlEx = new SQLException(message, sqlState, vendorErrorCode);
            }

            if (cause != null) {
                try {
                    sqlEx.initCause(cause);
                } catch (Throwable t) {
                    // we're not going to muck with that here, since it's an error condition anyway!
                }
            }

            return runThroughExceptionInterceptor(interceptor, sqlEx);

        } catch (Exception sqlEx) {
            SQLException unexpectedEx = new SQLException(
                    "Unable to create correct SQLException class instance, error class/codes may be incorrect. Reason: " + Util.stackTraceToString(sqlEx),
                    MysqlErrorNumbers.SQL_STATE_GENERAL_ERROR);

            return runThroughExceptionInterceptor(interceptor, unexpectedEx);

        }
    }

    public static SQLException createCommunicationsException(JdbcConnection conn, PacketSentTimeHolder packetSentTimeHolder,
            PacketReceivedTimeHolder packetReceivedTimeHolder, Exception underlyingException, ExceptionInterceptor interceptor) {

        SQLException exToReturn = new CommunicationsException(conn, packetSentTimeHolder, packetReceivedTimeHolder, underlyingException);

        if (underlyingException != null) {
            try {
                exToReturn.initCause(underlyingException);
            } catch (Throwable t) {
                // we're not going to muck with that here, since it's an error condition anyway!
            }
        }

        return runThroughExceptionInterceptor(interceptor, exToReturn);
    }

    public static SQLException createCommunicationsException(String message, Throwable underlyingException, ExceptionInterceptor interceptor) {
        SQLException exToReturn = null;

        exToReturn = new CommunicationsException(message, underlyingException);

        if (underlyingException != null) {
            try {
                exToReturn.initCause(underlyingException);
            } catch (Throwable t) {
                // we're not going to muck with that here, since it's an error condition anyway!
            }
        }

        return runThroughExceptionInterceptor(interceptor, exToReturn);
    }

    /**
     * Run exception through an ExceptionInterceptor chain.
     * 
     * @param exInterceptor
     *            exception interceptor
     * @param sqlEx
     *            cause
     * @return SQLException
     */
    private static SQLException runThroughExceptionInterceptor(ExceptionInterceptor exInterceptor, SQLException sqlEx) {
        if (exInterceptor != null) {
            SQLException interceptedEx = (SQLException) exInterceptor.interceptException(sqlEx);

            if (interceptedEx != null) {
                return interceptedEx;
            }
        }
        return sqlEx;
    }

    /**
     * Create a BatchUpdateException taking in consideration the JDBC version in use. For JDBC version prior to 4.2 the updates count array has int elements
     * while JDBC 4.2 and beyond uses long values.
     * 
     * @param underlyingEx
     *            underlying exception
     * @param updateCounts
     *            update counts of completed queries in this batch
     * @param interceptor
     *            exception interceptor
     * @return SQLException
     * @throws SQLException
     *             if an error occurs
     */
    public static SQLException createBatchUpdateException(SQLException underlyingEx, long[] updateCounts, ExceptionInterceptor interceptor)
            throws SQLException {
        // TODO should not throw SQLException
        SQLException newEx = (SQLException) Util.getInstance("java.sql.BatchUpdateException",
                new Class<?>[] { String.class, String.class, int.class, long[].class, Throwable.class },
                new Object[] { underlyingEx.getMessage(), underlyingEx.getSQLState(), underlyingEx.getErrorCode(), updateCounts, underlyingEx }, interceptor);
        return runThroughExceptionInterceptor(interceptor, newEx);
    }

    /**
     * Create a SQLFeatureNotSupportedException or a NotImplemented exception according to the JDBC version in use.
     * 
     * @return SQLException
     */
    public static SQLException createSQLFeatureNotSupportedException() {
        return new SQLFeatureNotSupportedException();
    }

    /**
     * Create a SQLFeatureNotSupportedException or a NotImplemented exception according to the JDBC version in use.
     * 
     * @param message
     *            error message
     * @param sqlState
     *            sqlState
     * @param interceptor
     *            exception interceptor
     * @return SQLException
     * @throws SQLException
     *             if an error occurs
     */
    public static SQLException createSQLFeatureNotSupportedException(String message, String sqlState, ExceptionInterceptor interceptor) throws SQLException {
        SQLException newEx = new SQLFeatureNotSupportedException(message, sqlState);
        return runThroughExceptionInterceptor(interceptor, newEx);
    }
}
