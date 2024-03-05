/*
 * Copyright (c) 2007, 2024, Oracle and/or its affiliates.
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
import java.sql.SQLException;

import com.mysql.cj.PingTarget;
import com.mysql.cj.Query;
import com.mysql.cj.exceptions.ExceptionInterceptor;
import com.mysql.cj.jdbc.result.ResultSetInternalMethods;

/**
 * This interface contains methods that are considered the "vendor extension" to the JDBC API for MySQL's implementation of java.sql.Statement.
 *
 * For those looking further into the driver implementation, it is not an API that is used for plugability of implementations inside our driver
 * (which is why there are still references to StatementImpl throughout the code).
 */
public interface JdbcStatement extends java.sql.Statement, Query {

    public static final int MAX_ROWS = 50000000; // From the MySQL FAQ

    /**
     * Workaround for containers that 'check' for sane values of
     * Statement.setFetchSize() so that applications can use
     * the Java variant of libmysql's mysql_use_result() behavior.
     *
     * @throws SQLException
     *             if an error occurs
     */
    void enableStreamingResults() throws SQLException;

    /**
     * Resets this statements fetch size and result set type to the values
     * they had before enableStreamingResults() was called.
     *
     * @throws SQLException
     *             if an error occurs
     */
    void disableStreamingResults() throws SQLException;

    /**
     * Sets an InputStream instance that will be used to send data
     * to the MySQL server for a "LOAD DATA LOCAL INFILE" statement
     * rather than a FileInputStream or URLInputStream that represents
     * the path given as an argument to the statement.
     *
     * This stream will be read to completion upon execution of a
     * "LOAD DATA LOCAL INFILE" statement, and will automatically
     * be closed by the driver, so it needs to be reset
     * before each call to execute*() that would cause the MySQL
     * server to request data to fulfill the request for
     * "LOAD DATA LOCAL INFILE".
     *
     * If this value is set to NULL, the driver will revert to using
     * a FileInputStream or URLInputStream as required.
     *
     * @param stream
     *            input stream
     */
    void setLocalInfileInputStream(InputStream stream);

    /**
     * Returns the InputStream instance that will be used to send
     * data in response to a "LOAD DATA LOCAL INFILE" statement.
     *
     * This method returns NULL if no such stream has been set
     * via setLocalInfileInputStream().
     *
     * @return
     *         input stream
     */
    InputStream getLocalInfileInputStream();

    void setPingTarget(PingTarget pingTarget);

    ExceptionInterceptor getExceptionInterceptor();

    /**
     * Callback for result set instances to remove them from the Set that
     * tracks them per-statement
     *
     * @param rs
     *            result set
     */

    void removeOpenResultSet(ResultSetInternalMethods rs);

    /**
     * Returns the number of open result sets for this statement.
     *
     * @return the number of open result sets for this statement
     */
    int getOpenResultSetCount();

    void setHoldResultsOpenOverClose(boolean holdResultsOpenOverClose);

    Query getQuery();

    void setAttribute(String name, Object value);

    void clearAttributes();

    ResultSetInternalMethods getResultSetInternal();

}
