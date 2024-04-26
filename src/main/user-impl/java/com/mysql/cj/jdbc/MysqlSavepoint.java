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

import java.sql.SQLException;
import java.sql.Savepoint;

import com.mysql.cj.Messages;
import com.mysql.cj.exceptions.ExceptionInterceptor;
import com.mysql.cj.exceptions.MysqlErrorNumbers;
import com.mysql.cj.jdbc.exceptions.SQLError;
import com.mysql.cj.util.StringUtils;

/**
 * Represents SQL SAVEPOINTS in MySQL.
 */
public class MysqlSavepoint implements Savepoint {

    private String savepointName;

    private ExceptionInterceptor exceptionInterceptor;

    /**
     * Creates an unnamed savepoint.
     *
     * @param exceptionInterceptor
     *            exception interceptor
     *
     * @throws SQLException
     *             if an error occurs
     */
    MysqlSavepoint(ExceptionInterceptor exceptionInterceptor) throws SQLException {
        this(StringUtils.getUniqueSavepointId(), exceptionInterceptor);
    }

    /**
     * Creates a named savepoint
     *
     * @param name
     *            the name of the savepoint.
     * @param exceptionInterceptor
     *            exception interceptor
     *
     * @throws SQLException
     *             if name == null or is empty.
     */
    MysqlSavepoint(String name, ExceptionInterceptor exceptionInterceptor) throws SQLException {
        if (name == null || name.length() == 0) {
            throw SQLError.createSQLException(Messages.getString("MysqlSavepoint.0"), MysqlErrorNumbers.SQLSTATE_CONNJ_ILLEGAL_ARGUMENT, exceptionInterceptor);
        }

        this.savepointName = name;

        this.exceptionInterceptor = exceptionInterceptor;
    }

    @Override
    public int getSavepointId() throws SQLException {
        throw SQLError.createSQLException(Messages.getString("MysqlSavepoint.1"), MysqlErrorNumbers.SQLSTATE_CONNJ_DRIVER_NOT_CAPABLE,
                this.exceptionInterceptor);
    }

    @Override
    public String getSavepointName() throws SQLException {
        return this.savepointName;
    }

}
