/*
 * Copyright (c) 2018, Oracle and/or its affiliates. All rights reserved.
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

import java.math.BigInteger;
import java.sql.SQLException;

import com.mysql.cj.MysqlType;
import com.mysql.cj.ParseInfo;
import com.mysql.cj.QueryBindings;

public interface JdbcPreparedStatement extends java.sql.PreparedStatement, JdbcStatement {

    void realClose(boolean calledExplicitly, boolean closeOpenResults) throws SQLException;

    QueryBindings<?> getQueryBindings();

    byte[] getBytesRepresentation(int parameterIndex) throws SQLException;

    ParseInfo getParseInfo();

    boolean isNull(int paramIndex) throws SQLException;

    String getPreparedSql();

    void setBytes(int parameterIndex, byte[] x, boolean checkForIntroducer, boolean escapeForMBChars) throws SQLException;

    /**
     * Used by updatable result sets for refreshRow() because the parameter has
     * already been escaped for updater or inserter prepared statements.
     * 
     * @param parameterIndex
     *            the parameter to set.
     * @param parameterAsBytes
     *            the parameter as a string.
     * 
     * @throws SQLException
     *             if an error occurs
     */
    void setBytesNoEscape(int parameterIndex, byte[] parameterAsBytes) throws SQLException;

    void setBytesNoEscapeNoQuotes(int parameterIndex, byte[] parameterAsBytes) throws SQLException;

    void setBigInteger(int parameterIndex, BigInteger x) throws SQLException;

    void setNull(int parameterIndex, MysqlType mysqlType) throws SQLException;

}
