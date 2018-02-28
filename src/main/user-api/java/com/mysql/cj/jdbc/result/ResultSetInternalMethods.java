/*
 * Copyright (c) 2007, 2018, Oracle and/or its affiliates. All rights reserved.
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

package com.mysql.cj.jdbc.result;

import java.math.BigInteger;
import java.sql.SQLException;
import java.sql.Types;

import com.mysql.cj.jdbc.JdbcPreparedStatement;
import com.mysql.cj.jdbc.JdbcStatement;
import com.mysql.cj.protocol.Resultset;
import com.mysql.cj.protocol.ResultsetRowsOwner;

/**
 * This interface is intended to be used by implementors of statement interceptors so that implementors can create static or dynamic (via
 * java.lang.reflect.Proxy) proxy instances of ResultSets. It consists of methods outside of java.sql.Result that are used internally by other classes in the
 * driver.
 * 
 * This interface, although public is <strong>not</strong> designed to be consumed publicly other than for the statement interceptor use case.
 */
public interface ResultSetInternalMethods extends java.sql.ResultSet, ResultsetRowsOwner, Resultset {

    /**
     * Functions like ResultSet.getObject(), but using the given SQL type
     * (as registered during CallableStatement.registerOutParameter()).
     * 
     * @param columnIndex
     *            1-based column index
     * @param desiredSqlType
     *            desired column type, one of {@link Types}
     * @return object
     * @throws SQLException
     *             if an error occurs
     */
    Object getObjectStoredProc(int columnIndex, int desiredSqlType) throws SQLException;

    /**
     * Functions like ResultSet.getObject(), but using the given SQL type
     * (as registered during CallableStatement.registerOutParameter()).
     * 
     * @param i
     *            1-based column index
     * @param map
     *            map
     * @param desiredSqlType
     *            desired column type, one of {@link Types}
     * @return object
     * @throws SQLException
     *             if an error occurs
     */
    Object getObjectStoredProc(int i, java.util.Map<Object, Object> map, int desiredSqlType) throws SQLException;

    /**
     * Functions like ResultSet.getObject(), but using the given SQL type
     * (as registered during CallableStatement.registerOutParameter()).
     * 
     * @param columnName
     *            column name
     * @param desiredSqlType
     *            desired column type, one of {@link Types}
     * @return object
     * @throws SQLException
     *             if an error occurs
     */
    Object getObjectStoredProc(String columnName, int desiredSqlType) throws SQLException;

    /**
     * Functions like ResultSet.getObject(), but using the given SQL type
     * (as registered during CallableStatement.registerOutParameter()).
     * 
     * @param colName
     *            column name
     * @param map
     *            map
     * @param desiredSqlType
     *            desired column type, one of {@link Types}
     * @return object
     * @throws SQLException
     *             if an error occurs
     */
    Object getObjectStoredProc(String colName, java.util.Map<Object, Object> map, int desiredSqlType) throws SQLException;

    /**
     * Closes this ResultSet and releases resources.
     * 
     * @param calledExplicitly
     *            was realClose called by the standard ResultSet.close() method, or was it closed internally by the
     *            driver?
     * @throws SQLException
     *             if an error occurs
     */
    void realClose(boolean calledExplicitly) throws SQLException;

    /**
     * Sets the first character of the query that was issued to create
     * this result set. The character should be upper-cased.
     * 
     * @param firstCharUpperCase
     *            character
     */
    void setFirstCharOfQuery(char firstCharUpperCase);

    /**
     * Sets the statement that "owns" this result set (usually used when the
     * result set should internally "belong" to one statement, but is created
     * by another.
     * 
     * @param owningStatement
     *            the statement this result set will belong to
     */
    void setOwningStatement(JdbcStatement owningStatement);

    /**
     * Returns the first character of the query that was issued to create this
     * result set, upper-cased.
     * 
     * @return character
     */
    char getFirstCharOfQuery();

    void setStatementUsedForFetchingRows(JdbcPreparedStatement stmt);

    /**
     * @param wrapperStatement
     *            The wrapperStatement to set.
     */
    void setWrapperStatement(java.sql.Statement wrapperStatement);

    void initializeWithMetadata() throws SQLException;

    void populateCachedMetaData(CachedResultSetMetaData cachedMetaData) throws SQLException;

    BigInteger getBigInteger(int columnIndex) throws SQLException;
}
