/*
  Copyright (c) 2007, 2016, Oracle and/or its affiliates. All rights reserved.

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

package com.mysql.cj.api.jdbc.result;

import java.math.BigInteger;
import java.sql.SQLException;

import com.mysql.cj.api.mysqla.result.Resultset;
import com.mysql.cj.api.mysqla.result.ResultsetRowsOwner;
import com.mysql.cj.jdbc.PreparedStatement;
import com.mysql.cj.jdbc.io.ResultSetFactory;
import com.mysql.cj.jdbc.result.CachedResultSetMetaData;

/**
 * This interface is intended to be used by implementors of statement interceptors so that implementors can create static or dynamic (via
 * java.lang.reflect.Proxy) proxy instances of ResultSets. It consists of methods outside of java.sql.Result that are used internally by other classes in the
 * driver.
 * 
 * This interface, although public is <strong>not</strong> designed to be consumed publicly other than for the statement interceptor use case.
 */
public interface ResultSetInternalMethods extends java.sql.ResultSet, ResultsetRowsOwner, Resultset {

    /**
     * Returns a new instance of this result set, that shares the
     * underlying row data.
     */
    ResultSetInternalMethods copy(ResultSetFactory resultSetFactory) throws SQLException;

    /**
     * Functions like ResultSet.getObject(), but using the given SQL type
     * (as registered during CallableStatement.registerOutParameter()).
     */
    Object getObjectStoredProc(int columnIndex, int desiredSqlType) throws SQLException;

    /**
     * Functions like ResultSet.getObject(), but using the given SQL type
     * (as registered during CallableStatement.registerOutParameter()).
     */
    Object getObjectStoredProc(int i, java.util.Map<Object, Object> map, int desiredSqlType) throws SQLException;

    /**
     * Functions like ResultSet.getObject(), but using the given SQL type
     * (as registered during CallableStatement.registerOutParameter()).
     */
    Object getObjectStoredProc(String columnName, int desiredSqlType) throws SQLException;

    /**
     * Functions like ResultSet.getObject(), but using the given SQL type
     * (as registered during CallableStatement.registerOutParameter()).
     */
    Object getObjectStoredProc(String colName, java.util.Map<Object, Object> map, int desiredSqlType) throws SQLException;

    /**
     * Closes this ResultSet and releases resources.
     * 
     * @param calledExplicitly
     *            was realClose called by the standard ResultSet.close() method, or was it closed internally by the
     *            driver?
     */
    void realClose(boolean calledExplicitly) throws SQLException;

    /**
     * Sets the first character of the query that was issued to create
     * this result set. The character should be upper-cased.
     */
    void setFirstCharOfQuery(char firstCharUpperCase);

    /**
     * Sets the statement that "owns" this result set (usually used when the
     * result set should internally "belong" to one statement, but is created
     * by another.
     */
    void setOwningStatement(com.mysql.cj.jdbc.StatementImpl owningStatement);

    /**
     * Returns the first character of the query that was issued to create this
     * result set, upper-cased.
     */
    char getFirstCharOfQuery();

    void setStatementUsedForFetchingRows(PreparedStatement stmt);

    /**
     * @param wrapperStatement
     *            The wrapperStatement to set.
     */
    void setWrapperStatement(java.sql.Statement wrapperStatement);

    void initializeWithMetadata() throws SQLException;

    void populateCachedMetaData(CachedResultSetMetaData cachedMetaData) throws SQLException;

    BigInteger getBigInteger(int columnIndex) throws SQLException;
}
