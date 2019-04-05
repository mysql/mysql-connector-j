/*
  Copyright (c) 2007, 2019, Oracle and/or its affiliates. All rights reserved.

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

import java.sql.SQLException;

/**
 * This interface is intended to be used by implementors of statement interceptors so that implementors can create static or dynamic (via
 * java.lang.reflect.Proxy) proxy instances of ResultSets. It consists of methods outside of java.sql.Result that are used internally by other classes in the
 * driver.
 * 
 * This interface, although public is <strong>not</strong> designed to be consumed publicly other than for the statement interceptor use case.
 */
public interface ResultSetInternalMethods extends java.sql.ResultSet {

    /**
     * Returns a new instance of this result set, that shares the
     * underlying row data.
     */
    public abstract ResultSetInternalMethods copy() throws SQLException;

    /**
     * Does the result set contain rows, or is it the result of a DDL or DML
     * statement?
     */
    public abstract boolean reallyResult();

    /**
     * Functions like ResultSet.getObject(), but using the given SQL type
     * (as registered during CallableStatement.registerOutParameter()).
     */
    public abstract Object getObjectStoredProc(int columnIndex, int desiredSqlType) throws SQLException;

    /**
     * Functions like ResultSet.getObject(), but using the given SQL type
     * (as registered during CallableStatement.registerOutParameter()).
     */
    public abstract Object getObjectStoredProc(int i, java.util.Map<Object, Object> map, int desiredSqlType) throws SQLException;

    /**
     * Functions like ResultSet.getObject(), but using the given SQL type
     * (as registered during CallableStatement.registerOutParameter()).
     */
    public abstract Object getObjectStoredProc(String columnName, int desiredSqlType) throws SQLException;

    /**
     * Functions like ResultSet.getObject(), but using the given SQL type
     * (as registered during CallableStatement.registerOutParameter()).
     */
    public abstract Object getObjectStoredProc(String colName, java.util.Map<Object, Object> map, int desiredSqlType) throws SQLException;

    /**
     * Returns the server informational message returned from a DDL or DML
     * statement (if any), or null if none.
     */
    public String getServerInfo();

    /**
     * Returns the update count for this result set (if one exists), otherwise
     * -1.
     * 
     * @ return the update count for this result set (if one exists), otherwise
     * -1.
     */
    public long getUpdateCount();

    /**
     * Returns the AUTO_INCREMENT value for the DDL/DML statement which created
     * this result set.
     * 
     * @return the AUTO_INCREMENT value for the DDL/DML statement which created
     *         this result set.
     */
    public long getUpdateID();

    /**
     * Closes this ResultSet and releases resources.
     * 
     * @param calledExplicitly
     *            was realClose called by the standard ResultSet.close() method, or was it closed internally by the
     *            driver?
     */
    public void realClose(boolean calledExplicitly) throws SQLException;

    /**
     * Returns true if this ResultSet is closed
     */
    public boolean isClosed() throws SQLException;

    /**
     * Sets the first character of the query that was issued to create
     * this result set. The character should be upper-cased.
     */
    public void setFirstCharOfQuery(char firstCharUpperCase);

    /**
     * Sets the statement that "owns" this result set (usually used when the
     * result set should internally "belong" to one statement, but is created
     * by another.
     */
    public void setOwningStatement(com.mysql.jdbc.StatementImpl owningStatement);

    /**
     * Returns the first character of the query that was issued to create this
     * result set, upper-cased.
     */
    public char getFirstCharOfQuery();

    /**
     * Clears the reference to the next result set in a multi-result set
     * "chain".
     */
    public void clearNextResult();

    /**
     * Returns the next ResultSet in a multi-resultset "chain", if any,
     * null if none exists.
     */
    public ResultSetInternalMethods getNextResultSet();

    public void setStatementUsedForFetchingRows(PreparedStatement stmt);

    /**
     * @param wrapperStatement
     *            The wrapperStatement to set.
     */
    public void setWrapperStatement(java.sql.Statement wrapperStatement);

    /**
     * Builds a hash between column names and their indices for fast retrieval.
     * This is done lazily to support findColumn() and get*(String), as it
     * can be more expensive than just retrieving result set values by ordinal
     * index.
     */
    public void buildIndexMapping() throws SQLException;

    public void initializeWithMetadata() throws SQLException;

    /**
     * Used by DatabaseMetadata implementations to coerce the metadata returned
     * by metadata queries into that required by the JDBC specification.
     * 
     * @param metadataFields
     *            the coerced metadata to be applied to result sets
     *            returned by "SHOW ..." or SELECTs on INFORMATION_SCHEMA performed on behalf
     *            of methods in DatabaseMetadata.
     */
    public void redefineFieldsForDBMD(Field[] metadataFields);

    public void populateCachedMetaData(CachedResultSetMetaData cachedMetaData) throws SQLException;

    public void initializeFromCachedMetaData(CachedResultSetMetaData cachedMetaData);

    public int getBytesSize() throws SQLException;

    int getId();
}
