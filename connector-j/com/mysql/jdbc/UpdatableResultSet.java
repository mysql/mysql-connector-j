/*
 Copyright (C) 2002 MySQL AB

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation; either version 2 of the License, or
   (at your option) any later version.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
   
 */
package com.mysql.jdbc;

import java.io.InputStream;

import java.math.BigDecimal;

import java.net.URL;

import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.Ref;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;

import java.util.ArrayList;


/**
 * @author Administrator
 *
 * To change this generated comment edit the template variable "typecomment":
 * Window>Preferences>Java>Templates.
 * To enable and disable the creation of type comments go to
 * Window>Preferences>Java>Code Generation.
 */
public class UpdatableResultSet
    extends ResultSet
{

    //~ Instance/static variables .............................................

    static private String quotedIdChar = null;
    protected String      deleteSQL = null;

    /**
     * PreparedStatement used to delete data
     */
    protected com.mysql.jdbc.PreparedStatement deleter   = null;
    protected String                           insertSQL = null;

    /**
     * PreparedStatement used to insert data
     */
    protected com.mysql.jdbc.PreparedStatement inserter = null;

    /**
     * List of primary keys
     */
    protected ArrayList primaryKeyIndicies = null;
    protected String    refreshSQL = null;

    /**
     * PreparedStatement used to refresh data
     */
    protected com.mysql.jdbc.PreparedStatement refresher;

    /**
     * SQL for in-place modifcation
     */
    protected String updateSQL = null;

    /**
     * PreparedStatement used to delete data
     */
    protected com.mysql.jdbc.PreparedStatement updater = null;

    /**
     * Are we in the middle of doing updates to the current row?
     */
    protected boolean doingUpdates = false;

    /**
     * Are we on the insert row?
     */
    protected boolean onInsertRow = false;

    /**
     * Is this result set updateable?
     */
    protected boolean isUpdatable = false;

    //~ Constructors ..........................................................

    // ****************************************************************
    //
    //                       END OF PUBLIC INTERFACE
    //
    // ****************************************************************

    /**
     * Create a new ResultSet - Note that we create ResultSets to
     * represent the results of everything.
     *
     * @param fields an array of Field objects (basically, the
     *    ResultSet MetaData)
     * @param tuples Vector of the actual data
     * @param status the status string returned from the back end
     * @param updateCount the number of rows affected by the operation
     * @param cursor the positioned update/delete cursor name
     */
    public UpdatableResultSet(Field[] fields, RowData rows, 
                              com.mysql.jdbc.Connection conn) throws SQLException
    {
        super(fields, rows, conn);
        isUpdatable = isUpdateable();
    }

    public UpdatableResultSet(Field[] fields, RowData rows) throws SQLException
    {
        super(fields, rows);
        isUpdatable = isUpdateable();
    }

    public UpdatableResultSet(long updateCount, long updateID)
    {
        super(updateCount, updateID);
    }

    //~ Methods ...............................................................

    /**
     * JDBC 2.0
     *
     * <p>Determine if the cursor is after the last row in the result 
     * set.   
     *
     * @return true if after the last row, false otherwise.  Returns
     * false when the result set contains no rows.
     * @exception SQLException if a database-access error occurs.
     */
    public synchronized boolean isAfterLast()
                                     throws SQLException
    {

        return super.isAfterLast();
    }

    /**
     * A column value can be retrieved as a stream of ASCII characters
     * and then read in chunks from the stream.  This method is
     * particulary suitable for retrieving large LONGVARCHAR values.
     * The JDBC driver will do any necessary conversion from the
     * database format into ASCII.
     *
     * <p><B>Note:</B> All the data in the returned stream must be read
     * prior to getting the value of any other column.  The next call
     * to a get method implicitly closes the stream.  Also, a stream
     * may return 0 for available() whether there is data available
     * or not.
     *
     * @param columnIndex the first column is 1, the second is 2, ...
     * @return a Java InputStream that delivers the database column
     *    value as a stream of one byte ASCII characters.  If the
     *    value is SQL NULL then the result is null
     * @exception java.sql.SQLException if a database access error occurs
     * @see getBinaryStream
     */
    public synchronized InputStream getAsciiStream(int columnIndex)
                                            throws java.sql.SQLException
    {

        return super.getAsciiStream(columnIndex);
    }

    public synchronized InputStream getAsciiStream(String columnName)
                                            throws java.sql.SQLException
    {

        return super.getAsciiStream(columnName);
    }

    //---------------------------------------------------------------------
    // Traversal/Positioning
    //---------------------------------------------------------------------

    /**
     * JDBC 2.0
     *
     * <p>Determine if the cursor is before the first row in the result 
     * set.   
     *
     * @return true if before the first row, false otherwise. Returns
     * false when the result set contains no rows.
     * @exception SQLException if a database-access error occurs.
     */
    public synchronized boolean isBeforeFirst()
                                       throws SQLException
    {

        return super.isBeforeFirst();
    }

    /**
     * Get the value of a column in the current row as a
     * java.lang.BigDecimal object
     *
     * @param columnIndex  the first column is 1, the second is 2...
     * @param scale the number of digits to the right of the decimal
     * @return the column value; if the value is SQL NULL, null
     * @exception java.sql.SQLException if a database access error occurs
     */
    public synchronized BigDecimal getBigDecimal(int columnIndex, int scale)
                                          throws java.sql.SQLException
    {

        return super.getBigDecimal(columnIndex, scale);
    }

    public synchronized BigDecimal getBigDecimal(String ColumnName, int scale)
                                          throws java.sql.SQLException
    {

        return super.getBigDecimal(findColumn(ColumnName), scale);
    }

    /**
     * A column value can also be retrieved as a binary strea.  This
     * method is suitable for retrieving LONGVARBINARY values.
     *
     * @param columnIndex the first column is 1, the second is 2...
     * @return a Java InputStream that delivers the database column value
     * as a stream of bytes.  If the value is SQL NULL, then the result
     * is null
     * @exception java.sql.SQLException if a database access error occurs
     * @see getAsciiStream
     * @see getUnicodeStream
     */
    public synchronized InputStream getBinaryStream(int columnIndex)
                                             throws java.sql.SQLException
    {

        return super.getBinaryStream(columnIndex);
    }

    public synchronized InputStream getBinaryStream(String columnName)
                                             throws java.sql.SQLException
    {

        return super.getBinaryStream(columnName);
    }

    /**
     * Get the value of a column in the current row as a Java boolean
     *
     * @param columnIndex the first column is 1, the second is 2...
     * @return the column value, false for SQL NULL
     * @exception java.sql.SQLException if a database access error occurs
     */
    public synchronized boolean getBoolean(int columnIndex)
                                    throws java.sql.SQLException
    {

        return super.getBoolean(columnIndex);
    }

    public synchronized boolean getBoolean(String ColumnName)
                                    throws java.sql.SQLException
    {

        return super.getBoolean(findColumn(ColumnName));
    }

    /**
     * Get the value of a column in the current row as a Java byte.
     *
     * @param columnIndex the first column is 1, the second is 2,...
     * @return the column value; 0 if SQL NULL
     * @exception java.sql.SQLException if a database access error occurs
     */
    public synchronized byte getByte(int columnIndex)
                              throws java.sql.SQLException
    {

        return super.getByte(columnIndex);
    }

    public synchronized byte getByte(String ColumnName)
                              throws java.sql.SQLException
    {

        return super.getByte(findColumn(ColumnName));
    }

    /**
     * Get the value of a column in the current row as a Java byte array.
     *
     * <p><b>Be warned</b> If the blob is huge, then you may run out
     * of memory.
     *
     * @param columnIndex the first column is 1, the second is 2, ...
     * @return the column value; if the value is SQL NULL, the result
     *    is null
     * @exception java.sql.SQLException if a database access error occurs
     */
    public synchronized byte[] getBytes(int columnIndex)
                                 throws java.sql.SQLException
    {

        return super.getBytes(columnIndex);
    }

    public synchronized byte[] getBytes(String ColumnName)
                                 throws java.sql.SQLException
    {

        return super.getBytes(findColumn(ColumnName));
    }

    /**
     * JDBC 2.0
     *
     * Return the concurrency of this result set.  The concurrency
     * used is determined by the statement that created the result set.
     *
     * @return the concurrency type, CONCUR_READ_ONLY, etc.
     * @exception SQLException if a database-access error occurs
     */
    public int getConcurrency()
                       throws SQLException
    {

        return (isUpdatable ? CONCUR_UPDATABLE : CONCUR_READ_ONLY);
    }

    /**
     * Get the name of the SQL cursor used by this ResultSet
     *
     * <p>In SQL, a result table is retrieved though a cursor that is
     * named.  The current row of a result can be updated or deleted
     * using a positioned update/delete statement that references
     * the cursor name.
     *
     * <p>JDBC supports this SQL feature by providing the name of the
     * SQL cursor used by a ResultSet.  The current row of a ResulSet
     * is also the current row of this SQL cursor.
     *
     * <p><B>Note:</B> If positioned update is not supported, a java.sql.SQLException
     * is thrown.
     *
     * @return the ResultSet's SQL cursor name.
     * @exception java.sql.SQLException if a database access error occurs
     */
    public synchronized String getCursorName()
                                      throws java.sql.SQLException
    {

        return super.getCursorName();
    }

    /**
     * Get the value of a column in the current row as a java.sql.Date
     * object
     *
     * @param columnIndex the first column is 1, the second is 2...
     * @return the column value; null if SQL NULL
     * @exception java.sql.SQLException if a database access error occurs
     */
    public synchronized java.sql.Date getDate(int columnIndex)
                                       throws java.sql.SQLException
    {

        return super.getDate(columnIndex);
    }

    public synchronized java.sql.Date getDate(String ColumnName)
                                       throws java.sql.SQLException
    {

        return super.getDate(findColumn(ColumnName));
    }

    /**
     * Get the value of a column in the current row as a Java double.
     *
     * @param columnIndex the first column is 1, the second is 2,...
     * @return the column value; 0 if SQL NULL
     * @exception java.sql.SQLException if a database access error occurs
     */
    public synchronized double getDouble(int columnIndex)
                                  throws java.sql.SQLException
    {

        return super.getDouble(columnIndex);
    }

    public synchronized double getDouble(String ColumnName)
                                  throws java.sql.SQLException
    {

        return super.getDouble(findColumn(ColumnName));
    }

    /**
     * JDBC 2.0
     *
     * <p>Determine if the cursor is on the first row of the result set.   
     *
     * @return true if on the first row, false otherwise.   
     * @exception SQLException if a database-access error occurs.
     */
    public synchronized boolean isFirst()
                                 throws SQLException
    {

        return super.isFirst();
    }

    /**
     * Get the value of a column in the current row as a Java float.
     *
     * @param columnIndex the first column is 1, the second is 2,...
     * @return the column value; 0 if SQL NULL
     * @exception java.sql.SQLException if a database access error occurs
     */
    public synchronized float getFloat(int columnIndex)
                                throws java.sql.SQLException
    {

        return super.getFloat(columnIndex);
    }

    public synchronized float getFloat(String ColumnName)
                                throws java.sql.SQLException
    {

        return super.getFloat(findColumn(ColumnName));
    }

    /**
     * Get the value of a column in the current row as a Java int.
     *
     * @param columnIndex the first column is 1, the second is 2,...
     * @return the column value; 0 if SQL NULL
     * @exception java.sql.SQLException if a database access error occurs
     */
    public synchronized int getInt(int columnIndex)
                            throws java.sql.SQLException
    {

        return super.getInt(columnIndex);
    }

    public synchronized int getInt(String ColumnName)
                            throws java.sql.SQLException
    {

        return super.getInt(findColumn(ColumnName));
    }

    /**
     * JDBC 2.0
     *
     * <p>Determine if the cursor is on the last row of the result set.   
     * Note: Calling isLast() may be expensive since the JDBC driver
     * might need to fetch ahead one row in order to determine 
     * whether the current row is the last row in the result set.
     *
     * @return true if on the last row, false otherwise. 
     * @exception SQLException if a database-access error occurs.
     */
    public synchronized boolean isLast()
                                throws SQLException
    {

        return super.isLast();
    }

    /**
     * Get the value of a column in the current row as a Java long.
     *
     * @param columnIndex the first column is 1, the second is 2,...
     * @return the column value; 0 if SQL NULL
     * @exception java.sql.SQLException if a database access error occurs
     */
    public synchronized long getLong(int columnIndex)
                              throws java.sql.SQLException
    {

        return super.getLong(columnIndex);
    }

    public synchronized long getLong(String ColumnName)
                              throws java.sql.SQLException
    {

        return super.getLong(findColumn(ColumnName));
    }

    /**
     * Get the value of a column in the current row as a Java object
     *
     * <p>This method will return the value of the given column as a
     * Java object.  The type of the Java object will be the default
     * Java Object type corresponding to the column's SQL type, following
     * the mapping specified in the JDBC specification.
     *
     * <p>This method may also be used to read database specific abstract
     * data types.
     *
     * @param columnIndex the first column is 1, the second is 2...
     * @return a Object holding the column value
     * @exception java.sql.SQLException if a database access error occurs
     */
    public synchronized Object getObject(int columnIndex)
                                  throws java.sql.SQLException
    {

        return super.getObject(columnIndex);
    }

    /**
     * Get the value of a column in the current row as a Java object
     *
     *<p> This method will return the value of the given column as a
     * Java object.  The type of the Java object will be the default
     * Java Object type corresponding to the column's SQL type, following
     * the mapping specified in the JDBC specification.
     *
     * <p>This method may also be used to read database specific abstract
     * data types.
     *
     * @param columnName is the SQL name of the column
     * @return a Object holding the column value
     * @exception java.sql.SQLException if a database access error occurs
     */
    public synchronized Object getObject(String columnName)
                                  throws java.sql.SQLException
    {

        return super.getObject(columnName);
    }

    /**
     * JDBC 2.0
     *
     * <p>Determine the current row number.  The first row is number 1, the
     * second number 2, etc.  
     *
     * @return the current row number, else return 0 if there is no 
     * current row
     * @exception SQLException if a database-access error occurs.
     */
    public synchronized int getRow()
                            throws SQLException
    {

        return super.getRow();
    }

    /**
     * Get the value of a column in the current row as a Java short.
     *
     * @param columnIndex the first column is 1, the second is 2,...
     * @return the column value; 0 if SQL NULL
     * @exception java.sql.SQLException if a database access error occurs
     */
    public synchronized short getShort(int columnIndex)
                                throws java.sql.SQLException
    {

        return super.getShort(columnIndex);
    }

    public synchronized short getShort(String ColumnName)
                                throws java.sql.SQLException
    {

        return super.getShort(findColumn(ColumnName));
    }

    /**
     * Get the value of a column in the current row as a Java String
     *
     * @param columnIndex the first column is 1, the second is 2...
     * @return the column value, null for SQL NULL
     * @exception java.sql.SQLException if a database access error occurs
     */
    public synchronized String getString(int columnIndex)
                                  throws java.sql.SQLException
    {

        return super.getString(columnIndex);
    }

    /**
     * The following routines simply convert the columnName into
     * a columnIndex and then call the appropriate routine above.
     *
     * @param columnName is the SQL name of the column
     * @return the column value
     * @exception java.sql.SQLException if a database access error occurs
     */
    public synchronized String getString(String ColumnName)
                                  throws java.sql.SQLException
    {

        return super.getString(findColumn(ColumnName));
    }

    /**
     * Get the value of a column in the current row as a java.sql.Time
     * object
     *
     * @param columnIndex the first column is 1, the second is 2...
     * @return the column value; null if SQL NULL
     * @exception java.sql.SQLException if a database access error occurs
     */
    public synchronized Time getTime(int columnIndex)
                              throws java.sql.SQLException
    {

        return super.getTime(columnIndex);
    }

    public synchronized Time getTime(String ColumnName)
                              throws java.sql.SQLException
    {

        return super.getTime(findColumn(ColumnName));
    }

    /**
     * Get the value of a column in the current row as a
     * java.sql.Timestamp object
     *
     * @param columnIndex the first column is 1, the second is 2...
     * @return the column value; null if SQL NULL
     * @exception java.sql.SQLException if a database access error occurs
     */
    public synchronized Timestamp getTimestamp(int columnIndex)
                                        throws java.sql.SQLException
    {

        return super.getTimestamp(columnIndex);
    }

    public synchronized Timestamp getTimestamp(String ColumnName)
                                        throws java.sql.SQLException
    {

        return super.getTimestamp(ColumnName);
    }

    /**
     * A column value can also be retrieved as a stream of Unicode
     * characters. We implement this as a binary stream.
     *
     * @param columnIndex the first column is 1, the second is 2...
     * @return a Java InputStream that delivers the database column value
     *    as a stream of two byte Unicode characters.  If the value is
     *    SQL NULL, then the result is null
     * @exception java.sql.SQLException if a database access error occurs
     * @see getAsciiStream
     * @see getBinaryStream
     */
    public synchronized InputStream getUnicodeStream(int columnIndex)
                                              throws java.sql.SQLException
    {

        return super.getUnicodeStream(columnIndex);
    }

    public synchronized InputStream getUnicodeStream(String columnName)
                                              throws java.sql.SQLException
    {

        return super.getUnicodeStream(columnName);
    }

    /**
     * JDBC 2.0
     *
     * <p>Move to an absolute row number in the result set.
     *
     * <p>If row is positive, moves to an absolute row with respect to the
     * beginning of the result set.  The first row is row 1, the second
     * is row 2, etc. 
     *
     * <p>If row is negative, moves to an absolute row position with respect to
     * the end of result set.  For example, calling absolute(-1) positions the 
     * cursor on the last row, absolute(-2) indicates the next-to-last
     * row, etc.
     *
     * <p>An attempt to position the cursor beyond the first/last row in
     * the result set, leaves the cursor before/after the first/last
     * row, respectively.
     *
     * <p>Note: Calling absolute(1) is the same as calling first().
     * Calling absolute(-1) is the same as calling last().
     *
     * @return true if on the result set, false if off.
     * @exception SQLException if a database-access error occurs, or 
     * row is 0, or result set type is TYPE_FORWARD_ONLY.
     */
    public synchronized boolean absolute(int row)
                                  throws SQLException
    {

        return super.absolute(row);
    }

    /**
     * JDBC 2.0
     *
     * <p>Moves to the end of the result set, just after the last
     * row.  Has no effect if the result set contains no rows.
     *
     * @exception SQLException if a database-access error occurs, or
     * result set type is TYPE_FORWARD_ONLY.
     */
    public synchronized void afterLast()
                                throws SQLException
    {
        super.afterLast();
    }

    /**
     * JDBC 2.0
     *
     * <p>Moves to the front of the result set, just before the
     * first row. Has no effect if the result set contains no rows.
     *
     * @exception SQLException if a database-access error occurs, or
     * result set type is TYPE_FORWARD_ONLY
     */
    public synchronized void beforeFirst()
                                  throws SQLException
    {
        super.beforeFirst();
    }

    /**
     * JDBC 2.0
     *
     * The cancelRowUpdates() method may be called after calling an
     * updateXXX() method(s) and before calling updateRow() to rollback 
     * the updates made to a row.  If no updates have been made or 
     * updateRow() has already been called, then this method has no 
     * effect.
     *
     * @exception SQLException if a database-access error occurs, or if
     * called when on the insert row.
     *
     */
    public synchronized void cancelRowUpdates()
                                       throws SQLException
    {

        if (doingUpdates) {
            doingUpdates = false;
            updater.clearParameters();
        }
    }

    /**
     * After this call, getWarnings returns null until a new warning
     * is reported for this ResultSet
     *
     * @exception java.sql.SQLException if a database access error occurs
     */
    public synchronized void clearWarnings()
                                    throws java.sql.SQLException
    {
        warningChain = null;
    }

    /**
     * In some cases, it is desirable to immediately release a ResultSet
     * database and JDBC resources instead of waiting for this to happen
     * when it is automatically closed.  The close method provides this
     * immediate release.
     *
     * <p><B>Note:</B> A ResultSet is automatically closed by the Statement
     * the Statement that generated it when that Statement is closed,
     * re-executed, or is used to retrieve the next result from a sequence
     * of multiple results.  A ResultSet is also automatically closed
     * when it is garbage collected.
     *
     * @exception java.sql.SQLException if a database access error occurs
     */
    public synchronized void close()
                            throws java.sql.SQLException
    {
        super.close();
    }

    /**
     * JDBC 2.0
     *
     * Delete the current row from the result set and the underlying
     * database.  Cannot be called when on the insert row.
     *
     * @exception SQLException if a database-access error occurs, or if
     * called when on the insert row.
     */
    public synchronized void deleteRow()
                                throws SQLException
    {

        if (!isUpdatable) {
            throw new NotUpdatable();
        }

        if (onInsertRow) {
            throw new SQLException("Can not call deleteRow() when on insert row");
        } else if (rowData.size() == 0) {
            throw new SQLException("Can't deleteRow() on empty result set");
        } else if (isBeforeFirst()) {
            throw new SQLException("Before start of result set. Can not call deleteRow().");
        } else if (isAfterLast()) {
            throw new SQLException("After end of result set. Can not call deleteRow().");
        }

        if (deleter == null) {

            if (deleteSQL == null) {
                generateStatements();
            }

            deleter = (com.mysql.jdbc.PreparedStatement)connection.prepareStatement(
                               deleteSQL);
        }

        deleter.clearParameters();

        String Encoding = null;

        if (connection.useUnicode()) {
            Encoding = connection.getEncoding();
        }

        try {

            int num_keys = primaryKeyIndicies.size();

            if (num_keys == 1) {

                int    index      = ((Integer)primaryKeyIndicies.get(0)).intValue();
                String CurrentVal = (Encoding == null
                                         ? new String(thisRow[index])
                                         : new String(thisRow[index], 
                                                      Encoding));
                deleter.setString(1, CurrentVal);
            } else {

                for (int i = 0; i < num_keys; i++) {

                    int    index      = ((Integer)primaryKeyIndicies.get(i)).intValue();
                    String CurrentVal = (Encoding == null
                                             ? new String(thisRow[index])
                                             : new String(thisRow[index], 
                                                          Encoding));
                    deleter.setString(i + 1, CurrentVal);
                }
            }

            deleter.executeUpdate();
            rowData.removeRow(rowData.getCurrentRowNumber());
        } catch (java.io.UnsupportedEncodingException UE) {
            throw new SQLException("Unsupported character encoding '" + 
                                   connection.getEncoding() + "'");
        }
    }

    /**
     * JDBC 2.0
     *
     * <p>Moves to the first row in the result set.  
     *
     * @return true if on a valid row, false if no rows in the result set.
     * @exception SQLException if a database-access error occurs, or
     * result set type is TYPE_FORWARD_ONLY.
     */
    public synchronized boolean first()
                               throws SQLException
    {

        return super.first();
    }

    /**
     * JDBC 2.0
     *
     * Insert the contents of the insert row into the result set and
     * the database.  Must be on the insert row when this method is called.
     *
     * @exception SQLException if a database-access error occurs,
     * if called when not on the insert row, or if all non-nullable columns in
     * the insert row have not been given a value
     */
    public synchronized void insertRow()
                                throws SQLException
    {

        if (!onInsertRow) {
            throw new SQLException("Not on insert row");
        } else {
            inserter.executeUpdate();

            int numPrimaryKeys = 0;

            if (primaryKeyIndicies != null) {
                numPrimaryKeys = primaryKeyIndicies.size();
            }

            long     autoIncrementId = inserter.getLastInsertID();
            int      num_fields = fields.length;
            byte[][] NewRow     = new byte[num_fields][];

            for (int i = 0; i < num_fields; i++) {

                if (inserter.isNull(i)) {
                    NewRow[i] = null;
                } else {
                    NewRow[i] = inserter.getBytes(i);
                }

                if (numPrimaryKeys == 1 && fields[i].isPrimaryKey() && 
                    autoIncrementId > 0) {
                    NewRow[i] = String.valueOf(autoIncrementId).getBytes();
                }
            }

            rowData.addRow(NewRow);
            resetInserter();
        }
    }

    /**
     * JDBC 2.0
     *
     * <p>Moves to the last row in the result set.  
     *
     * @return true if on a valid row, false if no rows in the result set.
     * @exception SQLException if a database-access error occurs, or
     * result set type is TYPE_FORWARD_ONLY.
     */
    public synchronized boolean last()
                              throws SQLException
    {

        return super.last();
    }

    /**
     * JDBC 2.0
     *
     * Move the cursor to the remembered cursor position, usually the
     * current row.  Has no effect unless the cursor is on the insert 
     * row. 
     *
     * @exception SQLException if a database-access error occurs,
     * or the result set is not updatable
     */
    public synchronized void moveToCurrentRow()
                                       throws SQLException
    {

        if (!isUpdatable) {
            throw new NotUpdatable();
        }

        onInsertRow = false;
    }

    /**
     * JDBC 2.0
     *
     * Move to the insert row.  The current cursor position is 
     * remembered while the cursor is positioned on the insert row.
     *
     * The insert row is a special row associated with an updatable
     * result set.  It is essentially a buffer where a new row may
     * be constructed by calling the updateXXX() methods prior to 
     * inserting the row into the result set.  
     *
     * Only the updateXXX(), getXXX(), and insertRow() methods may be 
     * called when the cursor is on the insert row.  All of the columns in 
     * a result set must be given a value each time this method is
     * called before calling insertRow().  UpdateXXX()must be called before
     * getXXX() on a column.
     *
     * @exception SQLException if a database-access error occurs,
     * or the result set is not updatable
     */
    public synchronized void moveToInsertRow()
                                      throws SQLException
    {

        if (!isUpdatable) {
            throw new NotUpdatable();
        }

        if (inserter == null) {
            generateStatements();
            inserter = (com.mysql.jdbc.PreparedStatement)connection.prepareStatement(
                                insertSQL);
            resetInserter();
        } else {
            resetInserter();
        }

        onInsertRow = true;
        doingUpdates = false;
    }

    /**
     * A ResultSet is initially positioned before its first row,
     * the first call to next makes the first row the current row;
     * the second call makes the second row the current row, etc.
     *
     * <p>If an input stream from the previous row is open, it is
     * implicitly closed.  The ResultSet's warning chain is cleared
     * when a new row is read
     *
     * @return true if the new current is valid; false if there are no
     *    more rows
     * @exception java.sql.SQLException if a database access error occurs
     */
    public synchronized boolean next()
                              throws java.sql.SQLException
    {

        return super.next();
    }

    /**
     * The prev method is not part of JDBC, but because of the
     * architecture of this driver it is possible to move both
     * forward and backward within the result set.
     *
     * <p>If an input stream from the previous row is open, it is
     * implicitly closed.  The ResultSet's warning chain is cleared
     * when a new row is read
     *
     * @return true if the new current is valid; false if there are no
     *    more rows
     * @exception java.sql.SQLException if a database access error occurs
     */
    public synchronized boolean prev()
                              throws java.sql.SQLException
    {

        return super.prev();
    }

    /**
     * JDBC 2.0
     *
     * <p>Moves to the previous row in the result set.  
     *
     * <p>Note: previous() is not the same as relative(-1) since it
     * makes sense to call previous() when there is no current row.
     *
     * @return true if on a valid row, false if off the result set.
     * @exception SQLException if a database-access error occurs, or
     * result set type is TYPE_FORWAR_DONLY.
     */
    public synchronized boolean previous()
                                  throws SQLException
    {

        return super.previous();
    }

    /**
     * JDBC 2.0
     *
     * Refresh the value of the current row with its current value in 
     * the database.  Cannot be called when on the insert row.
     *
     * The refreshRow() method provides a way for an application to 
     * explicitly tell the JDBC driver to refetch a row(s) from the
     * database.  An application may want to call refreshRow() when 
     * caching or prefetching is being done by the JDBC driver to
     * fetch the latest value of a row from the database.  The JDBC driver 
     * may actually refresh multiple rows at once if the fetch size is 
     * greater than one.
     * 
     * All values are refetched subject to the transaction isolation 
     * level and cursor sensitivity.  If refreshRow() is called after
     * calling updateXXX(), but before calling updateRow() then the
     * updates made to the row are lost.  Calling refreshRow() frequently
     * will likely slow performance.
     *
     * @exception SQLException if a database-access error occurs, or if
     * called when on the insert row.
     */
    public synchronized void refreshRow()
                                 throws SQLException
    {

        if (!isUpdatable) {
            throw new NotUpdatable();
        }

        if (onInsertRow) {
            throw new SQLException("Can not call refreshRow() when on insert row");
        } else if (rowData.size() == 0) {
            throw new SQLException("Can't refreshRow() on empty result set");
        } else if (isBeforeFirst()) {
            throw new SQLException("Before start of result set. Can not call refreshRow().");
        } else if (isAfterLast()) {
            throw new SQLException("After end of result set. Can not call refreshRow().");
        }

        if (refresher == null) {

            if (refreshSQL == null) {
                generateStatements();
            }

            refresher = (com.mysql.jdbc.PreparedStatement)connection.prepareStatement(
                                 refreshSQL);
        }

        refresher.clearParameters();

        String Encoding = null;

        if (connection.useUnicode()) {
            Encoding = connection.getEncoding();
        }

        try {

            int num_keys = primaryKeyIndicies.size();

            if (num_keys == 1) {

                int    index      = ((Integer)primaryKeyIndicies.get(0)).intValue();
                String CurrentVal = (Encoding == null
                                         ? new String(thisRow[index])
                                         : new String(thisRow[index], 
                                                      Encoding));
                refresher.setString(1, CurrentVal);
            } else {

                for (int i = 0; i < num_keys; i++) {

                    int    index      = ((Integer)primaryKeyIndicies.get(i)).intValue();
                    String CurrentVal = (Encoding == null
                                             ? new String(thisRow[index])
                                             : new String(thisRow[index], 
                                                          Encoding));
                    refresher.setString(i + 1, CurrentVal);
                }
            }

            java.sql.ResultSet rs = null;

            try {
                rs = refresher.executeQuery();

                int numCols = rs.getMetaData().getColumnCount();

                if (rs.next()) {

                    for (int i = 0; i < numCols; i++) {

                        byte[] val = rs.getBytes(i + 1);

                        if (val == null || rs.wasNull()) {
                            thisRow[i] = null;
                        } else {
                            thisRow[i] = rs.getBytes(i + 1);
                        }
                    }
                } else {
                    throw new SQLException("refreshRow() called on row that has been deleted or had primary key changed", 
                                           "S1000");
                }
            } finally {

                if (rs != null) {

                    try {
                        rs.close();
                    } catch (Exception ex) {
                    }
                }
            }
        } catch (java.io.UnsupportedEncodingException UE) {
            throw new SQLException("Unsupported character encoding '" + 
                                   connection.getEncoding() + "'");
        }
    }

    /**
     * JDBC 2.0
     *
     * <p>Moves a relative number of rows, either positive or negative.
     * Attempting to move beyond the first/last row in the
     * result set positions the cursor before/after the
     * the first/last row. Calling relative(0) is valid, but does
     * not change the cursor position.
     *
     * <p>Note: Calling relative(1) is different than calling next()
     * since is makes sense to call next() when there is no current row,
     * for example, when the cursor is positioned before the first row
     * or after the last row of the result set.
     *
     * @return true if on a row, false otherwise.
     * @exception SQLException if a database-access error occurs, or there
     * is no current row, or result set type is TYPE_FORWARD_ONLY.
     */
    public synchronized boolean relative(int rows)
                                  throws SQLException
    {

        return super.relative(rows);
    }

    /**
     * JDBC 2.0
     *
     * Determine if this row has been deleted.  A deleted row may leave
     * a visible "hole" in a result set.  This method can be used to
     * detect holes in a result set.  The value returned depends on whether 
     * or not the result set can detect deletions.
     *
     * @return true if deleted and deletes are detected
     * @exception SQLException if a database-access error occurs
     * 
     * @see DatabaseMetaData#deletesAreDetected
     */
    public synchronized boolean rowDeleted()
                                    throws SQLException
    {
        throw new NotImplemented();
    }

    /**
     * JDBC 2.0
     *
     * Determine if the current row has been inserted.  The value returned 
     * depends on whether or not the result set can detect visible inserts.
     *
     * @return true if inserted and inserts are detected
     * @exception SQLException if a database-access error occurs
     * 
     * @see DatabaseMetaData#insertsAreDetected
     */
    public synchronized boolean rowInserted()
                                     throws SQLException
    {
        throw new NotImplemented();
    }

    //---------------------------------------------------------------------
    // Updates
    //---------------------------------------------------------------------

    /**
     * JDBC 2.0
     *
     * Determine if the current row has been updated.  The value returned 
     * depends on whether or not the result set can detect updates.
     *
     * @return true if the row has been visibly updated by the owner or
     * another, and updates are detected
     * @exception SQLException if a database-access error occurs
     * 
     * @see DatabaseMetaData#updatesAreDetected
     */
    public synchronized boolean rowUpdated()
                                    throws SQLException
    {
        throw new NotImplemented();
    }

    /** 
     * JDBC 2.0
     *  
     * Update a column with an ascii stream value.
     *
     * The updateXXX() methods are used to update column values in the
     * current row, or the insert row.  The updateXXX() methods do not 
     * update the underlying database, instead the updateRow() or insertRow()
     * methods are called to update the database.
     *
     * @param columnIndex the first column is 1, the second is 2, ...
     * @param x the new column value
     * @param length the length of the stream
     * @exception SQLException if a database-access error occurs
     */
    public synchronized void updateAsciiStream(int columnIndex, 
                                               java.io.InputStream x, 
                                               int length)
                                        throws SQLException
    {

        if (!onInsertRow) {

            if (!doingUpdates) {
                doingUpdates = true;
                syncUpdate();
            }

            updater.setAsciiStream(columnIndex, x, length);
        } else {
            inserter.setAsciiStream(columnIndex, x, length);
        }
    }

    /** 
     * JDBC 2.0
     *  
     * Update a column with an ascii stream value.
     *
     * The updateXXX() methods are used to update column values in the
     * current row, or the insert row.  The updateXXX() methods do not 
     * update the underlying database, instead the updateRow() or insertRow()
     * methods are called to update the database.
     *
     * @param columnName the name of the column
     * @param x the new column value
     * @param length of the stream
     * @exception SQLException if a database-access error occurs
     */
    public synchronized void updateAsciiStream(String columnName, 
                                               java.io.InputStream x, 
                                               int length)
                                        throws SQLException
    {
        updateAsciiStream(findColumn(columnName), x, length);
    }

    /**
     * JDBC 2.0
     *  
     * Update a column with a BigDecimal value.
     *
     * The updateXXX() methods are used to update column values in the
     * current row, or the insert row.  The updateXXX() methods do not 
     * update the underlying database, instead the updateRow() or insertRow()
     * methods are called to update the database.
     *
     * @param columnIndex the first column is 1, the second is 2, ...
     * @param x the new column value
     * @exception SQLException if a database-access error occurs
     */
    public synchronized void updateBigDecimal(int columnIndex, BigDecimal x)
                                       throws SQLException
    {

        if (!onInsertRow) {

            if (!doingUpdates) {
                doingUpdates = true;
                syncUpdate();
            }

            updater.setBigDecimal(columnIndex, x);
        } else {
            inserter.setBigDecimal(columnIndex, x);
        }
    }

    /**
     * JDBC 2.0
     *  
     * Update a column with a BigDecimal value.
     *
     * The updateXXX() methods are used to update column values in the
     * current row, or the insert row.  The updateXXX() methods do not 
     * update the underlying database, instead the updateRow() or insertRow()
     * methods are called to update the database.
     *
     * @param columnName the name of the column
     * @param x the new column value
     * @exception SQLException if a database-access error occurs
     */
    public synchronized void updateBigDecimal(String columnName, BigDecimal x)
                                       throws SQLException
    {
        updateBigDecimal(findColumn(columnName), x);
    }

    /** 
     * JDBC 2.0
     *  
     * Update a column with a binary stream value.
     *
     * The updateXXX() methods are used to update column values in the
     * current row, or the insert row.  The updateXXX() methods do not 
     * update the underlying database, instead the updateRow() or insertRow()
     * methods are called to update the database.
     *
     * @param columnIndex the first column is 1, the second is 2, ...
     * @param x the new column value     
     * @param length the length of the stream
     * @exception SQLException if a database-access error occurs
     */
    public synchronized void updateBinaryStream(int columnIndex, 
                                                java.io.InputStream x, 
                                                int length)
                                         throws SQLException
    {

        if (!onInsertRow) {

            if (!doingUpdates) {
                doingUpdates = true;
                syncUpdate();
            }

            updater.setBinaryStream(columnIndex, x, length);
        } else {
            inserter.setBinaryStream(columnIndex, x, length);
        }
    }

    /** 
     * JDBC 2.0
     *  
     * Update a column with a binary stream value.
     *
     * The updateXXX() methods are used to update column values in the
     * current row, or the insert row.  The updateXXX() methods do not 
     * update the underlying database, instead the updateRow() or insertRow()
     * methods are called to update the database.
     *
     * @param columnName the name of the column
     * @param x the new column value
     * @param length of the stream
     * @exception SQLException if a database-access error occurs
     */
    public synchronized void updateBinaryStream(String columnName, 
                                                java.io.InputStream x, 
                                                int length)
                                         throws SQLException
    {
        updateBinaryStream(findColumn(columnName), x, length);
    }
    
     /**
     * @see ResultSet#updateBlob(int, Blob)
     */
    public void updateBlob(int arg0, java.sql.Blob arg1)
                    throws SQLException
    {
        throw new NotImplemented();
    }

    /**
     * @see ResultSet#updateBlob(String, Blob)
     */
    public void updateBlob(String arg0, java.sql.Blob arg1)
                    throws SQLException
    {
        throw new NotImplemented();
    }

    /**
     * JDBC 2.0
     * 
     * Update a column with a boolean value.
     *
     * The updateXXX() methods are used to update column values in the
     * current row, or the insert row.  The updateXXX() methods do not 
     * update the underlying database, instead the updateRow() or insertRow()
     * methods are called to update the database.
     *
     * @param columnIndex the first column is 1, the second is 2, ...
     * @param x the new column value
     * @exception SQLException if a database-access error occurs
     */
    public synchronized void updateBoolean(int columnIndex, boolean x)
                                    throws SQLException
    {

        if (!onInsertRow) {

            if (!doingUpdates) {
                doingUpdates = true;
                syncUpdate();
            }

            updater.setBoolean(columnIndex, x);
        } else {
            inserter.setBoolean(columnIndex, x);
        }
    }

    /**
     * JDBC 2.0
     *  
     * Update a column with a boolean value.
     *
     * The updateXXX() methods are used to update column values in the
     * current row, or the insert row.  The updateXXX() methods do not 
     * update the underlying database, instead the updateRow() or insertRow()
     * methods are called to update the database.
     *
     * @param columnName the name of the column
     * @param x the new column value
     * @exception SQLException if a database-access error occurs
     */
    public synchronized void updateBoolean(String columnName, boolean x)
                                    throws SQLException
    {
        updateBoolean(findColumn(columnName), x);
    }

    /**
     * JDBC 2.0
     *   
     * Update a column with a byte value.
     *
     * The updateXXX() methods are used to update column values in the
     * current row, or the insert row.  The updateXXX() methods do not 
     * update the underlying database, instead the updateRow() or insertRow()
     * methods are called to update the database.
     *
     * @param columnIndex the first column is 1, the second is 2, ...
     * @param x the new column value
     * @exception SQLException if a database-access error occurs
     */
    public synchronized void updateByte(int columnIndex, byte x)
                                 throws SQLException
    {

        if (!onInsertRow) {

            if (!doingUpdates) {
                doingUpdates = true;
                syncUpdate();
            }

            updater.setByte(columnIndex, x);
        } else {
            inserter.setByte(columnIndex, x);
        }
    }

    /**
     * JDBC 2.0
     *  
     * Update a column with a byte value.
     *
     * The updateXXX() methods are used to update column values in the
     * current row, or the insert row.  The updateXXX() methods do not 
     * update the underlying database, instead the updateRow() or insertRow()
     * methods are called to update the database.
     *
     * @param columnName the name of the column
     * @param x the new column value
     * @exception SQLException if a database-access error occurs
     */
    public synchronized void updateByte(String columnName, byte x)
                                 throws SQLException
    {
        updateByte(findColumn(columnName), x);
    }

    /**
     * JDBC 2.0
     *  
     * Update a column with a byte array value.
     *
     * The updateXXX() methods are used to update column values in the
     * current row, or the insert row.  The updateXXX() methods do not 
     * update the underlying database, instead the updateRow() or insertRow()
     * methods are called to update the database.
     *
     * @param columnIndex the first column is 1, the second is 2, ...
     * @param x the new column value
     * @exception SQLException if a database-access error occurs
     */
    public synchronized void updateBytes(int columnIndex, byte[] x)
                                  throws SQLException
    {

        if (!onInsertRow) {

            if (!doingUpdates) {
                doingUpdates = true;
                syncUpdate();
            }

            updater.setBytes(columnIndex, x);
        } else {
            inserter.setBytes(columnIndex, x);
        }
    }

    /**
     * JDBC 2.0
     *  
     * Update a column with a byte array value.
     *
     * The updateXXX() methods are used to update column values in the
     * current row, or the insert row.  The updateXXX() methods do not 
     * update the underlying database, instead the updateRow() or insertRow()
     * methods are called to update the database.
     *
     * @param columnName the name of the column
     * @param x the new column value
     * @exception SQLException if a database-access error occurs
     */
    public synchronized void updateBytes(String columnName, byte[] x)
                                  throws SQLException
    {
        updateBytes(findColumn(columnName), x);
    }

    /**
     * JDBC 2.0
     *  
     * Update a column with a character stream value.
     *
     * The updateXXX() methods are used to update column values in the
     * current row, or the insert row.  The updateXXX() methods do not 
     * update the underlying database, instead the updateRow() or insertRow()
     * methods are called to update the database.
     *
     * @param columnIndex the first column is 1, the second is 2, ...
     * @param x the new column value
     * @param length the length of the stream
     * @exception SQLException if a database-access error occurs
     */
    public synchronized void updateCharacterStream(int columnIndex, 
                                                   java.io.Reader x, 
                                                   int length)
                                            throws SQLException
    {

        if (!onInsertRow) {

            if (!doingUpdates) {
                doingUpdates = true;
                syncUpdate();
            }

            updater.setCharacterStream(columnIndex, x, length);
        } else {
            inserter.setCharacterStream(columnIndex, x, length);
        }
    }

    /**
     * JDBC 2.0
     *  
     * Update a column with a character stream value.
     *
     * The updateXXX() methods are used to update column values in the
     * current row, or the insert row.  The updateXXX() methods do not 
     * update the underlying database, instead the updateRow() or insertRow()
     * methods are called to update the database.
     *
     * @param columnName the name of the column
     * @param x the new column value
     * @param length of the stream
     * @exception SQLException if a database-access error occurs
     */
    public synchronized void updateCharacterStream(String columnName, 
                                                   java.io.Reader reader, 
                                                   int length)
                                            throws SQLException
    {
        updateCharacterStream(findColumn(columnName), reader, length);
    }

    /**
     * JDBC 2.0
     *  
     * Update a column with a Date value.
     *
     * The updateXXX() methods are used to update column values in the
     * current row, or the insert row.  The updateXXX() methods do not 
     * update the underlying database, instead the updateRow() or insertRow()
     * methods are called to update the database.
     *
     * @param columnIndex the first column is 1, the second is 2, ...
     * @param x the new column value
     * @exception SQLException if a database-access error occurs
     */
    public synchronized void updateDate(int columnIndex, java.sql.Date x)
                                 throws SQLException
    {

        if (!onInsertRow) {

            if (!doingUpdates) {
                doingUpdates = true;
                syncUpdate();
            }

            updater.setDate(columnIndex, x);
        } else {
            inserter.setDate(columnIndex, x);
        }
    }

    /**
     * JDBC 2.0
     *  
     * Update a column with a Date value.
     *
     * The updateXXX() methods are used to update column values in the
     * current row, or the insert row.  The updateXXX() methods do not 
     * update the underlying database, instead the updateRow() or insertRow()
     * methods are called to update the database.
     *
     * @param columnName the name of the column
     * @param x the new column value
     * @exception SQLException if a database-access error occurs
     */
    public synchronized void updateDate(String columnName, java.sql.Date x)
                                 throws SQLException
    {
        updateDate(findColumn(columnName), x);
    }

    /**
     * JDBC 2.0
     *  
     * Update a column with a Double value.
     *
     * The updateXXX() methods are used to update column values in the
     * current row, or the insert row.  The updateXXX() methods do not 
     * update the underlying database, instead the updateRow() or insertRow()
     * methods are called to update the database.
     *
     * @param columnIndex the first column is 1, the second is 2, ...
     * @param x the new column value
     * @exception SQLException if a database-access error occurs
     */
    public synchronized void updateDouble(int columnIndex, double x)
                                   throws SQLException
    {

        if (!onInsertRow) {

            if (!doingUpdates) {
                doingUpdates = true;
                syncUpdate();
            }

            updater.setDouble(columnIndex, x);
        } else {
            inserter.setDouble(columnIndex, x);
        }
    }

    /**
     * JDBC 2.0
     *  
     * Update a column with a double value.
     *
     * The updateXXX() methods are used to update column values in the
     * current row, or the insert row.  The updateXXX() methods do not 
     * update the underlying database, instead the updateRow() or insertRow()
     * methods are called to update the database.
     *
     * @param columnName the name of the column
     * @param x the new column value
     * @exception SQLException if a database-access error occurs
     */
    public synchronized void updateDouble(String columnName, double x)
                                   throws SQLException
    {
        updateDouble(findColumn(columnName), x);
    }

    /**
     * JDBC 2.0
     *  
     * Update a column with a float value.
     *
     * The updateXXX() methods are used to update column values in the
     * current row, or the insert row.  The updateXXX() methods do not 
     * update the underlying database, instead the updateRow() or insertRow()
     * methods are called to update the database.
     *
     * @param columnIndex the first column is 1, the second is 2, ...
     * @param x the new column value
     * @exception SQLException if a database-access error occurs
     */
    public synchronized void updateFloat(int columnIndex, float x)
                                  throws SQLException
    {

        if (!onInsertRow) {

            if (!doingUpdates) {
                doingUpdates = true;
                syncUpdate();
            }

            updater.setFloat(columnIndex, x);
        } else {
            inserter.setFloat(columnIndex, x);
        }
    }

    /**
     * JDBC 2.0
     *  
     * Update a column with a float value.
     *
     * The updateXXX() methods are used to update column values in the
     * current row, or the insert row.  The updateXXX() methods do not 
     * update the underlying database, instead the updateRow() or insertRow()
     * methods are called to update the database.
     *
     * @param columnName the name of the column
     * @param x the new column value
     * @exception SQLException if a database-access error occurs
     */
    public synchronized void updateFloat(String columnName, float x)
                                  throws SQLException
    {
        updateFloat(findColumn(columnName), x);
    }

    /**
     * JDBC 2.0
     *   
     * Update a column with an integer value.
     *
     * The updateXXX() methods are used to update column values in the
     * current row, or the insert row.  The updateXXX() methods do not 
     * update the underlying database, instead the updateRow() or insertRow()
     * methods are called to update the database.
     *
     * @param columnIndex the first column is 1, the second is 2, ...
     * @param x the new column value
     * @exception SQLException if a database-access error occurs
     */
    public synchronized void updateInt(int columnIndex, int x)
                                throws SQLException
    {

        if (!onInsertRow) {

            if (!doingUpdates) {
                doingUpdates = true;
                syncUpdate();
            }

            updater.setInt(columnIndex, x);
        } else {
            inserter.setInt(columnIndex, x);
        }
    }

    /**
     * JDBC 2.0
     *  
     * Update a column with an integer value.
     *
     * The updateXXX() methods are used to update column values in the
     * current row, or the insert row.  The updateXXX() methods do not 
     * update the underlying database, instead the updateRow() or insertRow()
     * methods are called to update the database.
     *
     * @param columnName the name of the column
     * @param x the new column value
     * @exception SQLException if a database-access error occurs
     */
    public synchronized void updateInt(String columnName, int x)
                                throws SQLException
    {
        updateInt(findColumn(columnName), x);
    }

    /**
     * JDBC 2.0
     *   
     * Update a column with a long value.
     *
     * The updateXXX() methods are used to update column values in the
     * current row, or the insert row.  The updateXXX() methods do not 
     * update the underlying database, instead the updateRow() or insertRow()
     * methods are called to update the database.
     *
     * @param columnIndex the first column is 1, the second is 2, ...
     * @param x the new column value
     * @exception SQLException if a database-access error occurs
     */
    public synchronized void updateLong(int columnIndex, long x)
                                 throws SQLException
    {

        if (!onInsertRow) {

            if (!doingUpdates) {
                doingUpdates = true;
                syncUpdate();
            }

            updater.setLong(columnIndex, x);
        } else {
            inserter.setLong(columnIndex, x);
        }
    }

    /**
     * JDBC 2.0
     *  
     * Update a column with a long value.
     *
     * The updateXXX() methods are used to update column values in the
     * current row, or the insert row.  The updateXXX() methods do not 
     * update the underlying database, instead the updateRow() or insertRow()
     * methods are called to update the database.
     *
     * @param columnName the name of the column
     * @param x the new column value
     * @exception SQLException if a database-access error occurs
     */
    public synchronized void updateLong(String columnName, long x)
                                 throws SQLException
    {
        updateLong(findColumn(columnName), x);
    }

    /**
     * JDBC 2.0
     * 
     * Give a nullable column a null value.
     * 
     * The updateXXX() methods are used to update column values in the
     * current row, or the insert row.  The updateXXX() methods do not 
     * update the underlying database, instead the updateRow() or insertRow()
     * methods are called to update the database.
     *
     * @param columnIndex the first column is 1, the second is 2, ...
     * @exception SQLException if a database-access error occurs
     */
    public synchronized void updateNull(int columnIndex)
                                 throws SQLException
    {

        if (!onInsertRow) {

            if (!doingUpdates) {
                doingUpdates = true;
                syncUpdate();
            }

            updater.setNull(columnIndex, 0);
        } else {
            inserter.setNull(columnIndex, 0);
        }
    }

    /**
     * JDBC 2.0
     *  
     * Update a column with a null value.
     *
     * The updateXXX() methods are used to update column values in the
     * current row, or the insert row.  The updateXXX() methods do not 
     * update the underlying database, instead the updateRow() or insertRow()
     * methods are called to update the database.
     *
     * @param columnName the name of the column
     * @exception SQLException if a database-access error occurs
     */
    public synchronized void updateNull(String columnName)
                                 throws SQLException
    {
        updateNull(findColumn(columnName));
    }

    /**
     * JDBC 2.0
     *  
     * Update a column with an Object value.
     *
     * The updateXXX() methods are used to update column values in the
     * current row, or the insert row.  The updateXXX() methods do not 
     * update the underlying database, instead the updateRow() or insertRow()
     * methods are called to update the database.
     *
     * @param columnIndex the first column is 1, the second is 2, ...
     * @param x the new column value
     * @param scale For java.sql.Types.DECIMAL or java.sql.Types.NUMERIC types
     *  this is the number of digits after the decimal.  For all other
     *  types this value will be ignored.
     * @exception SQLException if a database-access error occurs
     */
    public synchronized void updateObject(int columnIndex, Object x, int scale)
                                   throws SQLException
    {

        if (!onInsertRow) {

            if (!doingUpdates) {
                doingUpdates = true;
                syncUpdate();
            }

            updater.setObject(columnIndex, x);
        } else {
            inserter.setObject(columnIndex, x);
        }
    }

    /**
     * JDBC 2.0
     *  
     * Update a column with an Object value.
     *
     * The updateXXX() methods are used to update column values in the
     * current row, or the insert row.  The updateXXX() methods do not 
     * update the underlying database, instead the updateRow() or insertRow()
     * methods are called to update the database.
     *
     * @param columnIndex the first column is 1, the second is 2, ...
     * @param x the new column value
     * @exception SQLException if a database-access error occurs
     */
    public synchronized void updateObject(int columnIndex, Object x)
                                   throws SQLException
    {

        if (!onInsertRow) {

            if (!doingUpdates) {
                doingUpdates = true;
                syncUpdate();
            }

            updater.setObject(columnIndex, x);
        } else {
            inserter.setObject(columnIndex, x);
        }
    }

    /**
     * JDBC 2.0
     *  
     * Update a column with an Object value.
     *
     * The updateXXX() methods are used to update column values in the
     * current row, or the insert row.  The updateXXX() methods do not 
     * update the underlying database, instead the updateRow() or insertRow()
     * methods are called to update the database.
     *
     * @param columnName the name of the column
     * @param x the new column value
     * @param scale For java.sql.Types.DECIMAL or java.sql.Types.NUMERIC types
     *  this is the number of digits after the decimal.  For all other
     *  types this value will be ignored.
     * @exception SQLException if a database-access error occurs
     */
    public synchronized void updateObject(String columnName, Object x, 
                                          int scale)
                                   throws SQLException
    {
        updateObject(findColumn(columnName), x);
    }

    /**
     * JDBC 2.0
     *  
     * Update a column with an Object value.
     *
     * The updateXXX() methods are used to update column values in the
     * current row, or the insert row.  The updateXXX() methods do not 
     * update the underlying database, instead the updateRow() or insertRow()
     * methods are called to update the database.
     *
     * @param columnName the name of the column
     * @param x the new column value
     * @exception SQLException if a database-access error occurs
     */
    public synchronized void updateObject(String columnName, Object x)
                                   throws SQLException
    {
        updateObject(findColumn(columnName), x);
    }

    /**
     * JDBC 2.0
     *
     * Update the underlying database with the new contents of the
     * current row.  Cannot be called when on the insert row.
     *
     * @exception SQLException if a database-access error occurs, or
     * if called when on the insert row
     */
    public synchronized void updateRow()
                                throws SQLException
    {

        if (!isUpdatable) {
            throw new NotUpdatable();
        }

        if (doingUpdates) {
            updater.executeUpdate();

            //int num_fields = Fields.length;
            //for (int i = 0; i < num_fields; i++) {
            //   if (_Updater.isNull(i)) {
            //      System.out.println("isNull(" + i + ") = true");
            //      This_Row[i] = null;
            //   }
            //   else {
            //      System.out.println("_Updater.getBytes(i) = " + new String(_Updater.getBytes(i)));
            //
            //      This_Row[i] = _Updater.getBytes(i);
            //   }
            //}
            refreshRow();
            doingUpdates = false;
        }

        //
        // fixes calling updateRow() and then doing more
        // updates on same row...
        syncUpdate();
    }

    /**
     * JDBC 2.0
     *   
     * Update a column with a short value.
     *
     * The updateXXX() methods are used to update column values in the
     * current row, or the insert row.  The updateXXX() methods do not 
     * update the underlying database, instead the updateRow() or insertRow()
     * methods are called to update the database.
     *
     * @param columnIndex the first column is 1, the second is 2, ...
     * @param x the new column value
     * @exception SQLException if a database-access error occurs
     */
    public synchronized void updateShort(int columnIndex, short x)
                                  throws SQLException
    {

        if (!onInsertRow) {

            if (!doingUpdates) {
                doingUpdates = true;
                syncUpdate();
            }

            updater.setShort(columnIndex, x);
        } else {
            inserter.setShort(columnIndex, x);
        }
    }

    /**
     * JDBC 2.0
     *  
     * Update a column with a short value.
     *
     * The updateXXX() methods are used to update column values in the
     * current row, or the insert row.  The updateXXX() methods do not 
     * update the underlying database, instead the updateRow() or insertRow()
     * methods are called to update the database.
     *
     * @param columnName the name of the column
     * @param x the new column value
     * @exception SQLException if a database-access error occurs
     */
    public synchronized void updateShort(String columnName, short x)
                                  throws SQLException
    {
        updateShort(findColumn(columnName), x);
    }

    /**
     * JDBC 2.0
     *  
     * Update a column with a String value.
     *
     * The updateXXX() methods are used to update column values in the
     * current row, or the insert row.  The updateXXX() methods do not 
     * update the underlying database, instead the updateRow() or insertRow()
     * methods are called to update the database.
     *
     * @param columnIndex the first column is 1, the second is 2, ...
     * @param x the new column value
     * @exception SQLException if a database-access error occurs
     */
    public synchronized void updateString(int columnIndex, String x)
                                   throws SQLException
    {

        if (!onInsertRow) {

            if (!doingUpdates) {
                doingUpdates = true;
                syncUpdate();
            }

            updater.setString(columnIndex, x);
        } else {
            inserter.setString(columnIndex, x);
        }
    }

    /**
     * JDBC 2.0
     *  
     * Update a column with a String value.
     *
     * The updateXXX() methods are used to update column values in the
     * current row, or the insert row.  The updateXXX() methods do not 
     * update the underlying database, instead the updateRow() or insertRow()
     * methods are called to update the database.
     *
     * @param columnName the name of the column
     * @param x the new column value
     * @exception SQLException if a database-access error occurs
     */
    public synchronized void updateString(String columnName, String x)
                                   throws SQLException
    {
        updateString(findColumn(columnName), x);
    }

    /**
     * JDBC 2.0
     *  
     * Update a column with a Time value.
     *
     * The updateXXX() methods are used to update column values in the
     * current row, or the insert row.  The updateXXX() methods do not 
     * update the underlying database, instead the updateRow() or insertRow()
     * methods are called to update the database.
     *
     * @param columnIndex the first column is 1, the second is 2, ...
     * @param x the new column value
     * @exception SQLException if a database-access error occurs
     */
    public synchronized void updateTime(int columnIndex, java.sql.Time x)
                                 throws SQLException
    {

        if (!onInsertRow) {

            if (!doingUpdates) {
                doingUpdates = true;
                syncUpdate();
            }

            updater.setTime(columnIndex, x);
        } else {
            inserter.setTime(columnIndex, x);
        }
    }

    /**
     * JDBC 2.0
     *  
     * Update a column with a Time value.
     *
     * The updateXXX() methods are used to update column values in the
     * current row, or the insert row.  The updateXXX() methods do not 
     * update the underlying database, instead the updateRow() or insertRow()
     * methods are called to update the database.
     *
     * @param columnName the name of the column
     * @param x the new column value
     * @exception SQLException if a database-access error occurs
     */
    public synchronized void updateTime(String columnName, java.sql.Time x)
                                 throws SQLException
    {
        updateTime(findColumn(columnName), x);
    }

    /**
     * JDBC 2.0
     *  
     * Update a column with a Timestamp value.
     *
     * The updateXXX() methods are used to update column values in the
     * current row, or the insert row.  The updateXXX() methods do not 
     * update the underlying database, instead the updateRow() or insertRow()
     * methods are called to update the database.
     *
     * @param columnIndex the first column is 1, the second is 2, ...
     * @param x the new column value
     * @exception SQLException if a database-access error occurs
     */
    public synchronized void updateTimestamp(int columnIndex, 
                                             java.sql.Timestamp x)
                                      throws SQLException
    {

        if (!onInsertRow) {

            if (!doingUpdates) {
                doingUpdates = true;
                syncUpdate();
            }

            updater.setTimestamp(columnIndex, x);
        } else {
            inserter.setTimestamp(columnIndex, x);
        }
    }

    /**
     * JDBC 2.0
     *  
     * Update a column with a Timestamp value.
     *
     * The updateXXX() methods are used to update column values in the
     * current row, or the insert row.  The updateXXX() methods do not 
     * update the underlying database, instead the updateRow() or insertRow()
     * methods are called to update the database.
     *
     * @param columnName the name of the column
     * @param x the new column value
     * @exception SQLException if a database-access error occurs
     */
    public synchronized void updateTimestamp(String columnName, 
                                             java.sql.Timestamp x)
                                      throws SQLException
    {
        updateTimestamp(findColumn(columnName), x);
    }

    /**
     * A column may have the value of SQL NULL; wasNull() reports whether
     * the last column read had this special value.  Note that you must
     * first call getXXX on a column to try to read its value and then
     * call wasNull() to find if the value was SQL NULL
     *
     * @return true if the last column read was SQL NULL
     * @exception java.sql.SQLException if a database access error occurred
     */
    public synchronized boolean wasNull()
                                 throws java.sql.SQLException
    {

        return super.wasNull();
    }

    protected void setResultSetConcurrency(int concurrencyFlag)
    {
        super.setResultSetConcurrency(concurrencyFlag);

        //
        // FIXME: Issue warning when asked for updateable result set, but result set is not
        // updatable
        //
        if (concurrencyFlag == CONCUR_UPDATABLE && !isUpdateable()) {

            java.sql.SQLWarning warning = new java.sql.SQLWarning(
                                                  NotUpdatable.NOT_UPDATEABLE_MESSAGE);
        }
    }

    /**
     * Figure out whether or not this ResultSet is updateable,
     * and if so, generate the PreparedStatements to support updates.
     */
    protected void generateStatements()
                               throws SQLException
    {

        if (!isUpdatable) {
            throw new NotUpdatable();
        }

        boolean useQuotedIdentifiers = connection.supportsQuotedIdentifiers();
        String  quotedId           = getQuotedIdChar();
        String  TableName          = fields[0].getTableName();
        primaryKeyIndicies = new ArrayList();

        StringBuffer FieldValues        = new StringBuffer();
        StringBuffer KeyValues          = new StringBuffer();
        StringBuffer ColumnNames        = new StringBuffer();
        StringBuffer InsertPlaceHolders = new StringBuffer();
        boolean      first_time         = true;
        boolean      keys_first_time    = true;

        for (int i = 0; i < fields.length; i++) {

            if (fields[i].isPrimaryKey()) {
                primaryKeyIndicies.add(new Integer(i));

                if (!keys_first_time) {
                    KeyValues.append(" AND ");
                } else {
                    keys_first_time = false;
                }

                if (useQuotedIdentifiers) {
                    KeyValues.append(quotedId);
                }

                KeyValues.append(fields[i].getName());

                if (useQuotedIdentifiers) {
                    KeyValues.append(quotedId);
                }

                KeyValues.append("=?");
            }

            if (first_time) {
                first_time = false;
                FieldValues.append("SET ");
            } else {
                FieldValues.append(",");
                ColumnNames.append(",");
                InsertPlaceHolders.append(",");
            }

            InsertPlaceHolders.append("?");

            if (useQuotedIdentifiers) {
                ColumnNames.append(quotedId);
            }

            ColumnNames.append(fields[i].getName());

            if (useQuotedIdentifiers) {
                ColumnNames.append(quotedId);
            }

            if (useQuotedIdentifiers) {
                FieldValues.append(quotedId);
            }

            FieldValues.append(fields[i].getName());

            if (useQuotedIdentifiers) {
                FieldValues.append(quotedId);
            }

            FieldValues.append("=?");
        }

        String quotedIdStr = useQuotedIdentifiers ? quotedId : "";
        updateSQL  = "UPDATE " + quotedIdStr + TableName + quotedIdStr + 
                      " " + FieldValues.toString() + " WHERE " + 
                      KeyValues.toString();
        insertSQL  = "INSERT INTO " + quotedIdStr + TableName + 
                      quotedIdStr + " (" + ColumnNames.toString() + 
                      ") VALUES (" + InsertPlaceHolders.toString() + ")";
        refreshSQL = "SELECT " + ColumnNames.toString() + " FROM " + 
                      quotedIdStr + TableName + quotedIdStr + " WHERE " + 
                      KeyValues.toString();
        deleteSQL  = "DELETE FROM " + quotedIdStr + TableName + 
                      quotedIdStr + " WHERE " + KeyValues.toString();
    }

    /**
     * Allows Statements to determine the type of result set they ended
     * up building.
     */
    int getResultSetType()
    {

        return 0;
    }

    /**
     * Is this ResultSet updateable?
     */
    boolean isUpdateable()
    {

        if (fields.length > 0) {

            String TableName = fields[0].getTableName();

            //
            // References only one table?
            //
            for (int i = 1; i < fields.length; i++) {

                if (TableName == null || 
                    !fields[i].getTableName().equals(TableName)) {

                    return false;
                }
            }

            if (TableName == null || TableName.length() == 0) {

                return false;
            }
        } else {

            return false;
        }

        //
        // Contains the primary key?
        //
        boolean has_primary_key = false;

        for (int i = 0; i < fields.length; i++) {

            if (fields[i].isPrimaryKey()) {
                has_primary_key = true;

                break;
            }
        }

        if (!has_primary_key) {

            return false;
        }

        return true;
    }

    /**
     * Reset UPDATE prepared statement to value in current row.
     * 
     * This_Row MUST point to current, valid row.
     */
    void syncUpdate()
             throws SQLException
    {

        if (updater == null) {

            if (updateSQL == null) {
                generateStatements();
            }

            updater = (com.mysql.jdbc.PreparedStatement)connection.prepareStatement(
                               updateSQL);
        }

        int num_fields = fields.length;
        updater.clearParameters();

        for (int i = 0; i < num_fields; i++) {

            if (thisRow[i] != null) {
                updater.setBytes(i + 1, thisRow[i]);
            } else {
                updater.setNull(i + 1, 0);
            }
        }

        int num_keys = primaryKeyIndicies.size();

        if (num_keys == 1) {

            int index = ((Integer)primaryKeyIndicies.get(0)).intValue();
            updater.setBytes(num_fields + 1, 
                              thisRow[((Integer)primaryKeyIndicies.get(0)).intValue()]);
        } else {

            for (int i = 0; i < num_keys; i++) {

                byte[] currentVal = thisRow[((Integer)primaryKeyIndicies.get(
                                                      i)).intValue()];

                if (currentVal != null) {
                    updater.setBytes(num_fields + i + 1, currentVal);
                } else {
                    updater.setNull(num_fields + i + 1, 0);
                }
            }
        }
    }

    private synchronized String getQuotedIdChar()
                                         throws SQLException
    {

        if (quotedIdChar == null) {

            java.sql.DatabaseMetaData dbmd = connection.getMetaData();
            quotedIdChar = dbmd.getIdentifierQuoteString();
        }

        return quotedIdChar;
    }

    private void resetInserter()
                        throws SQLException
    {
        inserter.clearParameters();

        for (int i = 0; i < fields.length; i++) {
            inserter.setNull(i + 1, 0);
        }
    }

    private void resetUpdater()
                       throws SQLException
    {
        updater.clearParameters();

        for (int i = 0; i < fields.length; i++) {
            updater.setNull(i + 1, 0);
        }
    }
}