/*
  Copyright (c) 2002, 2016, Oracle and/or its affiliates. All rights reserved.

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

package com.mysql.cj.api.mysqla.result;

import com.mysql.cj.api.jdbc.result.ResultSetInternalMethods;
import com.mysql.cj.core.Messages;
import com.mysql.cj.core.exceptions.CJOperationNotSupportedException;
import com.mysql.cj.core.exceptions.ExceptionFactory;
import com.mysql.cj.core.result.Field;
import com.mysql.cj.jdbc.result.ResultSetImpl;
import com.mysql.cj.mysqla.result.ResultSetRow;

/**
 * This interface abstracts away how row data is accessed by the result set. It is meant to allow a static implementation (Current version), and a streaming
 * one.
 */
public interface RowData {

    /**
     * What's returned for the size of a result set when its size can not be
     * determined.
     */
    public static final int RESULT_SET_SIZE_UNKNOWN = -1;

    /**
     * Adds a row to this row data.
     * 
     * @param row
     *            the row to add
     */
    default void addRow(ResultSetRow row) {
        throw ExceptionFactory.createException(CJOperationNotSupportedException.class, Messages.getString("OperationNotSupportedException.0"));
    }

    /**
     * Moves to after last.
     * 
     */
    default void afterLast() {
        throw ExceptionFactory.createException(CJOperationNotSupportedException.class, Messages.getString("OperationNotSupportedException.0"));
    }

    /**
     * Moves to before first.
     * 
     */
    default void beforeFirst() {
        throw ExceptionFactory.createException(CJOperationNotSupportedException.class, Messages.getString("OperationNotSupportedException.0"));
    }

    /**
     * Moves to before last so next el is the last el.
     * 
     */
    default void beforeLast() {
        throw ExceptionFactory.createException(CJOperationNotSupportedException.class, Messages.getString("OperationNotSupportedException.0"));
    }

    /**
     * We're done.
     * 
     */
    void close();

    /**
     * Only works on non dynamic result sets.
     * 
     * @param index
     *            row number to get at
     * @return row data at index
     */
    default ResultSetRow getAt(int index) {
        throw ExceptionFactory.createException(CJOperationNotSupportedException.class, Messages.getString("OperationNotSupportedException.0"));
    }

    /**
     * Returns the current position in the result set as a row number.
     * 
     * @return the current row number
     */
    default int getCurrentRowNumber() {
        throw ExceptionFactory.createException(CJOperationNotSupportedException.class, Messages.getString("OperationNotSupportedException.0"));
    }

    /**
     * Returns the result set that 'owns' this RowData
     */
    ResultSetInternalMethods getOwner();

    /**
     * Returns true if another row exsists.
     * 
     * @return true if more rows
     */
    boolean hasNext();

    /**
     * Returns true if we got the last element.
     * 
     * @return true if after last row
     */
    boolean isAfterLast();

    /**
     * Returns if iteration has not occured yet.
     * 
     * @return true if before first row
     */
    boolean isBeforeFirst();

    /**
     * Returns true if the result set is dynamic.
     * 
     * This means that move back and move forward won't work because we do not
     * hold on to the records.
     * 
     * @return true if this result set is streaming from the server
     */
    default boolean isDynamic() {
        return true;
    }

    /**
     * Has no records.
     * 
     * @return true if no records
     */
    default boolean isEmpty() {
        throw ExceptionFactory.createException(CJOperationNotSupportedException.class, Messages.getString("OperationNotSupportedException.0"));
    }

    /**
     * Are we on the first row of the result set?
     * 
     * @return true if on first row
     */
    default boolean isFirst() {
        throw ExceptionFactory.createException(CJOperationNotSupportedException.class, Messages.getString("OperationNotSupportedException.0"));
    }

    /**
     * Are we on the last row of the result set?
     * 
     * @return true if on last row
     */
    default boolean isLast() {
        throw ExceptionFactory.createException(CJOperationNotSupportedException.class, Messages.getString("OperationNotSupportedException.0"));
    }

    /**
     * Moves the current position relative 'rows' from the current position.
     * 
     * @param rows
     *            the relative number of rows to move
     */
    default void moveRowRelative(int rows) {
        throw ExceptionFactory.createException(CJOperationNotSupportedException.class, Messages.getString("OperationNotSupportedException.0"));
    }

    /**
     * Returns the next row.
     * 
     * @return the next row value
     */
    ResultSetRow next();

    /**
     * Removes the row at the given index.
     * 
     * @param index
     *            the row to move to
     */
    default void removeRow(int index) {
        throw ExceptionFactory.createException(CJOperationNotSupportedException.class, Messages.getString("OperationNotSupportedException.0"));
    }

    /**
     * Moves the current position in the result set to the given row number.
     * 
     * @param rowNumber
     *            row to move to
     */
    default void setCurrentRow(int rowNumber) {
        throw ExceptionFactory.createException(CJOperationNotSupportedException.class, Messages.getString("OperationNotSupportedException.0"));
    }

    /**
     * Set the result set that 'owns' this RowData
     * 
     * @param rs
     *            the result set that 'owns' this RowData
     */
    void setOwner(ResultSetImpl rs);

    /**
     * Only works on non dynamic result sets.
     * 
     * @return the size of this row data
     */
    default int size() {
        return RESULT_SET_SIZE_UNKNOWN;
    }

    /**
     * Did this result set have no rows?
     */
    boolean wasEmpty();

    /**
     * Sometimes the driver doesn't have metadata until after
     * the statement has the result set in-hand (because it's cached),
     * so it can call this to set it after the fact.
     * 
     * @param metadata
     *            field-level metadata for the result set
     */
    void setMetadata(Field[] metadata);
}
