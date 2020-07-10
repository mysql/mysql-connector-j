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

package com.mysql.cj.protocol;

import com.mysql.cj.Messages;
import com.mysql.cj.exceptions.CJOperationNotSupportedException;
import com.mysql.cj.exceptions.ExceptionFactory;
import com.mysql.cj.result.Row;
import com.mysql.cj.result.RowList;

/**
 * This interface abstracts away how row data is accessed by the result set. It is meant to allow a static implementation (Current version), and a streaming
 * one. It extends the {@link RowList} functionality by providing row positioning, updatability and ownership.
 */
public interface ResultsetRows extends RowList, ProtocolEntity {

    /**
     * Adds a row.
     * 
     * @param row
     *            the row to add
     */
    default void addRow(Row row) {
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
     * Moves to before last.
     * 
     */
    default void beforeLast() {
        throw ExceptionFactory.createException(CJOperationNotSupportedException.class, Messages.getString("OperationNotSupportedException.0"));
    }

    /**
     * We're done.
     * 
     */
    default void close() {
    }

    /**
     * Returns the result set that 'owns' this RowData
     * 
     * @return {@link ResultsetRowsOwner}
     */
    ResultsetRowsOwner getOwner();

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
    void setOwner(ResultsetRowsOwner rs);

    /**
     * Did this result set have no rows?
     * 
     * @return true if the result set did not have rows
     */
    boolean wasEmpty();

    /**
     * Sometimes the driver doesn't have metadata until after
     * the statement has the result set in-hand (because it's cached),
     * so it can call this to set it after the fact.
     * 
     * @param columnDefinition
     *            field-level metadata for the result set
     */
    void setMetadata(ColumnDefinition columnDefinition);

    ColumnDefinition getMetadata();
}
