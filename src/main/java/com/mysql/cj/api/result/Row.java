/*
  Copyright (c) 2015, 2016, Oracle and/or its affiliates. All rights reserved.

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

package com.mysql.cj.api.result;

import com.mysql.cj.api.io.ValueDecoder;
import com.mysql.cj.api.io.ValueFactory;
import com.mysql.cj.api.mysqla.result.ColumnDefinition;
import com.mysql.cj.core.Messages;
import com.mysql.cj.core.exceptions.CJOperationNotSupportedException;
import com.mysql.cj.core.exceptions.ExceptionFactory;
import com.mysql.cj.core.result.Field;

/**
 * @todo
 */
public interface Row {
    /**
     * Retrieve a value for the given column. This is the main facility to access values from the Row
     * involving {@link ValueDecoder} and {@link ValueFactory} chain. Metadata <i>must</i> be set via
     * Row constructor or {@link #setMetadata(Field[])} call before calling this method to allow
     * correct columnIndex boundaries check and data type recognition.
     *
     * @param columnIndex
     *            index of column to retrieve value from (0-indexed, not JDBC 1-indexed)
     * @param vf
     *            value factory used to create the return value after decoding
     * @return The return value from the value factory
     */
    <T> T getValue(int columnIndex, ValueFactory<T> vf);

    /**
     * Set metadata to enable getValue functionality.
     * 
     * @param columnDefinition
     * @return
     */
    default Row setMetadata(ColumnDefinition columnDefinition) {
        throw ExceptionFactory.createException(CJOperationNotSupportedException.class, Messages.getString("OperationNotSupportedException.0"));
    }

    /**
     * Returns the value at the given column as a byte array.
     * The bytes represent the raw values returned by the server.
     * 
     * @param columnIndex
     *            index of column (starting at 0) to return from.
     * @return the value for the given column; if the value is SQL <code>NULL</code>, the value returned is <code>null</code>
     */
    default byte[] getBytes(int columnIndex) {
        // TODO check that "if the value is SQL NULL, the value returned is null" is correctly implemented
        throw ExceptionFactory.createException(CJOperationNotSupportedException.class, Messages.getString("OperationNotSupportedException.0"));
    }

    /**
     * Sets the given byte array as a raw column value (only works currently with ByteArrayRow).
     * 
     * @param columnIndex
     *            index of the column (starting at 0) to set to.
     * @param value
     *            the (raw) value to set
     */
    default void setBytes(int columnIndex, byte[] value) {
        throw ExceptionFactory.createException(CJOperationNotSupportedException.class, Messages.getString("OperationNotSupportedException.0"));
    }

    /**
     * Check whether a column is NULL and update the 'wasNull' status.
     * 
     * @param columnIndex
     *            of the column value (starting at 0) to check.
     * 
     * @return true if the column value is NULL, false if not.
     */
    boolean getNull(int columnIndex);

    /**
     * Was the last value retrieved a NULL value?
     */
    boolean wasNull();
}
