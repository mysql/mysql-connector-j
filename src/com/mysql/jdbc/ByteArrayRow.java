/*
  Copyright (c) 2007, 2015, Oracle and/or its affiliates. All rights reserved.

  The MySQL Connector/J is licensed under the terms of the GPLv2
  <http://www.gnu.org/licenses/old-licenses/gpl-2.0.html>, like most MySQL Connectors.
  There are special exceptions to the terms and conditions of the GPLv2 as it is applied to
  this software, see the FLOSS License Exception
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

import com.mysql.cj.api.exception.ExceptionInterceptor;
import com.mysql.cj.api.io.ValueDecoder;
import com.mysql.cj.api.io.ValueFactory;
import com.mysql.cj.core.io.MysqlTextValueDecoder;

/**
 * A RowHolder implementation that is for cached results (a-la mysql_store_result()).
 */
public class ByteArrayRow extends ResultSetRow {

    byte[][] internalRowData;

    public ByteArrayRow(byte[][] internalRowData, ExceptionInterceptor exceptionInterceptor, ValueDecoder valueDecoder) {
        super(exceptionInterceptor);

        this.internalRowData = internalRowData;
        this.valueDecoder = valueDecoder;
    }

    public ByteArrayRow(byte[][] internalRowData, ExceptionInterceptor exceptionInterceptor) {
        super(exceptionInterceptor);

        this.internalRowData = internalRowData;
        this.valueDecoder = new MysqlTextValueDecoder();
    }

    @Override
    public byte[] getColumnValue(int index) throws SQLException {
        // check null to set 'wasNull' status
        if (getNull(index)) {
            return null;
        }
        return this.internalRowData[index];
    }

    @Override
    public void setColumnValue(int index, byte[] value) throws SQLException {
        this.internalRowData[index] = value;
    }

    @Override
    public boolean isNull(int index) throws SQLException {
        return this.internalRowData[index] == null;
    }

    @Override
    public long length(int index) throws SQLException {
        if (this.internalRowData[index] == null) {
            return 0;
        }

        return this.internalRowData[index].length;
    }

    @Override
    public void closeOpenStreams() {
        // no-op for this type
    }

    /**
     * Implementation of getValue() based on the underlying byte array. Delegate to superclass for decoding.
     */
    public <T> T getValue(int columnIndex, ValueFactory<T> vf) throws SQLException {
        byte[] columnData = this.internalRowData[columnIndex];
        int length = columnData == null ? 0 : columnData.length;
        return getValueFromBytes(columnIndex, columnData, 0, length, vf);
    }
}
