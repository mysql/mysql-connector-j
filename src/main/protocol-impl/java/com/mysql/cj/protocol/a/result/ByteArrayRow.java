/*
 * Copyright (c) 2007, 2020, Oracle and/or its affiliates.
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

package com.mysql.cj.protocol.a.result;

import com.mysql.cj.exceptions.ExceptionInterceptor;
import com.mysql.cj.protocol.ValueDecoder;
import com.mysql.cj.protocol.a.MysqlBinaryValueDecoder;
import com.mysql.cj.protocol.a.MysqlTextValueDecoder;
import com.mysql.cj.protocol.result.AbstractResultsetRow;
import com.mysql.cj.result.ValueFactory;

/**
 * A RowHolder implementation that is for cached results (a-la mysql_store_result()).
 */
public class ByteArrayRow extends AbstractResultsetRow {

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
    public boolean isBinaryEncoded() {
        return this.valueDecoder instanceof MysqlBinaryValueDecoder;
    }

    @Override
    public byte[] getBytes(int index) {
        if (getNull(index)) {
            return null;
        }
        return this.internalRowData[index];
    }

    @Override
    public void setBytes(int index, byte[] value) {
        this.internalRowData[index] = value;
    }

    @Override
    public boolean getNull(int columnIndex) {
        this.wasNull = this.internalRowData[columnIndex] == null;
        return this.wasNull;
    }

    /**
     * Implementation of getValue() based on the underlying byte array. Delegate to superclass for decoding.
     */
    @Override
    public <T> T getValue(int columnIndex, ValueFactory<T> vf) {
        byte[] columnData = this.internalRowData[columnIndex];
        int length = columnData == null ? 0 : columnData.length;
        return getValueFromBytes(columnIndex, columnData, 0, length, vf);
    }
}
