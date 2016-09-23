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

package com.mysql.cj.mysqla.result;

import com.mysql.cj.api.exceptions.ExceptionInterceptor;
import com.mysql.cj.api.io.ValueDecoder;
import com.mysql.cj.api.io.ValueFactory;
import com.mysql.cj.api.mysqla.io.NativeProtocol.IntegerDataType;
import com.mysql.cj.api.mysqla.io.NativeProtocol.StringSelfDataType;
import com.mysql.cj.api.mysqla.io.PacketPayload;
import com.mysql.cj.api.mysqla.result.ColumnDefinition;
import com.mysql.cj.api.result.Row;

/**
 * A ResultSetRow implementation that holds one row packet (which is re-used by the driver, and thus saves memory allocations), and tries when possible to avoid
 * allocations to break out the results as individual byte[]s.
 * 
 * (this isn't possible when doing things like reading floating point values).
 */
public class TextBufferRow extends AbstractBufferRow {

    public TextBufferRow(PacketPayload buf, ColumnDefinition cd, ExceptionInterceptor exceptionInterceptor, ValueDecoder valueDecoder) {
        super(exceptionInterceptor);

        this.rowFromServer = buf;
        this.homePosition = this.rowFromServer.getPosition();
        this.valueDecoder = valueDecoder;

        if (cd.getFields() != null) {
            setMetadata(cd);
        }
    }

    @Override
    protected int findAndSeekToOffset(int index) {

        if (index == 0) {
            this.lastRequestedIndex = 0;
            this.lastRequestedPos = this.homePosition;
            this.rowFromServer.setPosition(this.homePosition);

            return 0;
        }

        if (index == this.lastRequestedIndex) {
            this.rowFromServer.setPosition(this.lastRequestedPos);

            return this.lastRequestedPos;
        }

        int startingIndex = 0;

        if (index > this.lastRequestedIndex) {
            if (this.lastRequestedIndex >= 0) {
                startingIndex = this.lastRequestedIndex;
            } else {
                startingIndex = 0;
            }

            this.rowFromServer.setPosition(this.lastRequestedPos);
        } else {
            this.rowFromServer.setPosition(this.homePosition);
        }

        for (int i = startingIndex; i < index; i++) {
            this.rowFromServer.skipBytes(StringSelfDataType.STRING_LENENC);
        }

        this.lastRequestedIndex = index;
        this.lastRequestedPos = this.rowFromServer.getPosition();

        return this.lastRequestedPos;
    }

    @Override
    public byte[] getBytes(int index) {
        findAndSeekToOffset(index);
        return this.rowFromServer.readBytes(StringSelfDataType.STRING_LENENC);
    }

    @Override
    public boolean getNull(int columnIndex) {
        findAndSeekToOffset(columnIndex);
        this.wasNull = this.rowFromServer.readInteger(IntegerDataType.INT_LENENC) == PacketPayload.NULL_LENGTH;
        return this.wasNull;
    }

    @Override
    public Row setMetadata(ColumnDefinition f) {
        super.setMetadata(f);
        return this;
    }

    /**
     * Implementation of getValue() based on the underlying Buffer object. Delegate to superclass for decoding.
     */
    @Override
    public <T> T getValue(int columnIndex, ValueFactory<T> vf) {
        findAndSeekToOffset(columnIndex);
        int length = (int) this.rowFromServer.readInteger(IntegerDataType.INT_LENENC);
        return getValueFromBytes(columnIndex, this.rowFromServer.getByteBuffer(), this.rowFromServer.getPosition(), length, vf);
    }
}
