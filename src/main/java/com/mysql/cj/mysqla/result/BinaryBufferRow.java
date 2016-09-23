/*
  Copyright (c) 2016, Oracle and/or its affiliates. All rights reserved.

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
import com.mysql.cj.api.mysqla.io.NativeProtocol.StringLengthDataType;
import com.mysql.cj.api.mysqla.io.NativeProtocol.StringSelfDataType;
import com.mysql.cj.api.mysqla.io.PacketPayload;
import com.mysql.cj.api.mysqla.result.ColumnDefinition;
import com.mysql.cj.api.result.Row;
import com.mysql.cj.core.Messages;
import com.mysql.cj.core.exceptions.ExceptionFactory;
import com.mysql.cj.mysqla.MysqlaConstants;
import com.mysql.cj.mysqla.MysqlaUtils;

/**
 * A BufferRow implementation that holds one row packet from a server-side prepared statement (which is re-used by the driver,
 * and thus saves memory allocations), and tries when possible to avoid allocations to break out the results as individual byte[]s.
 * Rows from a server-side prepared statement are encoded differently, so we have different ways of finding where each column is, and
 * unpacking them.
 * 
 * (this isn't possible when doing things like reading floating point values).
 */
public class BinaryBufferRow extends AbstractBufferRow {

    /**
     * The home position before the is-null bitmask for server-side prepared statement result sets
     */
    private int preNullBitmaskHomePosition = 0;

    /**
     * If binary-encoded, the NULL status of each column is at the beginning of the row, so we
     */
    private boolean[] isNull;

    public BinaryBufferRow(PacketPayload buf, ColumnDefinition cd, ExceptionInterceptor exceptionInterceptor, ValueDecoder valueDecoder) {
        super(exceptionInterceptor);

        this.rowFromServer = buf;
        this.homePosition = this.rowFromServer.getPosition();
        this.preNullBitmaskHomePosition = this.homePosition;
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
                // First-time "scan"
                startingIndex = 0;
                this.lastRequestedPos = this.homePosition;
            }

            this.rowFromServer.setPosition(this.lastRequestedPos);
        } else {
            this.rowFromServer.setPosition(this.homePosition);
        }

        for (int i = startingIndex; i < index; i++) {
            if (this.isNull[i]) {
                continue;
            }

            int type = this.metadata.getFields()[i].getMysqlTypeId();

            if (type != MysqlaConstants.FIELD_TYPE_NULL) {
                int length = MysqlaUtils.getBinaryEncodedLength(this.metadata.getFields()[i].getMysqlTypeId());
                if (length == 0) {
                    this.rowFromServer.skipBytes(StringSelfDataType.STRING_LENENC);
                } else if (length == -1) {
                    throw ExceptionFactory.createException(Messages.getString("MysqlIO.97") + type + Messages.getString("MysqlIO.98") + (i + 1)
                            + Messages.getString("MysqlIO.99") + this.metadata.getFields().length + Messages.getString("MysqlIO.100"),
                            this.exceptionInterceptor);
                } else {
                    int curPosition = this.rowFromServer.getPosition();
                    this.rowFromServer.setPosition(curPosition + length);
                }
            }
        }

        this.lastRequestedIndex = index;
        this.lastRequestedPos = this.rowFromServer.getPosition();

        return this.lastRequestedPos;
    }

    @Override
    public byte[] getBytes(int index) {
        findAndSeekToOffset(index);

        if (this.getNull(index)) {
            return null;
        }

        int type = this.metadata.getFields()[index].getMysqlTypeId();

        switch (type) {
            case MysqlaConstants.FIELD_TYPE_NULL:
                return null;

            case MysqlaConstants.FIELD_TYPE_TINY:
                return this.rowFromServer.readBytes(StringLengthDataType.STRING_FIXED, 1);

            default:
                int length = MysqlaUtils.getBinaryEncodedLength(type);
                if (length == 0) {
                    return this.rowFromServer.readBytes(StringSelfDataType.STRING_LENENC);
                } else if (length == -1) {
                    throw ExceptionFactory.createException(Messages.getString("MysqlIO.97") + type + Messages.getString("MysqlIO.98") + (index + 1)
                            + Messages.getString("MysqlIO.99") + this.metadata.getFields().length + Messages.getString("MysqlIO.100"),
                            this.exceptionInterceptor);
                } else {
                    return this.rowFromServer.readBytes(StringLengthDataType.STRING_FIXED, length);
                }
        }
    }

    /**
     * Check whether a column is NULL and update the 'wasNull' status.
     */
    @Override
    public boolean getNull(int columnIndex) {
        this.wasNull = this.isNull[columnIndex];
        return this.wasNull;
    }

    @Override
    public Row setMetadata(ColumnDefinition f) {
        super.setMetadata(f);
        setupIsNullBitmask();
        return this;
    }

    /**
     * Unpacks the bitmask at the head of the row packet that tells us what
     * columns hold null values, and sets the "home" position directly after the
     * bitmask.
     */
    private void setupIsNullBitmask() {
        if (this.isNull != null) {
            return; // we've already done this
        }

        this.rowFromServer.setPosition(this.preNullBitmaskHomePosition);

        int len = this.metadata.getFields().length;
        int nullCount = (len + 9) / 8;

        byte[] nullBitMask = this.rowFromServer.readBytes(StringLengthDataType.STRING_FIXED, nullCount);

        this.homePosition = this.rowFromServer.getPosition();

        this.isNull = new boolean[len];

        int nullMaskPos = 0;
        int bit = 4; // first two bits are reserved for future use

        for (int i = 0; i < len; i++) {

            this.isNull[i] = ((nullBitMask[nullMaskPos] & bit) != 0);

            if (((bit <<= 1) & 255) == 0) {
                bit = 1; /* To next byte */

                nullMaskPos++;
            }
        }
    }

    /**
     * Implementation of getValue() based on the underlying Buffer object. Delegate to superclass for decoding.
     */
    @Override
    public <T> T getValue(int columnIndex, ValueFactory<T> vf) {
        findAndSeekToOffset(columnIndex);

        // field length is type-specific in binary-encoded results
        int type = this.metadata.getFields()[columnIndex].getMysqlTypeId();
        int length = MysqlaUtils.getBinaryEncodedLength(type);
        if (length == 0) {
            length = (int) this.rowFromServer.readInteger(IntegerDataType.INT_LENENC);
        } else if (length == -1) {
            throw ExceptionFactory.createException(Messages.getString("MysqlIO.97") + type + Messages.getString("MysqlIO.98") + (columnIndex + 1)
                    + Messages.getString("MysqlIO.99") + this.metadata.getFields().length + Messages.getString("MysqlIO.100"), this.exceptionInterceptor);
        }

        return getValueFromBytes(columnIndex, this.rowFromServer.getByteBuffer(), this.rowFromServer.getPosition(), length, vf);
    }
}
