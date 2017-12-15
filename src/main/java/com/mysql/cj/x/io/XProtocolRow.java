/*
 * Copyright (c) 2015, 2017, Oracle and/or its affiliates. All rights reserved.
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

package com.mysql.cj.x.io;

import java.io.IOException;
import java.util.ArrayList;

import com.google.protobuf.ByteString;
import com.google.protobuf.CodedInputStream;
import com.mysql.cj.api.io.ValueFactory;
import com.mysql.cj.api.x.io.DecoderFunction;
import com.mysql.cj.core.exceptions.DataReadException;
import com.mysql.cj.core.result.Field;
import com.mysql.cj.mysqla.MysqlaConstants;
import com.mysql.cj.x.protobuf.MysqlxResultset.Row;

// TODO: write unit tests once server interface stabilizes
public class XProtocolRow implements com.mysql.cj.api.result.Row {
    private ArrayList<Field> metadata;
    private Row rowMessage;
    private boolean wasNull = false;

    public XProtocolRow(ArrayList<Field> metadata, Row rowMessage) {
        this.metadata = metadata;
        this.rowMessage = rowMessage;
    }

    public <T> T getValue(int columnIndex, ValueFactory<T> vf) {
        if (columnIndex >= this.metadata.size()) {
            throw new DataReadException("Invalid column");
        }
        Field f = this.metadata.get(columnIndex);
        ByteString byteString = this.rowMessage.getField(columnIndex);
        // for debugging
        //System.err.println("getValue bytes = " + com.mysql.cj.core.util.StringUtils.dumpAsHex(byteString.toByteArray(), byteString.toByteArray().length));
        try {
            if (byteString.size() == 0) {
                T result = vf.createFromNull();
                this.wasNull = result == null;
                return result;
            }

            // X Protocol uses 64-bit ints for everything
            if (f.getMysqlTypeId() == MysqlaConstants.FIELD_TYPE_LONGLONG) {
                if (f.isUnsigned()) {
                    return XProtocolDecoder.instance.decodeUnsignedLong(CodedInputStream.newInstance(byteString.toByteArray()), vf);
                }
            }

            DecoderFunction decoderFunction = XProtocolDecoder.MYSQL_TYPE_TO_DECODER_FUNCTION.get(f.getMysqlTypeId());
            if (decoderFunction != null) {
                this.wasNull = false;
                return decoderFunction.apply(CodedInputStream.newInstance(byteString.toByteArray()), vf);
            }
            throw new DataReadException("Unknown MySQL type constant: " + f.getMysqlTypeId());
        } catch (IOException ex) {
            // if reading the protobuf fields fails (CodedInputStream)
            throw new DataReadException(ex);
        }
    }

    public boolean getNull(int columnIndex) {
        ByteString byteString = this.rowMessage.getField(columnIndex);
        this.wasNull = byteString.size() == 0;
        return this.wasNull;
    }

    public boolean wasNull() {
        return this.wasNull;
    }
}
