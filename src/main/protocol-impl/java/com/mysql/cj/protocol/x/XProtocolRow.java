/*
 * Copyright (c) 2015, 2020, Oracle and/or its affiliates.
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

package com.mysql.cj.protocol.x;

import com.google.protobuf.ByteString;
import com.mysql.cj.MysqlType;
import com.mysql.cj.exceptions.DataReadException;
import com.mysql.cj.protocol.ColumnDefinition;
import com.mysql.cj.result.Field;
import com.mysql.cj.result.ValueFactory;
import com.mysql.cj.x.protobuf.MysqlxResultset.Row;

/**
 * ProtocolEntity representing an X Protocol result row.
 */
public class XProtocolRow implements com.mysql.cj.result.Row {
    private ColumnDefinition metadata;
    private Row rowMessage;
    private boolean wasNull = false;

    public XProtocolRow(Row rowMessage) {
        this.rowMessage = rowMessage;
    }

    @Override
    public com.mysql.cj.result.Row setMetadata(ColumnDefinition columnDefinition) {
        this.metadata = columnDefinition;
        return this;
    }

    public <T> T getValue(int columnIndex, ValueFactory<T> vf) {
        if (columnIndex >= this.metadata.getFields().length) {
            throw new DataReadException("Invalid column");
        }
        Field f = this.metadata.getFields()[columnIndex];
        ByteString byteString = this.rowMessage.getField(columnIndex);
        // for debugging
        //System.err.println("getValue bytes = " + com.mysql.cj.core.util.StringUtils.dumpAsHex(byteString.toByteArray(), byteString.toByteArray().length));
        //try {
        if (byteString.size() == 0) {
            T result = vf.createFromNull();
            this.wasNull = result == null;
            return result;
        }

        // TODO: implement remaining types when server is ready
        switch (f.getMysqlTypeId()) {
            case MysqlType.FIELD_TYPE_BIT:
                this.wasNull = false;
                return XProtocolDecoder.instance.decodeBit(byteString.toByteArray(), 0, byteString.size(), vf);

            case MysqlType.FIELD_TYPE_DATETIME:
                this.wasNull = false;
                // TODO scale is unavailable from X Protocol
                //return XProtocolDecoder.instance.decodeTimestamp(byteString.toByteArray(), 0, byteString.size(), f.getDecimals(), vf);
                return XProtocolDecoder.instance.decodeTimestamp(byteString.toByteArray(), 0, byteString.size(), 6, vf);

            case MysqlType.FIELD_TYPE_DOUBLE:
                this.wasNull = false;
                return XProtocolDecoder.instance.decodeDouble(byteString.toByteArray(), 0, byteString.size(), vf);

            case MysqlType.FIELD_TYPE_ENUM:
                this.wasNull = false;
                return XProtocolDecoder.instance.decodeByteArray(byteString.toByteArray(), 0, byteString.size(), f, vf);

            case MysqlType.FIELD_TYPE_FLOAT:
                this.wasNull = false;
                return XProtocolDecoder.instance.decodeFloat(byteString.toByteArray(), 0, byteString.size(), vf);

            //case MysqlType.FIELD_TYPE_GEOMETRY:
            //mysqlTypeToDecoderFunction.put(MysqlType.FIELD_TYPE_GEOMETRY, instance::decodeGeometry);
            //break;

            case MysqlType.FIELD_TYPE_JSON:
                this.wasNull = false;
                // TODO: do we need to really do anything special with JSON? just return correct stuff with getObject() I guess
                return XProtocolDecoder.instance.decodeByteArray(byteString.toByteArray(), 0, byteString.size(), f, vf);

            case MysqlType.FIELD_TYPE_LONGLONG:
                // X Protocol uses 64-bit ints for everything
                this.wasNull = false;
                if (f.isUnsigned()) {
                    return XProtocolDecoder.instance.decodeUInt8(byteString.toByteArray(), 0, byteString.size(), vf);
                }
                return XProtocolDecoder.instance.decodeInt8(byteString.toByteArray(), 0, byteString.size(), vf);

            case MysqlType.FIELD_TYPE_NEWDECIMAL:
                this.wasNull = false;
                return XProtocolDecoder.instance.decodeDecimal(byteString.toByteArray(), 0, byteString.size(), vf);

            case MysqlType.FIELD_TYPE_SET:
                this.wasNull = false;
                return XProtocolDecoder.instance.decodeSet(byteString.toByteArray(), 0, byteString.size(), f, vf);
            //return XProtocolDecoder.instance.decodeByteArray(byteString.toByteArray(), 0, byteString.size(), vf);

            case MysqlType.FIELD_TYPE_TIME:
                this.wasNull = false;
                // TODO scale is unavailable from X Protocol
                //return XProtocolDecoder.instance.decodeTime(byteString.toByteArray(), 0, byteString.size(), f.getDecimals(), vf);
                return XProtocolDecoder.instance.decodeTime(byteString.toByteArray(), 0, byteString.size(), 6, vf);

            case MysqlType.FIELD_TYPE_VARCHAR:
                this.wasNull = false;
                return XProtocolDecoder.instance.decodeByteArray(byteString.toByteArray(), 0, byteString.size(), f, vf);

            case MysqlType.FIELD_TYPE_VAR_STRING:
                this.wasNull = false;
                return XProtocolDecoder.instance.decodeByteArray(byteString.toByteArray(), 0, byteString.size(), f, vf);

            default:
                throw new DataReadException("Unknown MySQL type constant: " + f.getMysqlTypeId());
        }

        //            DecoderFunction decoderFunction = XProtocolDecoder.MYSQL_TYPE_TO_DECODER_FUNCTION.get(f.getMysqlTypeId());
        //            if (decoderFunction != null) {
        //                this.wasNull = false;
        //                return decoderFunction.apply(CodedInputStream.newInstance(byteString.toByteArray()), vf);
        //            }
        //            throw new DataReadException("Unknown MySQL type constant: " + f.getMysqlTypeId());
        //} catch (IOException ex) {
        // if reading the protobuf fields fails (CodedInputStream)
        //    throw new DataReadException(ex);
        //}
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
