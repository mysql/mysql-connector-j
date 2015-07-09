/*
  Copyright (c) 2015, Oracle and/or its affiliates. All rights reserved.

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

package com.mysql.cj.mysqlx.result;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import com.google.protobuf.ByteString;
import com.google.protobuf.CodedInputStream;

import static com.mysql.cj.mysqlx.protobuf.MysqlxSql.Row;

import com.mysql.cj.api.io.ValueFactory;
import com.mysql.cj.jdbc.Field;
import com.mysql.cj.mysqla.MysqlaConstants;
import com.mysql.cj.core.exceptions.WrongArgumentException;

/**
 * TODO: write unit tests once server interface stabilizes
 * @todo
 */
public class MysqlxRow implements com.mysql.cj.api.result.Row {
    private ArrayList<Field> metadata;
    private Row rowMessage;
    private boolean wasNull = false;

    @FunctionalInterface
    private static interface DecoderFunction {
        <T> T apply(CodedInputStream inputStream, ValueFactory<T> vf) throws IOException;
    }

    // TODO: ValueDecoder style, no protobuf messages anymore
    private static class MysqlxDecoder {
        private static MysqlxDecoder instance = new MysqlxDecoder();

        public static final Map<Integer, DecoderFunction> MYSQL_TYPE_TO_DECODER_FUNCTION;

        static {
            Map<Integer, DecoderFunction> mysqlTypeToDecoderFunction = new HashMap<>();

            // TODO: implement remaining types when server is ready
            //mysqlTypeToDecoderFunction.put(MysqlaConstants.FIELD_TYPE_BIT, instance::decodeBit);
            mysqlTypeToDecoderFunction.put(MysqlaConstants.FIELD_TYPE_DATE, instance::decodeDate);
            mysqlTypeToDecoderFunction.put(MysqlaConstants.FIELD_TYPE_DECIMAL, instance::decodeDecimal);
            mysqlTypeToDecoderFunction.put(MysqlaConstants.FIELD_TYPE_DOUBLE, instance::decodeDouble);
            mysqlTypeToDecoderFunction.put(MysqlaConstants.FIELD_TYPE_ENUM, instance::decodeString);
            mysqlTypeToDecoderFunction.put(MysqlaConstants.FIELD_TYPE_FLOAT, instance::decodeFloat);
            // mysqlTypeToDecoderFunction.put(MysqlaConstants.FIELD_TYPE_GEOMETRY, instance::decodeGeometry);
            // TODO: do we need to really do anything special with JSON? just return correct stuff with getObject() I guess
            mysqlTypeToDecoderFunction.put(MysqlaConstants.FIELD_TYPE_JSON, instance::decodeString);
            mysqlTypeToDecoderFunction.put(MysqlaConstants.FIELD_TYPE_LONG, instance::decodeLong);
            mysqlTypeToDecoderFunction.put(MysqlaConstants.FIELD_TYPE_SET, instance::decodeString);
            mysqlTypeToDecoderFunction.put(MysqlaConstants.FIELD_TYPE_TIME, instance::decodeTime);
            mysqlTypeToDecoderFunction.put(MysqlaConstants.FIELD_TYPE_TIMESTAMP, instance::decodeTimestamp);
            mysqlTypeToDecoderFunction.put(MysqlaConstants.FIELD_TYPE_VARCHAR, instance::decodeString);

            // TODO: longlong

            MYSQL_TYPE_TO_DECODER_FUNCTION = Collections.unmodifiableMap(mysqlTypeToDecoderFunction);
        }

        // TODO: vf should have the createFromString()? ARGH I can't decide which side should know the character set
        private <T> T decodeString(CodedInputStream inputStream, ValueFactory<T> vf) throws IOException {
            // c.f. Streaming_command_delegate::get_string()
            int size = inputStream.getBytesUntilLimit();
            return vf.createFromBytes(inputStream.readRawBytes(size), 0, size);
        }

        private <T> T decodeDate(CodedInputStream inputStream, ValueFactory<T> vf) throws IOException {
            // TODO: watch for changes in these
            int year = (int) inputStream.readInt64();
            int month = (int) inputStream.readInt64();
            int day = (int) inputStream.readInt64();
            return vf.createFromDate(year, month, day);
        }

        private <T> T decodeTimestamp(CodedInputStream inputStream, ValueFactory<T> vf) throws IOException {
            // TODO: watch for changes in these
            int year = (int) inputStream.readInt64();
            int month = (int) inputStream.readInt64();
            int day = (int) inputStream.readInt64();

            int hours = (int) inputStream.readInt64();
            int minutes = (int) inputStream.readInt64();
            int seconds = (int) inputStream.readInt64();

            int nanos = 1000 * (int) inputStream.readInt64();

            return vf.createFromTimestamp(year, month, day, hours, minutes, seconds, nanos);
        }

        private <T> T decodeTime(CodedInputStream inputStream, ValueFactory<T> vf) throws IOException {
            int hours = (int) inputStream.readInt64();
            int minutes = (int) inputStream.readInt64();
            int seconds = (int) inputStream.readInt64();

            int nanos = 1000 * (int) inputStream.readInt64();

            return vf.createFromTime(hours, minutes, seconds, nanos);
        }

        private <T> T decodeFloat(CodedInputStream inputStream, ValueFactory<T> vf) throws IOException {
            return vf.createFromDouble(inputStream.readFloat());
        }

        private <T> T decodeDouble(CodedInputStream inputStream, ValueFactory<T> vf) throws IOException {
            return vf.createFromDouble(inputStream.readDouble());
        }

        private <T> T decodeLong(CodedInputStream inputStream, ValueFactory<T> vf) throws IOException {
            return vf.createFromLong(inputStream.readSInt32());
        }

        private <T> T decodeDecimal(CodedInputStream inputStream, ValueFactory<T> vf) throws IOException {
            // This will be some stream of bytes encoded as BCD. we can check the # of digits to figure out the easiest way to treat it. (BigInteger vs
            // Bigdecimal, etc)
            throw new NullPointerException("TODO: implementation not finished");
        }
    }

    public MysqlxRow(ArrayList<Field> metadata, Row rowMessage) {
        this.metadata = metadata;
        this.rowMessage = rowMessage;
    }

    public <T> T getValue(int columnIndex, ValueFactory<T> vf) {
        // TODO: check BYTES for 0-length, then null
        // TODO: decode message in BYTES (type specific)
        // TODO: transform decoded message to type via valuefactory

        Field f = this.metadata.get(columnIndex);
        ByteString allBytes = this.rowMessage.getField(columnIndex);
        CodedInputStream inputStream = CodedInputStream.newInstance(allBytes.toByteArray());
        int valueSize;
        try {
            valueSize = inputStream.readRawVarint32();
            if (valueSize == 0) {
                T result = vf.createFromNull();
                this.wasNull = result == null;
                return result;
            }

            DecoderFunction decoderFunction = MysqlxDecoder.MYSQL_TYPE_TO_DECODER_FUNCTION.get(f.getMysqlType());
            if (decoderFunction != null) {
                this.wasNull = false;
                return decoderFunction.apply(inputStream, vf);
            } else {
                throw new WrongArgumentException("Unknown MySQL type constant: " + f.getMysqlType());
            }
        } catch (IOException ex) {
            // TODO: wrap properly
            throw new WrongArgumentException(ex);
        }
    }

    public boolean getNull(int columnIndex) {
        ByteString allBytes = this.rowMessage.getField(columnIndex);
        CodedInputStream inputStream = CodedInputStream.newInstance(allBytes.toByteArray());
        int valueSize;
        try {
            valueSize = inputStream.readRawVarint32();
            this.wasNull = valueSize == 0;
            return this.wasNull;
        } catch (IOException ex) {
            // TODO: wrap properly
            throw new WrongArgumentException(ex);
        }
    }

    public boolean wasNull() {
        return this.wasNull;
    }
}
