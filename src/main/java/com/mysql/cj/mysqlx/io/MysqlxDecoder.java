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

package com.mysql.cj.mysqlx.io;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import com.google.protobuf.CodedInputStream;
import com.mysql.cj.api.io.ValueFactory;
import com.mysql.cj.core.exceptions.AssertionFailedException;
import com.mysql.cj.mysqla.MysqlaConstants;

// TODO: ValueDecoder style, no protobuf messages anymore
public class MysqlxDecoder {

    @FunctionalInterface
    public static interface DecoderFunction {
        /**
         * @todo document
         *
         * @param inputStream
         *            the <code>CodedInputStream</code> over the bytes representing this value
         * @param vf
         *            the sink for the decoded value
         * @throws IOException
         *             propagated from {@link CodedInputStream}
         * @return the value factory's output
         */
        <T> T apply(CodedInputStream inputStream, ValueFactory<T> vf) throws IOException;
    }

    public static MysqlxDecoder instance = new MysqlxDecoder();

    public static final Map<Integer, DecoderFunction> MYSQL_TYPE_TO_DECODER_FUNCTION;

    static {
        Map<Integer, DecoderFunction> mysqlTypeToDecoderFunction = new HashMap<>();

        // TODO: implement remaining types when server is ready
        mysqlTypeToDecoderFunction.put(MysqlaConstants.FIELD_TYPE_BIT, instance::decodeBit);
        mysqlTypeToDecoderFunction.put(MysqlaConstants.FIELD_TYPE_DATETIME, instance::decodeDateOrTimestamp);
        mysqlTypeToDecoderFunction.put(MysqlaConstants.FIELD_TYPE_DOUBLE, instance::decodeDouble);
        mysqlTypeToDecoderFunction.put(MysqlaConstants.FIELD_TYPE_ENUM, instance::decodeString);
        mysqlTypeToDecoderFunction.put(MysqlaConstants.FIELD_TYPE_FLOAT, instance::decodeFloat);
        // mysqlTypeToDecoderFunction.put(MysqlaConstants.FIELD_TYPE_GEOMETRY, instance::decodeGeometry);
        // TODO: do we need to really do anything special with JSON? just return correct stuff with getObject() I guess
        mysqlTypeToDecoderFunction.put(MysqlaConstants.FIELD_TYPE_JSON, instance::decodeString);
        mysqlTypeToDecoderFunction.put(MysqlaConstants.FIELD_TYPE_LONGLONG, instance::decodeSignedLong);
        mysqlTypeToDecoderFunction.put(MysqlaConstants.FIELD_TYPE_NEWDECIMAL, instance::decodeDecimal);
        mysqlTypeToDecoderFunction.put(MysqlaConstants.FIELD_TYPE_SET, instance::decodeSet);
        mysqlTypeToDecoderFunction.put(MysqlaConstants.FIELD_TYPE_TIME, instance::decodeTime);
        mysqlTypeToDecoderFunction.put(MysqlaConstants.FIELD_TYPE_VARCHAR, instance::decodeString);
        mysqlTypeToDecoderFunction.put(MysqlaConstants.FIELD_TYPE_VAR_STRING, instance::decodeString);

        MYSQL_TYPE_TO_DECODER_FUNCTION = Collections.unmodifiableMap(mysqlTypeToDecoderFunction);
    }

    public <T> T decodeBit(CodedInputStream inputStream, ValueFactory<T> vf) throws IOException {
        // protobuf stores an unsigned 64bit int into a java long with the highest bit as the sign, we re-interpret it using ByteBuffer (with a prepended
        // 0-byte to avoid negative)
        byte[] bytes = ByteBuffer.allocate(Long.BYTES + 1).put((byte) 0).putLong(inputStream.readUInt64()).array();
        return vf.createFromBit(bytes, 0, Long.BYTES + 1);
    }

    // TODO: vf should have the createFromString()? ARGH I can't decide which side should know the character set
    public <T> T decodeString(CodedInputStream inputStream, ValueFactory<T> vf) throws IOException {
        // c.f. Streaming_command_delegate::get_string()
        int size = inputStream.getBytesUntilLimit();
        size--; // for null terminator
        return vf.createFromBytes(inputStream.readRawBytes(size), 0, size);
    }

    public <T> T decodeSet(CodedInputStream inputStream, ValueFactory<T> vf) throws IOException {
        StringBuilder vals = new StringBuilder();
        while (inputStream.getBytesUntilLimit() > 0) {
            if (vals.length() > 0) {
                vals.append(",");
            }
            long valLen = inputStream.readUInt64();
            // TODO: charset
            vals.append(new String(inputStream.readRawBytes((int) valLen)));
        }
        // TODO: charset mess here
        byte[] bytes = vals.toString().getBytes();
        return vf.createFromBytes(bytes, 0, bytes.length);
    }

    public <T> T decodeDateOrTimestamp(CodedInputStream inputStream, ValueFactory<T> vf) throws IOException {
        int year = (int) inputStream.readUInt64();
        int month = (int) inputStream.readUInt64();
        int day = (int) inputStream.readUInt64();

        // do we have a time too?
        if (inputStream.getBytesUntilLimit() > 0) {
            int hours = 0;
            int minutes = 0;
            int seconds = 0;

            int nanos = 0;

            if (!inputStream.isAtEnd()) {
                hours = (int) inputStream.readInt64();
                if (!inputStream.isAtEnd()) {
                    minutes = (int) inputStream.readInt64();
                    if (!inputStream.isAtEnd()) {
                        seconds = (int) inputStream.readInt64();
                        if (!inputStream.isAtEnd()) {
                            nanos = 1000 * (int) inputStream.readInt64();
                        }
                    }
                }
            }

            return vf.createFromTimestamp(year, month, day, hours, minutes, seconds, nanos);
        }
        return vf.createFromDate(year, month, day);
    }

    public <T> T decodeTime(CodedInputStream inputStream, ValueFactory<T> vf) throws IOException {
        boolean negative = inputStream.readRawByte() > 0;
        int hours = 0;
        int minutes = 0;
        int seconds = 0;

        int nanos = 0;

        if (!inputStream.isAtEnd()) {
            hours = (int) inputStream.readInt64();
            if (!inputStream.isAtEnd()) {
                minutes = (int) inputStream.readInt64();
                if (!inputStream.isAtEnd()) {
                    seconds = (int) inputStream.readInt64();
                    if (!inputStream.isAtEnd()) {
                        nanos = 1000 * (int) inputStream.readInt64();
                    }
                }
            }
        }

        return vf.createFromTime(negative ? -1 * hours : hours, minutes, seconds, nanos);
    }

    public <T> T decodeFloat(CodedInputStream inputStream, ValueFactory<T> vf) throws IOException {
        return vf.createFromDouble(inputStream.readFloat());
    }

    public <T> T decodeDouble(CodedInputStream inputStream, ValueFactory<T> vf) throws IOException {
        return vf.createFromDouble(inputStream.readDouble());
    }

    public <T> T decodeSignedLong(CodedInputStream inputStream, ValueFactory<T> vf) throws IOException {
        return vf.createFromLong(inputStream.readSInt64());
    }

    public <T> T decodeUnsignedLong(CodedInputStream inputStream, ValueFactory<T> vf) throws IOException {
        // protobuf stores an unsigned 64bit int into a java long with the highest bit as the sign, we re-interpret it using ByteBuffer (with a prepended
        // 0-byte to avoid negative)
        BigInteger v = new BigInteger(ByteBuffer.allocate(9).put((byte) 0).putLong(inputStream.readUInt64()).array());
        return vf.createFromBigInteger(v);
    }

    public <T> T decodeDecimal(CodedInputStream inputStream, ValueFactory<T> vf) throws IOException {
        // packed BCD format (c.f. wikipedia)
        // TODO: optimization possibilities include using int/long if the digits is < X and scale = 0
        byte scale = inputStream.readRawByte();
        // we allocate an extra char for the sign
        CharBuffer unscaledString = CharBuffer.allocate(2 * inputStream.getBytesUntilLimit());
        unscaledString.position(1);
        byte sign = 0;
        // read until we encounter the sign bit
        while (true) {
            int b = 0xFF & inputStream.readRawByte();
            if ((b >> 4) > 9) {
                sign = (byte) (b >> 4);
                break;
            }
            unscaledString.append((char) ((b >> 4) + '0'));
            if ((b & 0x0f) > 9) {
                sign = (byte) (b & 0x0f);
                break;
            }
            unscaledString.append((char) ((b & 0x0f) + '0'));
        }
        if (inputStream.getBytesUntilLimit() > 0) {
            throw AssertionFailedException.shouldNotHappen("Did not read all bytes while decoding decimal. Bytes left: " + inputStream.getBytesUntilLimit());
        }
        switch (sign) {
            case 0xa:
            case 0xc:
            case 0xe:
            case 0xf:
                unscaledString.put(0, '+');
                break;
            case 0xb:
            case 0xd:
                unscaledString.put(0, '-');
                break;
        }
        // may have filled the CharBuffer or one remaining. need to remove it before toString()
        int characters = unscaledString.position();
        unscaledString.clear(); // reset position
        BigInteger unscaled = new BigInteger(unscaledString.subSequence(0, characters).toString());
        return vf.createFromBigDecimal(new BigDecimal(unscaled, scale));
    }
}
