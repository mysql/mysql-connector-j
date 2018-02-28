/*
 * Copyright (c) 2015, 2018, Oracle and/or its affiliates. All rights reserved.
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

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;

import com.google.protobuf.CodedInputStream;
import com.mysql.cj.exceptions.AssertionFailedException;
import com.mysql.cj.exceptions.DataReadException;
import com.mysql.cj.protocol.ValueDecoder;
import com.mysql.cj.result.ValueFactory;

public class XProtocolDecoder implements ValueDecoder {

    public static XProtocolDecoder instance = new XProtocolDecoder();

    @Override
    public <T> T decodeDate(byte[] bytes, int offset, int length, ValueFactory<T> vf) {
        return decodeTimestamp(bytes, offset, length, vf);
    }

    @Override
    public <T> T decodeTime(byte[] bytes, int offset, int length, ValueFactory<T> vf) {
        try {
            CodedInputStream inputStream = CodedInputStream.newInstance(bytes, offset, length);
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
        } catch (IOException e) {
            throw new DataReadException(e);
        }
    }

    @Override
    public <T> T decodeTimestamp(byte[] bytes, int offset, int length, ValueFactory<T> vf) {
        try {
            CodedInputStream inputStream = CodedInputStream.newInstance(bytes, offset, length);
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
        } catch (IOException e) {
            throw new DataReadException(e);
        }
    }

    @Override
    public <T> T decodeInt1(byte[] bytes, int offset, int length, ValueFactory<T> vf) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public <T> T decodeUInt1(byte[] bytes, int offset, int length, ValueFactory<T> vf) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public <T> T decodeInt2(byte[] bytes, int offset, int length, ValueFactory<T> vf) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public <T> T decodeUInt2(byte[] bytes, int offset, int length, ValueFactory<T> vf) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public <T> T decodeInt4(byte[] bytes, int offset, int length, ValueFactory<T> vf) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public <T> T decodeUInt4(byte[] bytes, int offset, int length, ValueFactory<T> vf) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public <T> T decodeInt8(byte[] bytes, int offset, int length, ValueFactory<T> vf) {
        try {
            return vf.createFromLong(CodedInputStream.newInstance(bytes, offset, length).readSInt64());
        } catch (IOException e) {
            throw new DataReadException(e);
        }
    }

    @Override
    public <T> T decodeUInt8(byte[] bytes, int offset, int length, ValueFactory<T> vf) {
        try {
            // protobuf stores an unsigned 64bit int into a java long with the highest bit as the sign, we re-interpret it using ByteBuffer (with a prepended
            // 0-byte to avoid negative)
            BigInteger v = new BigInteger(
                    ByteBuffer.allocate(9).put((byte) 0).putLong(CodedInputStream.newInstance(bytes, offset, length).readUInt64()).array());
            return vf.createFromBigInteger(v);
        } catch (IOException e) {
            throw new DataReadException(e);
        }
    }

    @Override
    public <T> T decodeFloat(byte[] bytes, int offset, int length, ValueFactory<T> vf) {
        try {
            return vf.createFromDouble(CodedInputStream.newInstance(bytes, offset, length).readFloat());
        } catch (IOException e) {
            throw new DataReadException(e);
        }
    }

    @Override
    public <T> T decodeDouble(byte[] bytes, int offset, int length, ValueFactory<T> vf) {
        try {
            return vf.createFromDouble(CodedInputStream.newInstance(bytes, offset, length).readDouble());
        } catch (IOException e) {
            throw new DataReadException(e);
        }
    }

    @Override
    public <T> T decodeDecimal(byte[] bytes, int offset, int length, ValueFactory<T> vf) {
        try {
            CodedInputStream inputStream = CodedInputStream.newInstance(bytes, offset, length);
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
                throw AssertionFailedException
                        .shouldNotHappen("Did not read all bytes while decoding decimal. Bytes left: " + inputStream.getBytesUntilLimit());
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
        } catch (IOException e) {
            throw new DataReadException(e);
        }
    }

    @Override
    public <T> T decodeByteArray(byte[] bytes, int offset, int length, ValueFactory<T> vf) {
        try {
            CodedInputStream inputStream = CodedInputStream.newInstance(bytes, offset, length);
            // c.f. Streaming_command_delegate::get_string()
            int size = inputStream.getBytesUntilLimit();
            size--; // for null terminator
            return vf.createFromBytes(inputStream.readRawBytes(size), 0, size);
        } catch (IOException e) {
            throw new DataReadException(e);
        }
    }

    @Override
    public <T> T decodeBit(byte[] bytes, int offset, int length, ValueFactory<T> vf) {
        try {
            // protobuf stores an unsigned 64bit int into a java long with the highest bit as the sign, we re-interpret it using ByteBuffer (with a prepended
            // 0-byte to avoid negative)
            byte[] buf = ByteBuffer.allocate(Long.BYTES + 1).put((byte) 0).putLong(CodedInputStream.newInstance(bytes, offset, length).readUInt64()).array();
            return vf.createFromBit(buf, 0, Long.BYTES + 1);
        } catch (IOException e) {
            throw new DataReadException(e);
        }
    }

    @Override
    public <T> T decodeSet(byte[] bytes, int offset, int length, ValueFactory<T> vf) {
        try {
            CodedInputStream inputStream = CodedInputStream.newInstance(bytes, offset, length);
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
            byte[] buf = vals.toString().getBytes();
            return vf.createFromBytes(buf, 0, buf.length);
        } catch (IOException e) {
            throw new DataReadException(e);
        }
    }
}
