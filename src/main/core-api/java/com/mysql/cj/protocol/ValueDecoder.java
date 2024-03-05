/*
 * Copyright (c) 2015, 2024, Oracle and/or its affiliates.
 *
 * This program is free software; you can redistribute it and/or modify it under the terms of the GNU General Public License, version 2.0, as published by
 * the Free Software Foundation.
 *
 * This program is designed to work with certain software that is licensed under separate terms, as designated in a particular file or component or in
 * included license documentation. The authors of MySQL hereby grant you an additional permission to link the program and your derivative works with the
 * separately licensed software that they have either included with the program or referenced in the documentation.
 *
 * Without limiting anything contained in the foregoing, this file, which is part of MySQL Connector/J, is also subject to the Universal FOSS Exception,
 * version 1.0, a copy of which can be found at http://oss.oracle.com/licenses/universal-foss-exception.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License, version 2.0, for more details.
 *
 * You should have received a copy of the GNU General Public License along with this program; if not, write to the Free Software Foundation, Inc.,
 * 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA
 */

package com.mysql.cj.protocol;

import com.mysql.cj.result.Field;
import com.mysql.cj.result.ValueFactory;

/**
 * A value decoder is responsible for interpreting a byte array as a value. The type of the value is encoded in the method call. After decoding, the value
 * decoder passes an <i>intermediate representation</i> of the value to a {@link ValueFactory} for result value creation.
 *
 * @since 6.0
 */
public interface ValueDecoder {

    <T> T decodeDate(byte[] bytes, int offset, int length, ValueFactory<T> vf);

    <T> T decodeTime(byte[] bytes, int offset, int length, int scale, ValueFactory<T> vf);

    <T> T decodeTimestamp(byte[] bytes, int offset, int length, int scale, ValueFactory<T> vf);

    <T> T decodeDatetime(byte[] bytes, int offset, int length, int scale, ValueFactory<T> vf);

    <T> T decodeInt1(byte[] bytes, int offset, int length, ValueFactory<T> vf);

    <T> T decodeUInt1(byte[] bytes, int offset, int length, ValueFactory<T> vf);

    <T> T decodeInt2(byte[] bytes, int offset, int length, ValueFactory<T> vf);

    <T> T decodeUInt2(byte[] bytes, int offset, int length, ValueFactory<T> vf);

    <T> T decodeInt4(byte[] bytes, int offset, int length, ValueFactory<T> vf);

    <T> T decodeUInt4(byte[] bytes, int offset, int length, ValueFactory<T> vf);

    <T> T decodeInt8(byte[] bytes, int offset, int length, ValueFactory<T> vf);

    <T> T decodeUInt8(byte[] bytes, int offset, int length, ValueFactory<T> vf);

    <T> T decodeFloat(byte[] bytes, int offset, int length, ValueFactory<T> vf);

    <T> T decodeDouble(byte[] bytes, int offset, int length, ValueFactory<T> vf);

    <T> T decodeDecimal(byte[] bytes, int offset, int length, ValueFactory<T> vf);

    <T> T decodeByteArray(byte[] bytes, int offset, int length, Field f, ValueFactory<T> vf);

    <T> T decodeBit(byte[] bytes, int offset, int length, ValueFactory<T> vf);

    <T> T decodeSet(byte[] bytes, int offset, int length, Field f, ValueFactory<T> vf);

    <T> T decodeYear(byte[] bytes, int offset, int length, ValueFactory<T> vf);

}
