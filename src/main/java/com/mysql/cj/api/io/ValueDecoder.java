/*
  Copyright (c) 2015, Oracle and/or its affiliates. All rights reserved.

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

package com.mysql.cj.api.io;

/**
 * A value decoder is responsible for interpreting a byte array as a value. The type of the value is encoded in the method call. After decoding, the value
 * decoder passes an <i>intermediate representation</i> of the value to a {@link ValueFactory} for result value creation.
 * 
 * @since 6.0
 */
public interface ValueDecoder {
    <T> T decodeDate(byte[] bytes, int offset, int length, ValueFactory<T> vf);

    <T> T decodeTime(byte[] bytes, int offset, int length, ValueFactory<T> vf);

    <T> T decodeTimestamp(byte[] bytes, int offset, int length, ValueFactory<T> vf);

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

    <T> T decodeByteArray(byte[] bytes, int offset, int length, ValueFactory<T> vf);

    <T> T decodeBit(byte[] bytes, int offset, int length, ValueFactory<T> vf);
}
