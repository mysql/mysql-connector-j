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

package com.mysql.cj.result;

import java.math.BigDecimal;
import java.math.BigInteger;

/**
 * A value factory for creating byte values.
 */
public class ByteValueFactory extends DefaultValueFactory<Byte> {
    @Override
    public Byte createFromBigInteger(BigInteger i) {
        return (byte) i.intValue();
    }

    @Override
    public Byte createFromLong(long l) {
        return (byte) l;
    }

    @Override
    public Byte createFromBigDecimal(BigDecimal d) {
        return (byte) d.longValue();
    }

    @Override
    public Byte createFromDouble(double d) {
        return (byte) d;
    }

    @Override
    public Byte createFromBit(byte[] bytes, int offset, int length) {
        return bytes[offset + length - 1];
    }

    @Override
    public Byte createFromNull() {
        return 0;
    }

    public String getTargetTypeName() {
        return Byte.class.getName();
    }
}
