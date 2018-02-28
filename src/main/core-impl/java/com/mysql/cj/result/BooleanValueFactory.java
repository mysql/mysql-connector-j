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

import com.mysql.cj.util.DataTypeUtil;

/**
 * A value factory for creating {@link java.lang.Boolean} values.
 */
public class BooleanValueFactory extends DefaultValueFactory<Boolean> {
    @Override
    public Boolean createFromLong(long l) {
        // Goes back to ODBC driver compatibility, and VB/Automation Languages/COM, where in Windows "-1" can mean true as well.
        return (l == -1 || l > 0);
    }

    @Override
    public Boolean createFromBigInteger(BigInteger i) {
        return i.compareTo(BigInteger.valueOf(0)) > 0 || i.compareTo(BigInteger.valueOf(-1)) == 0;
    }

    @Override
    // getBoolean() from DOUBLE, DECIMAL are required by JDBC spec....
    public Boolean createFromDouble(double d) {
        // this means that 0.1 or -1 will be TRUE
        return d > 0 || d == -1.0d;
    }

    @Override
    public Boolean createFromBigDecimal(BigDecimal d) {
        // this means that 0.1 or -1 will be TRUE
        return d.compareTo(BigDecimal.valueOf(0)) > 0 || d.compareTo(BigDecimal.valueOf(-1)) == 0;
    }

    @Override
    public Boolean createFromBit(byte[] bytes, int offset, int length) {
        return createFromLong(DataTypeUtil.bitToLong(bytes, offset, length));
    }

    @Override
    public Boolean createFromNull() {
        return false;
    }

    public String getTargetTypeName() {
        return Boolean.class.getName();
    }
}
