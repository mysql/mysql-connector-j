/*
 * Copyright (c) 2015, 2019, Oracle and/or its affiliates. All rights reserved.
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

import com.mysql.cj.Messages;
import com.mysql.cj.conf.PropertySet;
import com.mysql.cj.exceptions.NumberOutOfRange;
import com.mysql.cj.util.DataTypeUtil;

/**
 * A value factory for creating long values.
 */
public class LongValueFactory extends AbstractNumericValueFactory<Long> {

    public LongValueFactory(PropertySet pset) {
        super(pset);
    }

    @Override
    public Long createFromBigInteger(BigInteger i) {
        if (this.jdbcCompliantTruncationForReads
                && (i.compareTo(BigInteger.valueOf(Long.MIN_VALUE)) < 0 || i.compareTo(BigInteger.valueOf(Long.MAX_VALUE)) > 0)) {
            throw new NumberOutOfRange(Messages.getString("ResultSet.NumberOutOfRange", new Object[] { i, getTargetTypeName() }));
        }
        return i.longValue();
    }

    @Override
    public Long createFromLong(long l) {
        if (this.jdbcCompliantTruncationForReads && (l < Long.MIN_VALUE || l > Long.MAX_VALUE)) {
            throw new NumberOutOfRange(Messages.getString("ResultSet.NumberOutOfRange", new Object[] { Long.valueOf(l).toString(), getTargetTypeName() }));
        }
        return l;
    }

    @Override
    public Long createFromBigDecimal(BigDecimal d) {
        if (this.jdbcCompliantTruncationForReads
                && (d.compareTo(BigDecimal.valueOf(Long.MIN_VALUE)) < 0 || d.compareTo(BigDecimal.valueOf(Long.MAX_VALUE)) > 0)) {
            throw new NumberOutOfRange(Messages.getString("ResultSet.NumberOutOfRange", new Object[] { d, getTargetTypeName() }));
        }
        return d.longValue();
    }

    @Override
    public Long createFromDouble(double d) {
        if (this.jdbcCompliantTruncationForReads && (d < Long.MIN_VALUE || d > Long.MAX_VALUE)) {
            throw new NumberOutOfRange(Messages.getString("ResultSet.NumberOutOfRange", new Object[] { d, getTargetTypeName() }));
        }
        return (long) d;
    }

    @Override
    public Long createFromBit(byte[] bytes, int offset, int length) {
        return DataTypeUtil.bitToLong(bytes, offset, length);
    }

    @Override
    public Long createFromNull() {
        return (long) 0;
    }

    public String getTargetTypeName() {
        return Long.class.getName();
    }
}
