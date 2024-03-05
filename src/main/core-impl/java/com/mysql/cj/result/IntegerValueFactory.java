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

package com.mysql.cj.result;

import java.math.BigDecimal;
import java.math.BigInteger;

import com.mysql.cj.Constants;
import com.mysql.cj.Messages;
import com.mysql.cj.conf.PropertySet;
import com.mysql.cj.exceptions.NumberOutOfRange;
import com.mysql.cj.util.DataTypeUtil;

/**
 * A value factory for creating int values.
 */
public class IntegerValueFactory extends AbstractNumericValueFactory<Integer> {

    public IntegerValueFactory(PropertySet pset) {
        super(pset);
    }

    @Override
    public Integer createFromBigInteger(BigInteger i) {
        if (this.jdbcCompliantTruncationForReads
                && (i.compareTo(Constants.BIG_INTEGER_MIN_INTEGER_VALUE) < 0 || i.compareTo(Constants.BIG_INTEGER_MAX_INTEGER_VALUE) > 0)) {
            throw new NumberOutOfRange(Messages.getString("ResultSet.NumberOutOfRange", new Object[] { i, getTargetTypeName() }));
        }
        return i.intValue();
    }

    @Override
    public Integer createFromLong(long l) {
        if (this.jdbcCompliantTruncationForReads && (l < Integer.MIN_VALUE || l > Integer.MAX_VALUE)) {
            throw new NumberOutOfRange(Messages.getString("ResultSet.NumberOutOfRange", new Object[] { Long.valueOf(l).toString(), getTargetTypeName() }));
        }
        return (int) l;
    }

    @Override
    public Integer createFromBigDecimal(BigDecimal d) {
        if (this.jdbcCompliantTruncationForReads
                && (d.compareTo(Constants.BIG_DECIMAL_MIN_INTEGER_VALUE) < 0 || d.compareTo(Constants.BIG_DECIMAL_MAX_INTEGER_VALUE) > 0)) {
            throw new NumberOutOfRange(Messages.getString("ResultSet.NumberOutOfRange", new Object[] { d, getTargetTypeName() }));
        }
        return (int) d.longValue();
    }

    @Override
    public Integer createFromDouble(double d) {
        if (this.jdbcCompliantTruncationForReads && (d < Integer.MIN_VALUE || d > Integer.MAX_VALUE)) {
            throw new NumberOutOfRange(Messages.getString("ResultSet.NumberOutOfRange", new Object[] { d, getTargetTypeName() }));
        }
        return (int) d;
    }

    @Override
    public Integer createFromBit(byte[] bytes, int offset, int length) {
        long l = DataTypeUtil.bitToLong(bytes, offset, length);
        if (this.jdbcCompliantTruncationForReads && l >> 32 != 0) {
            throw new NumberOutOfRange(Messages.getString("ResultSet.NumberOutOfRange", new Object[] { Long.valueOf(l).toString(), getTargetTypeName() }));
        }
        return (int) l;
    }

    @Override
    public String getTargetTypeName() {
        return Integer.class.getName();
    }

}
