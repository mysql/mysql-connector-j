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

package com.mysql.cj.result;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;

import com.mysql.cj.Constants;
import com.mysql.cj.Messages;
import com.mysql.cj.conf.PropertySet;
import com.mysql.cj.exceptions.NumberOutOfRange;

/**
 * A value factory for creating float values.
 */
public class FloatValueFactory extends AbstractNumericValueFactory<Float> {

    public FloatValueFactory(PropertySet pset) {
        super(pset);
    }

    @Override
    public Float createFromBigInteger(BigInteger i) {
        if (this.jdbcCompliantTruncationForReads && (new BigDecimal(i).compareTo(Constants.BIG_DECIMAL_MAX_NEGATIVE_FLOAT_VALUE) < 0
                || new BigDecimal(i).compareTo(Constants.BIG_DECIMAL_MAX_FLOAT_VALUE) > 0)) {
            throw new NumberOutOfRange(Messages.getString("ResultSet.NumberOutOfRange", new Object[] { i, getTargetTypeName() }));
        }
        return (float) i.doubleValue();
    }

    @Override
    public Float createFromLong(long l) {
        if (this.jdbcCompliantTruncationForReads && (l < -Float.MAX_VALUE || l > Float.MAX_VALUE)) {
            throw new NumberOutOfRange(Messages.getString("ResultSet.NumberOutOfRange", new Object[] { l, getTargetTypeName() }));
        }
        return (float) l;
    }

    @Override
    public Float createFromBigDecimal(BigDecimal d) {
        if (this.jdbcCompliantTruncationForReads
                && (d.compareTo(Constants.BIG_DECIMAL_MAX_NEGATIVE_FLOAT_VALUE) < 0 || d.compareTo(Constants.BIG_DECIMAL_MAX_FLOAT_VALUE) > 0)) {
            throw new NumberOutOfRange(Messages.getString("ResultSet.NumberOutOfRange", new Object[] { d, getTargetTypeName() }));
        }
        return (float) d.doubleValue();
    }

    @Override
    public Float createFromDouble(double d) {
        if (this.jdbcCompliantTruncationForReads && (d < -Float.MAX_VALUE || d > Float.MAX_VALUE)) {
            throw new NumberOutOfRange(Messages.getString("ResultSet.NumberOutOfRange", new Object[] { d, getTargetTypeName() }));
        }
        return (float) d;
    }

    @Override
    public Float createFromBit(byte[] bytes, int offset, int length) {
        return new BigInteger(ByteBuffer.allocate(length + 1).put((byte) 0).put(bytes, offset, length).array()).floatValue();
    }

    public String getTargetTypeName() {
        return Float.class.getName();
    }
}
