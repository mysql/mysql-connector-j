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

import com.mysql.cj.Messages;
import com.mysql.cj.exceptions.NumberOutOfRange;

/**
 * A decorating value factory to enforce floating point range limits.
 */
public class FloatingPointBoundsEnforcer<T> extends BaseDecoratingValueFactory<T> {
    private double min;
    private double max;

    public FloatingPointBoundsEnforcer(ValueFactory<T> targetVf, double min, double max) {
        super(targetVf);
        this.min = min;
        this.max = max;
    }

    @Override
    public T createFromLong(long l) {
        if (l < this.min || l > this.max) {
            throw new NumberOutOfRange(Messages.getString("ResultSet.NumberOutOfRange", new Object[] { l, this.targetVf.getTargetTypeName() }));
        }
        return this.targetVf.createFromLong(l);
    }

    @Override
    public T createFromBigInteger(BigInteger i) {
        if (new BigDecimal(i).compareTo(BigDecimal.valueOf(this.min)) < 0 || new BigDecimal(i).compareTo(BigDecimal.valueOf(this.max)) > 0) {
            throw new NumberOutOfRange(Messages.getString("ResultSet.NumberOutOfRange", new Object[] { i, this.targetVf.getTargetTypeName() }));
        }
        return this.targetVf.createFromBigInteger(i);
    }

    @Override
    public T createFromDouble(double d) {
        if (d < this.min || d > this.max) {
            throw new NumberOutOfRange(Messages.getString("ResultSet.NumberOutOfRange", new Object[] { d, this.targetVf.getTargetTypeName() }));
        }
        return this.targetVf.createFromDouble(d);
    }

    @Override
    public T createFromBigDecimal(BigDecimal d) {
        if (d.compareTo(BigDecimal.valueOf(this.min)) < 0 || d.compareTo(BigDecimal.valueOf(this.max)) > 0) {
            throw new NumberOutOfRange(Messages.getString("ResultSet.NumberOutOfRange", new Object[] { d, this.targetVf.getTargetTypeName() }));
        }
        return this.targetVf.createFromBigDecimal(d);
    }
}
