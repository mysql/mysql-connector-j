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

package com.mysql.cj.core.io;

import java.math.BigDecimal;
import java.math.BigInteger;

import com.mysql.cj.api.io.ValueFactory;
import com.mysql.cj.core.Messages;
import com.mysql.cj.core.exceptions.NumberOutOfRange;

/**
 * A decorating value factory to enforce integer range limits.
 */
public class IntegerBoundsEnforcer<T> extends BaseDecoratingValueFactory<T> {
    private long min;
    private long max;

    public IntegerBoundsEnforcer(ValueFactory<T> targetVf, long min, long max) {
        super(targetVf);
        this.min = min;
        this.max = max;
    }

    @Override
    public T createFromLong(long l) {
        if (l < this.min || l > this.max) {
            throw new NumberOutOfRange(
                    Messages.getString("ResultSet.NumberOutOfRange", new Object[] { Long.valueOf(l).toString(), this.targetVf.getTargetTypeName() }));
        }
        return this.targetVf.createFromLong(l);
    }

    @Override
    public T createFromBigInteger(BigInteger i) {
        if (i.compareTo(BigInteger.valueOf(this.min)) < 0 || i.compareTo(BigInteger.valueOf(this.max)) > 0) {
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
