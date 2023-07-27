/*
 * Copyright (c) 2015, 2023, Oracle and/or its affiliates.
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

import com.mysql.cj.conf.PropertySet;

/**
 * A value factory for creating {@link java.math.BigDecimal} values.
 */
public class BigDecimalValueFactory extends AbstractNumericValueFactory<BigDecimal> {

    int scale;
    boolean hasScale;

    public BigDecimalValueFactory(PropertySet pset) {
        super(pset);
    }

    public BigDecimalValueFactory(PropertySet pset, int scale) {
        super(pset);
        this.scale = scale;
        this.hasScale = true;
    }

    /**
     * Adjust the result value by apply the scale, if appropriate.
     *
     * @param d
     *            value
     * @return result
     */
    private BigDecimal adjustResult(BigDecimal d) {
        if (this.hasScale) {
            try {
                return d.setScale(this.scale);
            } catch (ArithmeticException ex) {
                // try this if above fails
                return d.setScale(this.scale, BigDecimal.ROUND_HALF_UP);
            }
        }

        return d;
    }

    @Override
    public BigDecimal createFromBigInteger(BigInteger i) {
        return adjustResult(new BigDecimal(i));
    }

    @Override
    public BigDecimal createFromLong(long l) {
        return adjustResult(BigDecimal.valueOf(l));
    }

    @Override
    public BigDecimal createFromBigDecimal(BigDecimal d) {
        return adjustResult(d);
    }

    @Override
    public BigDecimal createFromDouble(double d) {
        return adjustResult(BigDecimal.valueOf(d));
    }

    @Override
    public BigDecimal createFromBit(byte[] bytes, int offset, int length) {
        return new BigDecimal(new BigInteger(ByteBuffer.allocate(length + 1).put((byte) 0).put(bytes, offset, length).array()));
    }

    @Override
    public String getTargetTypeName() {
        return BigDecimal.class.getName();
    }

}
