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
import java.nio.ByteBuffer;

/**
 * A value factory for creating {@link java.math.BigDecimal} values.
 */
public class BigDecimalValueFactory extends DefaultValueFactory<BigDecimal> {
    int scale;
    boolean hasScale;

    public BigDecimalValueFactory() {
    }

    public BigDecimalValueFactory(int scale) {
        this.scale = scale;
        this.hasScale = true;
    }

    /**
     * Adjust the result value by apply the scale, if appropriate.
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

    public String getTargetTypeName() {
        return BigDecimal.class.getName();
    }
}
