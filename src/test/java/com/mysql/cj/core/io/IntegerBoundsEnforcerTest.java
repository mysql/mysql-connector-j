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

import org.junit.Test;

import com.mysql.cj.api.io.ValueFactory;
import com.mysql.cj.core.exceptions.NumberOutOfRange;

/**
 * Tests for {@link IntegerBoundsEnforcer}
 */
public class IntegerBoundsEnforcerTest {
    ValueFactory<String> rawStringVf = new StringValueFactory();
    ValueFactory<String> enforcing100Vf = new IntegerBoundsEnforcer<String>(this.rawStringVf, -100, 100);

    @Test
    public void testWithinBounds() {
        this.enforcing100Vf.createFromLong(1);
        this.enforcing100Vf.createFromLong(100);
        this.enforcing100Vf.createFromLong(-1);
        this.enforcing100Vf.createFromLong(-100);
        this.enforcing100Vf.createFromBigInteger(BigInteger.valueOf(1));
        this.enforcing100Vf.createFromBigInteger(BigInteger.valueOf(100));
        this.enforcing100Vf.createFromBigInteger(BigInteger.valueOf(-1));
        this.enforcing100Vf.createFromBigInteger(BigInteger.valueOf(-100));
        this.enforcing100Vf.createFromDouble(1);
        this.enforcing100Vf.createFromDouble(100);
        this.enforcing100Vf.createFromDouble(-1);
        this.enforcing100Vf.createFromDouble(-100);
        this.enforcing100Vf.createFromBigDecimal(BigDecimal.valueOf(1));
        this.enforcing100Vf.createFromBigDecimal(BigDecimal.valueOf(100));
        this.enforcing100Vf.createFromBigDecimal(BigDecimal.valueOf(-1));
        this.enforcing100Vf.createFromBigDecimal(BigDecimal.valueOf(-100));
    }

    @Test
    public void testOtherTypes() {
        // ok as long as there's no exception
        this.enforcing100Vf.createFromDate(2006, 1, 1);
        this.enforcing100Vf.createFromTime(12, 0, 0, 0);
        this.enforcing100Vf.createFromTimestamp(2006, 1, 1, 12, 0, 0, 0);
        this.enforcing100Vf.createFromBytes("abc".getBytes(), 0, 3);
        this.enforcing100Vf.createFromNull();
    }

    // exceeding max & min bounds for all types
    @Test(expected = NumberOutOfRange.class)
    public void testOutsideMaxBoundsLong() {
        this.enforcing100Vf.createFromLong(101);
    }

    @Test(expected = NumberOutOfRange.class)
    public void testOutsideMaxBoundsBigInteger() {
        this.enforcing100Vf.createFromBigInteger(BigInteger.valueOf(101));
    }

    @Test(expected = NumberOutOfRange.class)
    public void testOutsideMaxBoundsDouble() {
        this.enforcing100Vf.createFromDouble(100.5);
    }

    @Test(expected = NumberOutOfRange.class)
    public void testOutsideMaxBoundsBigDecimal() {
        this.enforcing100Vf.createFromBigDecimal(BigDecimal.valueOf(101));
    }

    @Test(expected = NumberOutOfRange.class)
    public void testOutsideMinBoundsLong() {
        this.enforcing100Vf.createFromLong(-101);
    }

    @Test(expected = NumberOutOfRange.class)
    public void testOutsideMinBoundsBigInteger() {
        this.enforcing100Vf.createFromBigInteger(BigInteger.valueOf(-101));
    }

    @Test(expected = NumberOutOfRange.class)
    public void testOutsideMinBoundsDouble() {
        this.enforcing100Vf.createFromDouble(-100.5);
    }

    @Test(expected = NumberOutOfRange.class)
    public void testOutsideMinBoundsBigDecimal() {
        this.enforcing100Vf.createFromBigDecimal(BigDecimal.valueOf(-101));
    }
}
