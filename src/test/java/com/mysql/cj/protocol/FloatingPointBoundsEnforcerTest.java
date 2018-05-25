/*
 * Copyright (c) 2018, Oracle and/or its affiliates. All rights reserved.
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

package com.mysql.cj.protocol;

import java.math.BigDecimal;
import java.math.BigInteger;

import org.junit.Test;

import com.mysql.cj.exceptions.NumberOutOfRange;
import com.mysql.cj.result.FloatingPointBoundsEnforcer;
import com.mysql.cj.result.StringValueFactory;
import com.mysql.cj.result.ValueFactory;

/**
 * Tests for {@link FloatingPointBoundsEnforcer}
 */
public class FloatingPointBoundsEnforcerTest {
    ValueFactory<String> rawStringVf = new StringValueFactory();
    ValueFactory<String> enforcing100Vf = new FloatingPointBoundsEnforcer<>(this.rawStringVf, -100, 100);

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
