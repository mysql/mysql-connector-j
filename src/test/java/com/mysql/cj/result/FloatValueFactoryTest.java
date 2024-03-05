/*
 * Copyright (c) 2018, 2024, Oracle and/or its affiliates.
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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.math.BigDecimal;

import org.junit.jupiter.api.Test;

import com.mysql.cj.Constants;
import com.mysql.cj.MysqlType;
import com.mysql.cj.conf.DefaultPropertySet;
import com.mysql.cj.conf.PropertyKey;
import com.mysql.cj.conf.PropertySet;
import com.mysql.cj.exceptions.DataConversionException;
import com.mysql.cj.exceptions.NumberOutOfRange;
import com.mysql.cj.protocol.InternalDate;
import com.mysql.cj.protocol.InternalTime;
import com.mysql.cj.protocol.InternalTimestamp;

/**
 * Tests for {@link FloatValueFactory}
 */
public class FloatValueFactoryTest extends CommonAsserts {

    PropertySet pset = new DefaultPropertySet();
    ValueFactory<Float> vf = new FloatValueFactory(this.pset);

    @Test
    public void testCreateFromDate() {
        assertThrows(DataConversionException.class, "Unsupported conversion from DATE to java.lang.Float", () -> {
            FloatValueFactoryTest.this.vf.createFromDate(new InternalDate(2006, 1, 1));
            return null;
        });
    }

    @Test
    public void testCreateFromTime() {
        assertThrows(DataConversionException.class, "Unsupported conversion from TIME to java.lang.Float", () -> {
            FloatValueFactoryTest.this.vf.createFromTime(new InternalTime(12, 0, 0, 0, 0));
            return null;
        });
    }

    @Test
    public void testCreateFromTimestamp() {
        assertThrows(DataConversionException.class, "Unsupported conversion from TIMESTAMP to java.lang.Float", () -> {
            FloatValueFactoryTest.this.vf.createFromTimestamp(new InternalTimestamp(2006, 1, 1, 12, 0, 0, 0, 0));
            return null;
        });
    }

    @Test
    public void testCreateFromLong() {
        assertEquals(Float.valueOf(1f), this.vf.createFromLong(1));
        assertEquals(Float.valueOf(Long.MAX_VALUE), this.vf.createFromLong(Long.MAX_VALUE));
        assertEquals(Float.valueOf(-1f), this.vf.createFromLong(-1));
        assertEquals(Float.valueOf(Long.MIN_VALUE), this.vf.createFromLong(Long.MIN_VALUE));
    }

    @Test
    public void testCreateFromBigInteger() {
        assertEquals(Float.valueOf(1f), this.vf.createFromBigInteger(Constants.BIG_INTEGER_ONE));
        assertEquals(Float.valueOf(Float.MAX_VALUE), this.vf.createFromBigInteger(Constants.BIG_DECIMAL_MAX_FLOAT_VALUE.toBigInteger()));
        assertEquals(Float.valueOf(-1f), this.vf.createFromBigInteger(Constants.BIG_INTEGER_NEGATIVE_ONE));
        assertEquals(Float.valueOf(-Float.MAX_VALUE), this.vf.createFromBigInteger(Constants.BIG_DECIMAL_MAX_NEGATIVE_FLOAT_VALUE.toBigInteger()));
        assertThrows(NumberOutOfRange.class, "Value .+ is outside of valid range for type java.lang.Float", () -> {
            FloatValueFactoryTest.this.vf.createFromBigInteger(Constants.BIG_DECIMAL_MAX_FLOAT_VALUE.toBigInteger().add(Constants.BIG_INTEGER_ONE));
            return null;
        });
        assertThrows(NumberOutOfRange.class, "Value .+ is outside of valid range for type java.lang.Float", () -> {
            FloatValueFactoryTest.this.vf
                    .createFromBigInteger(Constants.BIG_DECIMAL_MAX_NEGATIVE_FLOAT_VALUE.toBigInteger().subtract(Constants.BIG_INTEGER_ONE));
            return null;
        });
    }

    @Test
    public void testCreateFromDouble() {
        assertEquals(Float.valueOf(1f), this.vf.createFromDouble(1));
        assertEquals(Float.valueOf(Float.MAX_VALUE), this.vf.createFromDouble(Float.MAX_VALUE));
        assertEquals(Float.valueOf(-1f), this.vf.createFromDouble(-1));
        assertEquals(Float.valueOf(-Float.MAX_VALUE), this.vf.createFromDouble(-Float.MAX_VALUE));
        assertThrows(NumberOutOfRange.class, "Value .+ is outside of valid range for type java.lang.Float", () -> {
            FloatValueFactoryTest.this.vf.createFromDouble((double) Float.MAX_VALUE + Float.MAX_VALUE);
            return null;
        });
        assertThrows(NumberOutOfRange.class, "Value .+ is outside of valid range for type java.lang.Float", () -> {
            FloatValueFactoryTest.this.vf.createFromDouble((double) -Float.MAX_VALUE - Float.MAX_VALUE);
            return null;
        });
    }

    @Test
    public void testCreateFromBigDecimal() {
        assertEquals(Float.valueOf(1f), this.vf.createFromBigDecimal(Constants.BIG_DECIMAL_ONE));
        assertEquals(Float.valueOf(Float.MAX_VALUE), this.vf.createFromBigDecimal(Constants.BIG_DECIMAL_MAX_FLOAT_VALUE));
        assertEquals(Float.valueOf(-1f), this.vf.createFromBigDecimal(Constants.BIG_DECIMAL_NEGATIVE_ONE));
        assertEquals(Float.valueOf(-Float.MAX_VALUE), this.vf.createFromBigDecimal(Constants.BIG_DECIMAL_MAX_NEGATIVE_FLOAT_VALUE));
        assertThrows(NumberOutOfRange.class, "Value .+ is outside of valid range for type java.lang.Float", () -> {
            FloatValueFactoryTest.this.vf.createFromBigDecimal(Constants.BIG_DECIMAL_MAX_FLOAT_VALUE.add(Constants.BIG_DECIMAL_ONE));
            return null;
        });
        assertThrows(NumberOutOfRange.class, "Value .+ is outside of valid range for type java.lang.Float", () -> {
            FloatValueFactoryTest.this.vf.createFromBigDecimal(BigDecimal.valueOf((double) -Float.MAX_VALUE - Float.MAX_VALUE));
            return null;
        });
    }

    @Test
    public void testCreateFromBytes() {
        Field f = new Field("test", "test", 33, "UTF-8", MysqlType.VARCHAR, 10);

        this.pset.getBooleanProperty(PropertyKey.emptyStringsConvertToZero).setValue(true);
        assertEquals(Float.valueOf(0), this.vf.createFromBytes("".getBytes(), 0, 0, f));

        this.pset.getBooleanProperty(PropertyKey.emptyStringsConvertToZero).setValue(false);
        assertThrows(DataConversionException.class, "Cannot determine value type from string ''", () -> {
            FloatValueFactoryTest.this.vf.createFromBytes("".getBytes(), 0, 0, f);
            return null;
        });

        assertEquals(Float.valueOf(-1.0f), this.vf.createFromBytes("-1.0".getBytes(), 0, 4, f));
        assertEquals(Float.valueOf(1.0f), this.vf.createFromBytes("1e0".getBytes(), 0, 3, f));
        assertEquals(Float.valueOf(10.0f), this.vf.createFromBytes("1e1".getBytes(), 0, 3, f));
        assertEquals(Float.valueOf(12.0f), this.vf.createFromBytes("1.2E1".getBytes(), 0, 5, f));
        assertEquals(Float.valueOf(0.012f), this.vf.createFromBytes("1.2E-2".getBytes(), 0, 6, f));
        assertEquals(Float.valueOf(0.0f), this.vf.createFromBytes("0.0".getBytes(), 0, 3, f));
        assertEquals(Float.valueOf(-10.0f), this.vf.createFromBytes("-1e1".getBytes(), 0, 4, f));

        assertEquals(Float.valueOf(1), this.vf.createFromBytes("1".getBytes(), 0, 1, f));
        assertEquals(Float.valueOf(123), this.vf.createFromBytes("123".getBytes(), 0, 3, f));
        assertEquals(Float.valueOf(-1), this.vf.createFromBytes("-1".getBytes(), 0, 2, f));
        assertEquals(Float.valueOf(-123), this.vf.createFromBytes("-123".getBytes(), 0, 4, f));
        assertEquals(Float.valueOf(0), this.vf.createFromBytes("0".getBytes(), 0, 1, f));
        assertEquals(Float.valueOf(0), this.vf.createFromBytes("000".getBytes(), 0, 3, f));

        assertThrows(DataConversionException.class, "Cannot determine value type from string 'just a string'", () -> {
            FloatValueFactoryTest.this.vf.createFromBytes("just a string".getBytes(), 0, 13, f);
            return null;
        });
    }

    @Test
    public void testCreateFromBit() {
        assertEquals(Float.valueOf(0), this.vf.createFromBit(new byte[] { 0 }, 0, 1));
        assertEquals(Float.valueOf(1), this.vf.createFromBit(new byte[] { 1 }, 0, 1));
        assertEquals(Float.valueOf(2), this.vf.createFromBit(new byte[] { 2 }, 0, 1));
        assertEquals(Float.valueOf(255), this.vf.createFromBit(new byte[] { (byte) 0xff }, 0, 1));
        assertEquals(Float.valueOf(65535), this.vf.createFromBit(new byte[] { (byte) 0xff, (byte) 0xff }, 0, 2));
        assertEquals(Float.valueOf(0xffffffL), this.vf.createFromBit(new byte[] { (byte) 0xff, (byte) 0xff, (byte) 0xff }, 0, 3));
        assertEquals(Float.valueOf(0xffffffffL), this.vf.createFromBit(new byte[] { (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff }, 0, 4));
        assertEquals(Float.valueOf(0xffffffffffL), this.vf.createFromBit(new byte[] { (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff }, 0, 5));
        assertEquals(Float.valueOf(0xffffffffffffL),
                this.vf.createFromBit(new byte[] { (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff }, 0, 6));
        assertEquals(Float.valueOf(0xffffffffffffffL),
                this.vf.createFromBit(new byte[] { (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff }, 0, 7));
        assertEquals(Float.valueOf("1.8446744073709552E19"), this.vf
                .createFromBit(new byte[] { (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff }, 0, 8));
    }

    @Test
    public void testCreateFromNull() {
        assertNull(this.vf.createFromNull());
    }

}
