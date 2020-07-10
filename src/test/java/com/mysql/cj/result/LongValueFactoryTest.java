/*
 * Copyright (c) 2019, 2020, Oracle and/or its affiliates.
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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.concurrent.Callable;

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
 * Tests for {@link LongValueFactory}
 */
public class LongValueFactoryTest extends CommonAsserts {
    PropertySet pset = new DefaultPropertySet();
    ValueFactory<Long> vf = new LongValueFactory(this.pset);

    @Test
    public void testCreateFromDate() {
        assertThrows(DataConversionException.class, "Unsupported conversion from DATE to java.lang.Long", new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                LongValueFactoryTest.this.vf.createFromDate(new InternalDate(2006, 1, 1));
                return null;
            }
        });
    }

    @Test
    public void testCreateFromTime() {
        assertThrows(DataConversionException.class, "Unsupported conversion from TIME to java.lang.Long", new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                LongValueFactoryTest.this.vf.createFromTime(new InternalTime(12, 0, 0, 0, 0));
                return null;
            }
        });
    }

    @Test
    public void testCreateFromTimestamp() {
        assertThrows(DataConversionException.class, "Unsupported conversion from TIMESTAMP to java.lang.Long", new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                LongValueFactoryTest.this.vf.createFromTimestamp(new InternalTimestamp(2006, 1, 1, 12, 0, 0, 0, 0));
                return null;
            }
        });
    }

    @Test
    public void testCreateFromLong() {
        assertEquals(Long.valueOf(1), this.vf.createFromLong(1));
        assertEquals(Long.valueOf(Long.MAX_VALUE), this.vf.createFromLong(Long.MAX_VALUE));
        assertEquals(Long.valueOf(-1), this.vf.createFromLong(-1));
        assertEquals(Long.valueOf(Long.MIN_VALUE), this.vf.createFromLong(Long.MIN_VALUE));
    }

    @Test
    public void testCreateFromBigInteger() {
        assertEquals(Long.valueOf(1), this.vf.createFromBigInteger(Constants.BIG_INTEGER_ONE));
        assertEquals(Long.valueOf(Long.MAX_VALUE), this.vf.createFromBigInteger(Constants.BIG_INTEGER_MAX_LONG_VALUE));
        assertEquals(Long.valueOf(-1), this.vf.createFromBigInteger(Constants.BIG_INTEGER_NEGATIVE_ONE));
        assertEquals(Long.valueOf(Long.MIN_VALUE), this.vf.createFromBigInteger(Constants.BIG_INTEGER_MIN_LONG_VALUE));
        assertThrows(NumberOutOfRange.class, "Value .+ is outside of valid range for type java.lang.Long", new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                LongValueFactoryTest.this.vf.createFromBigInteger(Constants.BIG_INTEGER_MAX_LONG_VALUE.add(BigInteger.ONE));
                return null;
            }
        });
        assertThrows(NumberOutOfRange.class, "Value .+ is outside of valid range for type java.lang.Long", new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                LongValueFactoryTest.this.vf.createFromBigInteger(Constants.BIG_INTEGER_MIN_LONG_VALUE.subtract(BigInteger.ONE));
                return null;
            }
        });
    }

    @Test
    public void testCreateFromDouble() {
        assertEquals(Long.valueOf(1), this.vf.createFromDouble(1));
        assertEquals(Long.valueOf(Long.MAX_VALUE), this.vf.createFromDouble(Long.MAX_VALUE));
        assertEquals(Long.valueOf(-1), this.vf.createFromDouble(-1));
        assertEquals(Long.valueOf(Long.MIN_VALUE), this.vf.createFromDouble(Long.MIN_VALUE));
        assertThrows(NumberOutOfRange.class, "Value .+ is outside of valid range for type java.lang.Long", new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                LongValueFactoryTest.this.vf.createFromDouble(Constants.BIG_INTEGER_MAX_LONG_VALUE.add(Constants.BIG_INTEGER_MAX_INTEGER_VALUE).doubleValue());
                return null;
            }
        });
        assertThrows(NumberOutOfRange.class, "Value .+ is outside of valid range for type java.lang.Long", new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                LongValueFactoryTest.this.vf.createFromDouble(Constants.BIG_INTEGER_MIN_LONG_VALUE.add(Constants.BIG_INTEGER_MIN_INTEGER_VALUE).doubleValue());
                return null;
            }
        });
    }

    @Test
    public void testCreateFromBigDecimal() {
        assertEquals(Long.valueOf(1), this.vf.createFromBigDecimal(Constants.BIG_DECIMAL_ONE));
        assertEquals(Long.valueOf(Long.MAX_VALUE), this.vf.createFromBigDecimal(Constants.BIG_DECIMAL_MAX_LONG_VALUE));
        assertEquals(Long.valueOf(-1), this.vf.createFromBigDecimal(Constants.BIG_DECIMAL_NEGATIVE_ONE));
        assertEquals(Long.valueOf(Long.MIN_VALUE), this.vf.createFromBigDecimal(Constants.BIG_DECIMAL_MIN_LONG_VALUE));
        assertThrows(NumberOutOfRange.class, "Value .+ is outside of valid range for type java.lang.Long", new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                LongValueFactoryTest.this.vf.createFromBigDecimal(
                        BigDecimal.valueOf(Constants.BIG_INTEGER_MAX_LONG_VALUE.add(Constants.BIG_INTEGER_MAX_INTEGER_VALUE).doubleValue()));
                return null;
            }
        });
        assertThrows(NumberOutOfRange.class, "Value .+ is outside of valid range for type java.lang.Long", new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                LongValueFactoryTest.this.vf.createFromBigDecimal(
                        BigDecimal.valueOf(Constants.BIG_INTEGER_MIN_LONG_VALUE.add(Constants.BIG_INTEGER_MIN_INTEGER_VALUE).doubleValue()));
                return null;
            }
        });
    }

    @Test
    public void testCreateFromBytes() {
        Field f = new Field("test", "test", 33, "UTF-8", MysqlType.VARCHAR, 10);

        this.pset.getBooleanProperty(PropertyKey.emptyStringsConvertToZero).setValue(true);
        assertEquals(Long.valueOf(0), this.vf.createFromBytes("".getBytes(), 0, 0, f));

        this.pset.getBooleanProperty(PropertyKey.emptyStringsConvertToZero).setValue(false);
        assertThrows(DataConversionException.class, "Cannot determine value type from string ''", new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                LongValueFactoryTest.this.vf.createFromBytes("".getBytes(), 0, 0, f);
                return null;
            }
        });

        assertEquals(Long.valueOf(-1), this.vf.createFromBytes("-1.0".getBytes(), 0, 4, f));
        assertEquals(Long.valueOf(1), this.vf.createFromBytes("1e0".getBytes(), 0, 3, f));
        assertEquals(Long.valueOf(10), this.vf.createFromBytes("1e1".getBytes(), 0, 3, f));
        assertEquals(Long.valueOf(12), this.vf.createFromBytes("1.2E1".getBytes(), 0, 5, f));
        assertEquals(Long.valueOf(0), this.vf.createFromBytes("1.2E-2".getBytes(), 0, 6, f)); // TODO check truncation ?
        assertEquals(Long.valueOf(0), this.vf.createFromBytes("0.0".getBytes(), 0, 3, f));
        assertEquals(Long.valueOf(-10), this.vf.createFromBytes("-1e1".getBytes(), 0, 4, f));

        assertEquals(Long.valueOf(1), this.vf.createFromBytes("1".getBytes(), 0, 1, f));
        assertEquals(Long.valueOf(123), this.vf.createFromBytes("123".getBytes(), 0, 3, f));
        assertEquals(Long.valueOf(-1), this.vf.createFromBytes("-1".getBytes(), 0, 2, f));
        assertEquals(Long.valueOf(-123), this.vf.createFromBytes("-123".getBytes(), 0, 4, f));
        assertEquals(Long.valueOf(0), this.vf.createFromBytes("0".getBytes(), 0, 1, f));
        assertEquals(Long.valueOf(0), this.vf.createFromBytes("000".getBytes(), 0, 3, f));

        assertThrows(DataConversionException.class, "Cannot determine value type from string 'just a string'", new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                LongValueFactoryTest.this.vf.createFromBytes("just a string".getBytes(), 0, 13, f);
                return null;
            }
        });
    }

    @Test
    public void testCreateFromBit() {
        assertEquals(Long.valueOf(0), this.vf.createFromBit(new byte[] { 0 }, 0, 1));
        assertEquals(Long.valueOf(1), this.vf.createFromBit(new byte[] { 1 }, 0, 1));
        assertEquals(Long.valueOf(2), this.vf.createFromBit(new byte[] { 2 }, 0, 1));
        assertEquals(Long.valueOf(0xffL), this.vf.createFromBit(new byte[] { (byte) 0xff }, 0, 1));
        assertEquals(Long.valueOf(0xffffL), this.vf.createFromBit(new byte[] { (byte) 0xff, (byte) 0xff }, 0, 2));
        assertEquals(Long.valueOf(0xffffffL), this.vf.createFromBit(new byte[] { (byte) 0xff, (byte) 0xff, (byte) 0xff }, 0, 3));
        assertEquals(Long.valueOf(0xffffffffL), this.vf.createFromBit(new byte[] { (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff }, 0, 4));
        assertEquals(Long.valueOf(0xffffffffffL), this.vf.createFromBit(new byte[] { (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff }, 0, 5));
        assertEquals(Long.valueOf(0xffffffffffffL),
                this.vf.createFromBit(new byte[] { (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff }, 0, 6));
        assertEquals(Long.valueOf(0xffffffffffffffL),
                this.vf.createFromBit(new byte[] { (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff }, 0, 7));
        assertEquals(Long.valueOf(0xffffffffffffffffL), this.vf
                .createFromBit(new byte[] { (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff }, 0, 8));
    }

    @Test
    public void testCreateFromNull() {
        assertNull(this.vf.createFromNull());
    }
}
