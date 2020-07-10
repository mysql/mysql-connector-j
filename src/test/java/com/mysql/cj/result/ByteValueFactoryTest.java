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
import java.util.Properties;
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
 * Tests for {@link ByteValueFactory}
 */
public class ByteValueFactoryTest extends CommonAsserts {
    PropertySet pset = new DefaultPropertySet();
    ValueFactory<Byte> vf = new ByteValueFactory(this.pset);

    @Test
    public void testCreateFromDate() {
        assertThrows(DataConversionException.class, "Unsupported conversion from DATE to java.lang.Byte", new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                ByteValueFactoryTest.this.vf.createFromDate(new InternalDate(2006, 1, 1));
                return null;
            }
        });
    }

    @Test
    public void testCreateFromTime() {
        assertThrows(DataConversionException.class, "Unsupported conversion from TIME to java.lang.Byte", new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                ByteValueFactoryTest.this.vf.createFromTime(new InternalTime(12, 0, 0, 0, 0));
                return null;
            }
        });
    }

    @Test
    public void testCreateFromTimestamp() {
        assertThrows(DataConversionException.class, "Unsupported conversion from TIMESTAMP to java.lang.Byte", new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                ByteValueFactoryTest.this.vf.createFromTimestamp(new InternalTimestamp(2006, 1, 1, 12, 0, 0, 0, 0));
                return null;
            }
        });
    }

    @Test
    public void testCreateFromLong() {
        assertEquals(Byte.valueOf((byte) 1), this.vf.createFromLong(1));
        assertEquals(Byte.valueOf(Byte.MAX_VALUE), this.vf.createFromLong(Byte.MAX_VALUE));
        assertEquals(Byte.valueOf((byte) -1), this.vf.createFromLong(-1));
        assertEquals(Byte.valueOf(Byte.MIN_VALUE), this.vf.createFromLong(Byte.MIN_VALUE));
        assertThrows(NumberOutOfRange.class, "Value '128' is outside of valid range for type java.lang.Byte", new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                ByteValueFactoryTest.this.vf.createFromLong(Byte.MAX_VALUE + 1);
                return null;
            }
        });
        assertThrows(NumberOutOfRange.class, "Value '-129' is outside of valid range for type java.lang.Byte", new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                ByteValueFactoryTest.this.vf.createFromLong(Byte.MIN_VALUE - 1);
                return null;
            }
        });
    }

    @Test
    public void testCreateFromBigInteger() {
        assertEquals(Byte.valueOf((byte) 1), this.vf.createFromBigInteger(Constants.BIG_INTEGER_ONE));
        assertEquals(Byte.valueOf(Byte.MAX_VALUE), this.vf.createFromBigInteger(Constants.BIG_INTEGER_MAX_BYTE_VALUE));
        assertEquals(Byte.valueOf((byte) -1), this.vf.createFromBigInteger(Constants.BIG_INTEGER_NEGATIVE_ONE));
        assertEquals(Byte.valueOf(Byte.MIN_VALUE), this.vf.createFromBigInteger(Constants.BIG_INTEGER_MIN_BYTE_VALUE));
        assertThrows(NumberOutOfRange.class, "Value .+ is outside of valid range for type java.lang.Byte", new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                ByteValueFactoryTest.this.vf.createFromBigInteger(BigInteger.valueOf(Byte.MAX_VALUE + 1));
                return null;
            }
        });
        assertThrows(NumberOutOfRange.class, "Value .+ is outside of valid range for type java.lang.Byte", new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                ByteValueFactoryTest.this.vf.createFromBigInteger(BigInteger.valueOf(Byte.MIN_VALUE - 1));
                return null;
            }
        });
    }

    @Test
    public void testCreateFromDouble() {
        assertEquals(Byte.valueOf((byte) 1), this.vf.createFromDouble(1));
        assertEquals(Byte.valueOf(Byte.MAX_VALUE), this.vf.createFromDouble(Byte.MAX_VALUE));
        assertEquals(Byte.valueOf((byte) -1), this.vf.createFromDouble(-1));
        assertEquals(Byte.valueOf(Byte.MIN_VALUE), this.vf.createFromDouble(Byte.MIN_VALUE));
        assertThrows(NumberOutOfRange.class, "Value .+ is outside of valid range for type java.lang.Byte", new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                ByteValueFactoryTest.this.vf.createFromDouble(Byte.MAX_VALUE + 0.5);
                return null;
            }
        });
        assertThrows(NumberOutOfRange.class, "Value .+ is outside of valid range for type java.lang.Byte", new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                ByteValueFactoryTest.this.vf.createFromDouble(Byte.MIN_VALUE - 0.5);
                return null;
            }
        });
    }

    @Test
    public void testCreateFromBigDecimal() {
        assertEquals(Byte.valueOf((byte) 1), this.vf.createFromBigDecimal(Constants.BIG_DECIMAL_ONE));
        assertEquals(Byte.valueOf(Byte.MAX_VALUE), this.vf.createFromBigDecimal(Constants.BIG_DECIMAL_MAX_BYTE_VALUE));
        assertEquals(Byte.valueOf((byte) -1), this.vf.createFromBigDecimal(Constants.BIG_DECIMAL_NEGATIVE_ONE));
        assertEquals(Byte.valueOf(Byte.MIN_VALUE), this.vf.createFromBigDecimal(Constants.BIG_DECIMAL_MIN_BYTE_VALUE));
        assertThrows(NumberOutOfRange.class, "Value .+ is outside of valid range for type java.lang.Byte", new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                ByteValueFactoryTest.this.vf.createFromBigDecimal(BigDecimal.valueOf(Byte.MAX_VALUE + 1));
                return null;
            }
        });
        assertThrows(NumberOutOfRange.class, "Value .+ is outside of valid range for type java.lang.Byte", new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                ByteValueFactoryTest.this.vf.createFromBigDecimal(BigDecimal.valueOf(Byte.MIN_VALUE - 1));
                return null;
            }
        });
    }

    @Test
    public void testCreateFromBytes() {
        Field f = new Field("test", "test", 33, "UTF-8", MysqlType.VARCHAR, 10);

        this.pset.getBooleanProperty(PropertyKey.emptyStringsConvertToZero).setValue(true);
        assertEquals(Byte.valueOf((byte) 0), this.vf.createFromBytes("".getBytes(), 0, 0, f));

        this.pset.getBooleanProperty(PropertyKey.emptyStringsConvertToZero).setValue(false);
        assertThrows(NumberOutOfRange.class, "Value .+ is outside of valid range for type java.lang.Byte", new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                ByteValueFactoryTest.this.vf.createFromBytes("".getBytes(), 0, 0, f);
                return null;
            }
        });

        // jdbcCompliantTruncation initial value is cached in ValueFactiry, thus we need to construct a property set with required initial value
        Properties props = new Properties();
        props.setProperty(PropertyKey.jdbcCompliantTruncation.getKeyName(), "false");
        PropertySet pset1 = new DefaultPropertySet();
        pset1.initializeProperties(props);
        ValueFactory<Byte> vfAllowTrunc = new ByteValueFactory(pset1);
        assertEquals(Byte.valueOf((byte) '-'), vfAllowTrunc.createFromBytes("-1.0".getBytes(), 0, 4, f));
        assertEquals(Byte.valueOf((byte) '1'), vfAllowTrunc.createFromBytes("1e0".getBytes(), 0, 3, f));
        assertEquals(Byte.valueOf((byte) '-'), vfAllowTrunc.createFromBytes("-1e1".getBytes(), 0, 4, f));
        assertEquals(Byte.valueOf((byte) '1'), vfAllowTrunc.createFromBytes("1".getBytes(), 0, 1, f));
        assertEquals(Byte.valueOf((byte) '1'), vfAllowTrunc.createFromBytes("123".getBytes(), 0, 3, f));
        assertEquals(Byte.valueOf((byte) '-'), vfAllowTrunc.createFromBytes("-1".getBytes(), 0, 2, f));
        assertEquals(Byte.valueOf((byte) '0'), vfAllowTrunc.createFromBytes("0".getBytes(), 0, 1, f));
        assertEquals(Byte.valueOf((byte) '0'), vfAllowTrunc.createFromBytes("000".getBytes(), 0, 3, f));

        // jdbcCompliantTruncation=true by default
        assertEquals(Byte.valueOf((byte) '1'), this.vf.createFromBytes("1".getBytes(), 0, 1, f));
        assertEquals(Byte.valueOf((byte) 1), this.vf.createFromBytes(new byte[] { 1 }, 0, 1, f));
        assertThrows(NumberOutOfRange.class, "Value '-1.0' is outside of valid range for type java.lang.Byte", new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                ByteValueFactoryTest.this.vf.createFromBytes("-1.0".getBytes(), 0, 4, f);
                return null;
            }
        });
        assertThrows(NumberOutOfRange.class, "Value '1e0' is outside of valid range for type java.lang.Byte", new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                ByteValueFactoryTest.this.vf.createFromBytes("1e0".getBytes(), 0, 3, f);
                return null;
            }
        });
        assertThrows(NumberOutOfRange.class, "Value '123' is outside of valid range for type java.lang.Byte", new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                ByteValueFactoryTest.this.vf.createFromBytes("123".getBytes(), 0, 3, f);
                return null;
            }
        });
        assertThrows(NumberOutOfRange.class, "Value 'just a string' is outside of valid range for type java.lang.Byte", new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                ByteValueFactoryTest.this.vf.createFromBytes("just a string".getBytes(), 0, 13, f);
                return null;
            }
        });
    }

    @Test
    public void testCreateFromBit() {
        assertEquals(Byte.valueOf((byte) 0), this.vf.createFromBit(new byte[] { 0 }, 0, 1));
        assertEquals(Byte.valueOf((byte) 1), this.vf.createFromBit(new byte[] { 1 }, 0, 1));
        assertEquals(Byte.valueOf((byte) 2), this.vf.createFromBit(new byte[] { 2 }, 0, 1));
        assertEquals(Byte.valueOf((byte) 127), this.vf.createFromBit(new byte[] { (byte) 127 }, 0, 1));
        assertEquals(Byte.valueOf((byte) -128), this.vf.createFromBit(new byte[] { (byte) -128 }, 0, 1));
        assertEquals(Byte.valueOf((byte) 0xff), this.vf.createFromBit(new byte[] { (byte) 0xff }, 0, 1));
        assertThrows(NumberOutOfRange.class, "Value .+ is outside of valid range for type java.lang.Byte", new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                ByteValueFactoryTest.this.vf.createFromBit(new byte[] { (byte) 200, (byte) 100 }, 0, 2);
                return null;
            }
        });
    }

    @Test
    public void testCreateFromNull() {
        assertNull(this.vf.createFromNull());
    }
}
