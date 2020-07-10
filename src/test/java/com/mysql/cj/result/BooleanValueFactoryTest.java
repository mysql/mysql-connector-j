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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.Callable;

import org.junit.jupiter.api.Test;

import com.mysql.cj.Constants;
import com.mysql.cj.MysqlType;
import com.mysql.cj.conf.DefaultPropertySet;
import com.mysql.cj.conf.PropertyKey;
import com.mysql.cj.conf.PropertySet;
import com.mysql.cj.exceptions.DataConversionException;
import com.mysql.cj.protocol.InternalDate;
import com.mysql.cj.protocol.InternalTime;
import com.mysql.cj.protocol.InternalTimestamp;

/**
 * Tests for {@link BooleanValueFactory}
 */
public class BooleanValueFactoryTest extends CommonAsserts {
    PropertySet pset = new DefaultPropertySet();
    ValueFactory<Boolean> vf = new BooleanValueFactory(this.pset);

    @Test
    public void testCreateFromDate() {
        assertThrows(DataConversionException.class, "Unsupported conversion from DATE to java.lang.Boolean", new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                BooleanValueFactoryTest.this.vf.createFromDate(new InternalDate(2006, 1, 1));
                return null;
            }
        });
    }

    @Test
    public void testCreateFromTime() {
        assertThrows(DataConversionException.class, "Unsupported conversion from TIME to java.lang.Boolean", new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                BooleanValueFactoryTest.this.vf.createFromTime(new InternalTime(12, 0, 0, 0, 0));
                return null;
            }
        });
    }

    @Test
    public void testCreateFromTimestamp() {
        assertThrows(DataConversionException.class, "Unsupported conversion from TIMESTAMP to java.lang.Boolean", new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                BooleanValueFactoryTest.this.vf.createFromTimestamp(new InternalTimestamp(2006, 1, 1, 12, 0, 0, 0, 0));
                return null;
            }
        });
    }

    @Test
    public void testCreateFromLong() {
        assertTrue(this.vf.createFromLong(1));
        assertTrue(this.vf.createFromLong(Integer.MAX_VALUE));
        assertTrue(this.vf.createFromLong(-1));
        assertFalse(this.vf.createFromLong(Integer.MIN_VALUE));
        assertFalse(this.vf.createFromLong(0));
    }

    @Test
    public void testCreateFromBigInteger() {
        assertTrue(this.vf.createFromBigInteger(Constants.BIG_INTEGER_ONE));
        assertTrue(this.vf.createFromBigInteger(Constants.BIG_INTEGER_MAX_INTEGER_VALUE));
        assertTrue(this.vf.createFromBigInteger(Constants.BIG_INTEGER_NEGATIVE_ONE));
        assertFalse(this.vf.createFromBigInteger(Constants.BIG_INTEGER_MIN_INTEGER_VALUE));
        assertFalse(this.vf.createFromBigInteger(Constants.BIG_INTEGER_ZERO));
    }

    @Test
    public void testCreateFromDouble() {
        assertTrue(this.vf.createFromDouble(1));
        assertTrue(this.vf.createFromDouble(Integer.MAX_VALUE));
        assertTrue(this.vf.createFromDouble(-1));
        assertFalse(this.vf.createFromDouble(Integer.MIN_VALUE));
        assertFalse(this.vf.createFromDouble(0));
    }

    @Test
    public void testCreateFromBigDecimal() {
        assertTrue(this.vf.createFromBigDecimal(Constants.BIG_DECIMAL_ONE));
        assertTrue(this.vf.createFromBigDecimal(Constants.BIG_DECIMAL_MAX_INTEGER_VALUE));
        assertTrue(this.vf.createFromBigDecimal(Constants.BIG_DECIMAL_NEGATIVE_ONE));
        assertFalse(this.vf.createFromBigDecimal(Constants.BIG_DECIMAL_MIN_INTEGER_VALUE));
        assertFalse(this.vf.createFromBigDecimal(Constants.BIG_DECIMAL_ZERO));
    }

    @Test
    public void testCreateFromBytes() {
        Field f = new Field("test", "test", 33, "UTF-8", MysqlType.VARCHAR, 10);

        this.pset.getBooleanProperty(PropertyKey.emptyStringsConvertToZero).setValue(true);
        assertFalse(this.vf.createFromBytes("".getBytes(), 0, 0, f));

        this.pset.getBooleanProperty(PropertyKey.emptyStringsConvertToZero).setValue(false);
        assertThrows(DataConversionException.class, "Cannot determine value type from string ''", new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                BooleanValueFactoryTest.this.vf.createFromBytes("".getBytes(), 0, 0, f);
                return null;
            }
        });

        assertFalse(this.vf.createFromBytes("False".getBytes(), 0, 5, f));
        assertFalse(this.vf.createFromBytes("N".getBytes(), 0, 1, f));
        assertTrue(this.vf.createFromBytes("tRue".getBytes(), 0, 4, f));
        assertTrue(this.vf.createFromBytes("Y".getBytes(), 0, 1, f));

        assertTrue(this.vf.createFromBytes("-1.0".getBytes(), 0, 4, f));
        assertTrue(this.vf.createFromBytes("1e0".getBytes(), 0, 3, f));
        assertTrue(this.vf.createFromBytes("1e1".getBytes(), 0, 3, f));
        assertTrue(this.vf.createFromBytes("1.2E1".getBytes(), 0, 5, f));
        assertTrue(this.vf.createFromBytes("1.2E-2".getBytes(), 0, 6, f));
        assertFalse(this.vf.createFromBytes("0.0".getBytes(), 0, 3, f));
        assertFalse(this.vf.createFromBytes("-1e1".getBytes(), 0, 4, f));

        assertTrue(this.vf.createFromBytes("1".getBytes(), 0, 1, f));
        assertTrue(this.vf.createFromBytes("123".getBytes(), 0, 3, f));
        assertTrue(this.vf.createFromBytes("-1".getBytes(), 0, 2, f));
        assertFalse(this.vf.createFromBytes("-123".getBytes(), 0, 4, f));
        assertFalse(this.vf.createFromBytes("0".getBytes(), 0, 1, f));
        assertFalse(this.vf.createFromBytes("000".getBytes(), 0, 3, f));

        assertThrows(DataConversionException.class, "Cannot determine value type from string 'just a string'", new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                BooleanValueFactoryTest.this.vf.createFromBytes("just a string".getBytes(), 0, 13, f);
                return null;
            }
        });
    }

    @Test
    public void testCreateFromBit() {
        assertFalse(this.vf.createFromBit(new byte[] { 0 }, 0, 1));
        assertTrue(this.vf.createFromBit(new byte[] { 1 }, 0, 1));
        assertTrue(this.vf.createFromBit(new byte[] { 2 }, 0, 1));
        assertTrue(this.vf.createFromBit(new byte[] { (byte) 0xff }, 0, 1));
    }

    @Test
    public void testCreateFromNull() {
        assertNull(this.vf.createFromNull());
    }
}
