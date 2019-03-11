/*
 * Copyright (c) 2019, Oracle and/or its affiliates. All rights reserved.
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

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import com.mysql.cj.Constants;
import com.mysql.cj.MysqlType;
import com.mysql.cj.conf.DefaultPropertySet;
import com.mysql.cj.conf.PropertyKey;
import com.mysql.cj.conf.PropertySet;
import com.mysql.cj.protocol.InternalDate;
import com.mysql.cj.protocol.InternalTime;
import com.mysql.cj.protocol.InternalTimestamp;

/**
 * Tests for {@link StringValueFactory}
 */
public class StringValueFactoryTest extends CommonAsserts {
    PropertySet pset = new DefaultPropertySet();
    ValueFactory<String> vf = new StringValueFactory(this.pset);

    @Test
    public void testCreateFromDate() {
        this.vf.createFromDate(new InternalDate(2006, 1, 1));
        assertEquals("2015-05-01", this.vf.createFromDate(new InternalDate(2015, 5, 1))); // May 1st
    }

    @Test
    public void testCreateFromTime() {
        assertEquals("12:20:02.000000004", this.vf.createFromTime(new InternalTime(12, 20, 02, 4)));
        assertEquals("01:01:01.000000001", this.vf.createFromTime(new InternalTime(1, 1, 1, 1)));
        assertEquals("-1:00:00", this.vf.createFromTime(new InternalTime(-1, 0, 0, 0)).toString());
        assertEquals("-13:00:00", this.vf.createFromTime(new InternalTime(-13, 0, 0, 0)).toString());
        assertEquals("44:05:06", this.vf.createFromTime(new InternalTime(44, 5, 6, 0)).toString());
        assertEquals("44:05:06.000000300", this.vf.createFromTime(new InternalTime(44, 5, 6, 300)).toString());
    }

    @Test
    public void testCreateFromTimestamp() {
        assertEquals("2015-05-01 12:20:02.000000004", this.vf.createFromTimestamp(new InternalTimestamp(2015, 05, 01, 12, 20, 02, 4)));
        assertEquals("2018-01-01 01:01:01.000000001", this.vf.createFromTimestamp(new InternalTimestamp(2018, 1, 1, 1, 1, 1, 1)));
        assertEquals("0000-00-00 01:01:01.000000001", this.vf.createFromTimestamp(new InternalTimestamp(0, 0, 0, 1, 1, 1, 1)));
        assertEquals("0000-00-01 01:01:01.000000001", this.vf.createFromTimestamp(new InternalTimestamp(0, 0, 1, 1, 1, 1, 1)));
        assertEquals("0000-01-00 01:01:01.000000001", this.vf.createFromTimestamp(new InternalTimestamp(0, 1, 0, 1, 1, 1, 1)));
        assertEquals("0000-01-01 01:01:01.000000001", this.vf.createFromTimestamp(new InternalTimestamp(0, 1, 1, 1, 1, 1, 1)));
        assertEquals("0001-00-00 01:01:01.000000001", this.vf.createFromTimestamp(new InternalTimestamp(1, 0, 0, 1, 1, 1, 1)));
        assertEquals("0001-00-01 01:01:01.000000001", this.vf.createFromTimestamp(new InternalTimestamp(1, 0, 1, 1, 1, 1, 1)));
        assertEquals("0001-01-00 01:01:01.000000001", this.vf.createFromTimestamp(new InternalTimestamp(1, 1, 0, 1, 1, 1, 1)));
    }

    @Test
    public void testCreateFromLong() {
        assertEquals("1", this.vf.createFromLong(1));
        assertEquals("2147483647", this.vf.createFromLong(Integer.MAX_VALUE));
        assertEquals("-1", this.vf.createFromLong(-1));
        assertEquals("-2147483648", this.vf.createFromLong(Integer.MIN_VALUE));
    }

    @Test
    public void testCreateFromBigInteger() {
        assertEquals("1", this.vf.createFromBigInteger(Constants.BIG_INTEGER_ONE));
        assertEquals("2147483647", this.vf.createFromBigInteger(Constants.BIG_INTEGER_MAX_INTEGER_VALUE));
        assertEquals("-1", this.vf.createFromBigInteger(Constants.BIG_INTEGER_NEGATIVE_ONE));
        assertEquals("-2147483648", this.vf.createFromBigInteger(Constants.BIG_INTEGER_MIN_INTEGER_VALUE));
    }

    @Test
    public void testCreateFromDouble() {
        assertEquals("1.0", this.vf.createFromDouble(1));
        assertEquals("2.147483647E9", this.vf.createFromDouble(Integer.MAX_VALUE));
        assertEquals("-1.0", this.vf.createFromDouble(-1));
        assertEquals("-2.147483648E9", this.vf.createFromDouble(Integer.MIN_VALUE));
    }

    @Test
    public void testCreateFromBigDecimal() {
        assertEquals("1", this.vf.createFromBigDecimal(Constants.BIG_DECIMAL_ONE));
        assertEquals("2147483647", this.vf.createFromBigDecimal(Constants.BIG_DECIMAL_MAX_INTEGER_VALUE));
        assertEquals("-1", this.vf.createFromBigDecimal(Constants.BIG_DECIMAL_NEGATIVE_ONE));
        assertEquals("-2147483648", this.vf.createFromBigDecimal(Constants.BIG_DECIMAL_MIN_INTEGER_VALUE));
    }

    @Test
    public void testCreateFromBytes() {
        Field f = new Field("test", "test", 33, "UTF-8", MysqlType.VARCHAR, 10);

        this.pset.getBooleanProperty(PropertyKey.emptyStringsConvertToZero).setValue(true);
        assertEquals("", this.vf.createFromBytes("".getBytes(), 0, 0, f));

        this.pset.getBooleanProperty(PropertyKey.emptyStringsConvertToZero).setValue(false);
        assertEquals("", this.vf.createFromBytes("".getBytes(), 0, 0, f));

        assertEquals("2006-07-01", this.vf.createFromBytes("2006-07-01".getBytes(), 0, 10, f));
        assertEquals("12:13:14", this.vf.createFromBytes("12:13:14".getBytes(), 0, 8, f));
        assertEquals("2006-07-01 12:13:14", this.vf.createFromBytes("2006-07-01 12:13:14".getBytes(), 0, 19, f));

        assertEquals("-654", this.vf.createFromBytes("-654".getBytes(), 0, 4, f));
        assertEquals("654", this.vf.createFromBytes("654".getBytes(), 0, 3, f));

        assertEquals("-1.0", this.vf.createFromBytes("-1.0".getBytes(), 0, 4, f));
        assertEquals("1e0", this.vf.createFromBytes("1e0".getBytes(), 0, 3, f));
        assertEquals("1e1", this.vf.createFromBytes("1e1".getBytes(), 0, 3, f));
        assertEquals("1.2E1", this.vf.createFromBytes("1.2E1".getBytes(), 0, 5, f));
        assertEquals("1.2E-2", this.vf.createFromBytes("1.2E-2".getBytes(), 0, 6, f));

        assertEquals("nothin", this.vf.createFromBytes("nothing useful".getBytes(), 0, 6, f));

        assertEquals("true", this.vf.createFromBytes("true".getBytes(), 0, 4, f));
        assertEquals("false", this.vf.createFromBytes("false".getBytes(), 0, 5, f));
    }

    @Test
    public void testCreateFromBit() {
        assertEquals("49", this.vf.createFromBit("1".getBytes(), 0, 1));

        assertEquals("0", this.vf.createFromBit(new byte[] { 0 }, 0, 1));
        assertEquals("1", this.vf.createFromBit(new byte[] { 1 }, 0, 1));
        assertEquals("2", this.vf.createFromBit(new byte[] { 2 }, 0, 1));
        assertEquals("255", this.vf.createFromBit(new byte[] { (byte) 0xff }, 0, 1));
        assertEquals("65535", this.vf.createFromBit(new byte[] { (byte) 0xff, (byte) 0xff }, 0, 2));

        assertEquals(Long.valueOf(0xffffffffL).toString(), this.vf.createFromBit(new byte[] { (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff }, 0, 4));
        assertEquals(Long.valueOf(0xffffffffffL).toString(),
                this.vf.createFromBit(new byte[] { (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff }, 0, 5));
        assertEquals(Long.valueOf(0xffffffffffffL).toString(),
                this.vf.createFromBit(new byte[] { (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff }, 0, 6));
        assertEquals(Long.valueOf(0xffffffffffffffL).toString(),
                this.vf.createFromBit(new byte[] { (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff }, 0, 7));
        assertEquals(Long.valueOf(0xffffffffffffffffL).toString(), this.vf
                .createFromBit(new byte[] { (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff }, 0, 8));
    }

    @Test
    public void testCreateFromNull() {
        this.vf.createFromNull();
    }
}
