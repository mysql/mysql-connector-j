/*
 * Copyright (c) 2015, 2018, Oracle and/or its affiliates. All rights reserved.
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import org.junit.Test;

import com.mysql.cj.exceptions.DataConversionException;
import com.mysql.cj.result.BooleanValueFactory;
import com.mysql.cj.result.StringConverter;
import com.mysql.cj.result.StringValueFactory;
import com.mysql.cj.result.ValueFactory;

/**
 * Tests for {@link StringConverter}. Here we exercise the weird/wacky ways that we and/or JDBC allow retrieving data from columns other than the matching
 * types.
 */
public class StringConverterTest {
    private StringConverter<String> stringConverter = new StringConverter<>(null, new StringValueFactory());

    @Test
    public void testEmptyStringException() {
        this.stringConverter.setEmptyStringsConvertToZero(false);
        try {
            this.stringConverter.createFromBytes(new byte[] {}, 0, 0);
            fail("Empty string should not convert to anything");
        } catch (DataConversionException ex) {
            // expected
        }
    }

    @Test
    public void testEmptyStringToZero() {
        this.stringConverter.setEmptyStringsConvertToZero(true);
        assertEquals("0", this.stringConverter.createFromBytes(new byte[] {}, 0, 0));
    }

    @Test
    public void testBooleanFromString() {
        // true/false are the only values we support
        ValueFactory<Boolean> sc = new StringConverter<>(null, new BooleanValueFactory());
        assertEquals(true, sc.createFromBytes("true".getBytes(), 0, 4));
        assertEquals(false, sc.createFromBytes("false".getBytes(), 0, 5));
        try {
            sc.createFromBytes("xyz".getBytes(), 0, 3);
            fail("Cannot get boolean from arbitrary strings");
        } catch (DataConversionException ex) {
            // expected
        }
    }

    @Test
    public void testFloatFromString() {
        assertEquals("-1.0", this.stringConverter.createFromBytes("-1.0".getBytes(), 0, 4));
        assertEquals("1.0", this.stringConverter.createFromBytes("1e0".getBytes(), 0, 3));
        assertEquals("10.0", this.stringConverter.createFromBytes("1e1".getBytes(), 0, 3));
        assertEquals("12.0", this.stringConverter.createFromBytes("1.2E1".getBytes(), 0, 5));
        assertEquals("0.012", this.stringConverter.createFromBytes("1.2E-2".getBytes(), 0, 6));
    }

    @Test
    public void testIntFromString() {
        assertEquals("-654", this.stringConverter.createFromBytes("-654".getBytes(), 0, 4));
        assertEquals("654", this.stringConverter.createFromBytes("654".getBytes(), 0, 3));
    }

    @Test
    public void testDateFromString() {
        assertEquals("2006-07-01", this.stringConverter.createFromBytes("2006-07-01".getBytes(), 0, 10));
    }

    @Test
    public void testTimeFromString() {
        assertEquals("12:13:14", this.stringConverter.createFromBytes("12:13:14".getBytes(), 0, 8));
    }

    @Test
    public void testTimestampFromString() {
        assertEquals("2006-07-01 12:13:14", this.stringConverter.createFromBytes("2006-07-01 12:13:14".getBytes(), 0, 19));
    }

    @Test
    public void testArbitraryStringException() {
        byte[] val = "nothing useful".getBytes();
        try {
            this.stringConverter.createFromBytes(val, 0, val.length);
            fail("arbitrary string cannot be converted to anything useful");
        } catch (IllegalArgumentException ex) {
            // expected
        }
    }
}
