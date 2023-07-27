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

package com.mysql.cj.protocol.a;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

import com.mysql.cj.Constants;
import com.mysql.cj.conf.DefaultPropertySet;
import com.mysql.cj.exceptions.NumberOutOfRange;
import com.mysql.cj.protocol.InternalTime;
import com.mysql.cj.protocol.InternalTimestamp;
import com.mysql.cj.result.DefaultValueFactory;
import com.mysql.cj.result.Field;
import com.mysql.cj.result.StringValueFactory;
import com.mysql.cj.result.ValueFactory;

/**
 * Tests for {@link MysqlTextValueDecoder}.
 */
public class MysqlTextValueDecoderTest {

    private MysqlTextValueDecoder valueDecoder = new MysqlTextValueDecoder();

    @Test
    public void testNanosecondParsing() {
        // test value factory to extract the parsed nano-seconds
        ValueFactory<Integer> vf = new DefaultValueFactory<Integer>(new DefaultPropertySet()) {

            @Override
            public Integer createFromTime(InternalTime it) {
                return it.getNanos();
            }

            @Override
            public Integer createFromTimestamp(InternalTimestamp its) {
                return its.getNanos();
            }

            @Override
            public String getTargetTypeName() {
                return Integer.class.getName();
            }

            @Override
            public Integer createFromBytes(byte[] bytes, int offset, int length, Field f) {
                return null;
            }

        };

        // the fractional second part is determined by the # of digits
        assertEquals(new Integer(900000000), this.valueDecoder.decodeTimestamp("2016-03-14 14:34:01.9".getBytes(), 0, 21, 9, vf));
        assertEquals(new Integer(950000000), this.valueDecoder.decodeTimestamp("2016-03-14 14:34:01.95".getBytes(), 0, 22, 9, vf));
        assertEquals(new Integer(956000000), this.valueDecoder.decodeTimestamp("2016-03-14 14:34:01.956".getBytes(), 0, 23, 9, vf));

        assertEquals(new Integer(900000000), this.valueDecoder.decodeTime("14:34:01.9".getBytes(), 0, 10, 9, vf));
        assertEquals(new Integer(950000000), this.valueDecoder.decodeTime("14:34:01.95".getBytes(), 0, 11, 9, vf));
        assertEquals(new Integer(956000000), this.valueDecoder.decodeTime("14:34:01.956".getBytes(), 0, 12, 9, vf));
    }

    @Test
    public void testIntValues() {
        ValueFactory<String> vf = new StringValueFactory(new DefaultPropertySet());
        assertEquals(String.valueOf(Integer.MIN_VALUE), this.valueDecoder.decodeInt4(String.valueOf(Integer.MIN_VALUE).getBytes(), 0, 11, vf));
        assertEquals(String.valueOf(Integer.MAX_VALUE), this.valueDecoder.decodeInt4(String.valueOf(Integer.MAX_VALUE).getBytes(), 0, 10, vf));

        assertEquals(String.valueOf(Integer.MAX_VALUE), this.valueDecoder.decodeUInt4(String.valueOf(Integer.MAX_VALUE).getBytes(), 0, 10, vf));
        assertEquals("2147483648",
                this.valueDecoder.decodeUInt4(Constants.BIG_INTEGER_MAX_INTEGER_VALUE.add(Constants.BIG_INTEGER_ONE).toString().getBytes(), 0, 10, vf));

        assertThrows(NumberOutOfRange.class, () -> {
            this.valueDecoder.decodeInt4(Constants.BIG_INTEGER_MAX_INTEGER_VALUE.add(Constants.BIG_INTEGER_ONE).toString().getBytes(), 0, 10, vf);
        }, "Exception should be thrown for decodeInt4(Integer.MAX_VALUE + 1)");

        byte[] uint8LessThanMaxLong = "8223372036854775807".getBytes();
        ValueFactory<String> fromLongOnly = new DefaultValueFactory<String>(new DefaultPropertySet()) {

            @Override
            public String createFromLong(long l) {
                return Long.valueOf(l).toString();
            }

            @Override
            public String getTargetTypeName() {
                return null;
            }

            @Override
            public String createFromBytes(byte[] bytes, int offset, int length, Field f) {
                return null;
            }

        };
        assertEquals("8223372036854775807", this.valueDecoder.decodeUInt8(uint8LessThanMaxLong, 0, uint8LessThanMaxLong.length, fromLongOnly));
        byte[] uint8MoreThanMaxLong1 = "9223372036854775807".getBytes();
        byte[] uint8MoreThanMaxLong2 = "18223372036854775807".getBytes();
        assertEquals("9223372036854775807", this.valueDecoder.decodeUInt8(uint8MoreThanMaxLong1, 0, uint8MoreThanMaxLong1.length, vf));
        assertEquals("18223372036854775807", this.valueDecoder.decodeUInt8(uint8MoreThanMaxLong2, 0, uint8MoreThanMaxLong2.length, vf));
    }

    @Test
    public void testIsTime() {
        assertTrue(MysqlTextValueDecoder.isTime("10:00:00"));
        assertTrue(MysqlTextValueDecoder.isTime("100:00:00"));
        assertTrue(MysqlTextValueDecoder.isTime("-10:00:00"));
        assertTrue(MysqlTextValueDecoder.isTime("-100:00:00"));
        assertTrue(MysqlTextValueDecoder.isTime("10:00:00.1"));
        assertTrue(MysqlTextValueDecoder.isTime("100:00:00.12"));
        assertTrue(MysqlTextValueDecoder.isTime("-10:00:00.12345"));
        assertTrue(MysqlTextValueDecoder.isTime("-100:00:00.123456"));

        assertFalse(MysqlTextValueDecoder.isTime("10:000:00"));
        assertFalse(MysqlTextValueDecoder.isTime("10:00:000"));
        assertFalse(MysqlTextValueDecoder.isTime("1Z:00:00"));
        assertFalse(MysqlTextValueDecoder.isTime("10:Z0:00"));
        assertFalse(MysqlTextValueDecoder.isTime("10:00:Z0"));
        assertFalse(MysqlTextValueDecoder.isTime("10:00:00Z"));
        assertFalse(MysqlTextValueDecoder.isTime("+100:00:00"));
        assertFalse(MysqlTextValueDecoder.isTime("Z100:00:00"));
        assertFalse(MysqlTextValueDecoder.isTime("10:00:00.Z"));
        assertFalse(MysqlTextValueDecoder.isTime("10:00:00.12345Z"));
        assertFalse(MysqlTextValueDecoder.isTime("10:00:00.12345+01:00"));
    }

    @Test
    public void testIsTimestamp() {
        assertTrue(MysqlTextValueDecoder.isTimestamp("2004-01-01 10:00:00"));
        assertTrue(MysqlTextValueDecoder.isTimestamp("2004-01-01 10:00:00.1"));
        assertTrue(MysqlTextValueDecoder.isTimestamp("2004-01-01 10:00:00.123456789"));

        assertFalse(MysqlTextValueDecoder.isTimestamp("200-01-01 10:00:00"));
        assertFalse(MysqlTextValueDecoder.isTimestamp("20-01-01 10:00:00"));
        assertFalse(MysqlTextValueDecoder.isTimestamp("2000-01-01 100:00:00"));
        assertFalse(MysqlTextValueDecoder.isTimestamp("2000-01-01 10:000:00"));
        assertFalse(MysqlTextValueDecoder.isTimestamp("2000-01-01 10:00:000"));
        assertFalse(MysqlTextValueDecoder.isTimestamp("2000-01-01 100:00:00"));
        assertFalse(MysqlTextValueDecoder.isTimestamp("200-01-01 10:00:00.1"));
        assertFalse(MysqlTextValueDecoder.isTimestamp("20-01-01 10:00:00.1"));
        assertFalse(MysqlTextValueDecoder.isTimestamp("2000-01-01 100:00:00.1"));
        assertFalse(MysqlTextValueDecoder.isTimestamp("2000-01-01 10:000:00.1"));
        assertFalse(MysqlTextValueDecoder.isTimestamp("2000-01-01 10:00:000.1"));
        assertFalse(MysqlTextValueDecoder.isTimestamp("2000-01-01 100:00:00.1"));

        assertFalse(MysqlTextValueDecoder.isTimestamp("Z004-01-01 10:00:00"));
        assertFalse(MysqlTextValueDecoder.isTimestamp("2Z04-01-01 10:00:00"));
        assertFalse(MysqlTextValueDecoder.isTimestamp("20Z4-01-01 10:00:00"));
        assertFalse(MysqlTextValueDecoder.isTimestamp("200Z-01-01 10:00:00"));
        assertFalse(MysqlTextValueDecoder.isTimestamp("2004Z01-01 10:00:00"));
        assertFalse(MysqlTextValueDecoder.isTimestamp("2004-Z1-01 10:00:00"));
        assertFalse(MysqlTextValueDecoder.isTimestamp("2004-0Z-01 10:00:00"));
        assertFalse(MysqlTextValueDecoder.isTimestamp("2004-01Z01 10:00:00"));
        assertFalse(MysqlTextValueDecoder.isTimestamp("2004-01-Z1 10:00:00"));
        assertFalse(MysqlTextValueDecoder.isTimestamp("2004-01-0Z 10:00:00"));
        assertFalse(MysqlTextValueDecoder.isTimestamp("2004-01-01Z10:00:00"));
        assertFalse(MysqlTextValueDecoder.isTimestamp("2004-01-01 Z0:00:00"));
        assertFalse(MysqlTextValueDecoder.isTimestamp("2004-01-01 1Z:00:00"));
        assertFalse(MysqlTextValueDecoder.isTimestamp("2004-01-01 10Z00:00"));
        assertFalse(MysqlTextValueDecoder.isTimestamp("2004-01-01 10:Z0:00"));
        assertFalse(MysqlTextValueDecoder.isTimestamp("2004-01-01 10:0Z:00"));
        assertFalse(MysqlTextValueDecoder.isTimestamp("2004-01-01 10:00Z00"));
        assertFalse(MysqlTextValueDecoder.isTimestamp("2004-01-01 10:00:Z0"));
        assertFalse(MysqlTextValueDecoder.isTimestamp("2004-01-01 10:00:0Z"));
        assertFalse(MysqlTextValueDecoder.isTimestamp("2004-01-01 10:00:00Z"));
        assertFalse(MysqlTextValueDecoder.isTimestamp("2004-01-01 10:00:00+01:00"));
    }

}
