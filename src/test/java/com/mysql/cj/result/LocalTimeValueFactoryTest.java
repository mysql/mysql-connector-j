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
import java.math.BigInteger;
import java.time.LocalTime;

import org.junit.jupiter.api.Test;

import com.mysql.cj.MysqlType;
import com.mysql.cj.conf.DefaultPropertySet;
import com.mysql.cj.conf.PropertyKey;
import com.mysql.cj.conf.PropertySet;
import com.mysql.cj.exceptions.DataConversionException;
import com.mysql.cj.exceptions.DataReadException;
import com.mysql.cj.protocol.InternalDate;
import com.mysql.cj.protocol.InternalTime;
import com.mysql.cj.protocol.InternalTimestamp;

public class LocalTimeValueFactoryTest extends CommonAsserts {

    @Test
    public void testBasics() {
        assertEquals("java.time.LocalTime", this.vf.getTargetTypeName());
    }

    PropertySet pset = new DefaultPropertySet();
    LocalTimeValueFactory vf = new LocalTimeValueFactory(this.pset,
            warning -> assertEquals("Precision lost converting DATETIME/TIMESTAMP to java.time.LocalTime", warning));

    @Test
    public void testCreateFromDate() {
        assertEquals(LocalTime.of(0, 0), this.vf.createFromDate(new InternalDate(2018, 1, 1)));
    }

    @Test
    public void testCreateFromTime() {
        assertEquals(LocalTime.of(1, 1, 1, 1), this.vf.createFromTime(new InternalTime(1, 1, 1, 1, 9)));

        assertThrows(DataReadException.class,
                "The value '-1:00:00' is an invalid TIME value. JDBC Time objects represent a wall-clock time and not a duration as MySQL treats them. If you are treating this type as a duration, consider retrieving this value as a string and dealing with it according to your requirements.",
                () -> {
                    LocalTimeValueFactoryTest.this.vf.createFromTime(new InternalTime(-1, 0, 0, 0, 9));
                    return null;
                });

        assertThrows(DataReadException.class,
                "The value '44:00:00' is an invalid TIME value. JDBC Time objects represent a wall-clock time and not a duration as MySQL treats them. If you are treating this type as a duration, consider retrieving this value as a string and dealing with it according to your requirements.",
                () -> {
                    LocalTimeValueFactoryTest.this.vf.createFromTime(new InternalTime(44, 0, 0, 0, 9));
                    return null;
                });
    }

    @Test
    public void testCreateFromTimestamp() {
        assertEquals(LocalTime.of(1, 1, 1, 1), this.vf.createFromTimestamp(new InternalTimestamp(2018, 1, 1, 1, 1, 1, 1, 9)));
        assertEquals(LocalTime.of(1, 1, 1, 1), this.vf.createFromTimestamp(new InternalTimestamp(0, 0, 0, 1, 1, 1, 1, 9)));
        assertEquals(LocalTime.of(1, 1, 1, 1), this.vf.createFromTimestamp(new InternalTimestamp(0, 0, 1, 1, 1, 1, 1, 9)));
        assertEquals(LocalTime.of(1, 1, 1, 1), this.vf.createFromTimestamp(new InternalTimestamp(0, 1, 1, 1, 1, 1, 1, 9)));
    }

    @Test
    public void testCreateFromLong() {
        assertThrows(DataConversionException.class, "Unsupported conversion from LONG to java.time.LocalTime", () -> {
            LocalTimeValueFactoryTest.this.vf.createFromLong(22L);
            return null;
        });
    }

    @Test
    public void testCreateFromBigInteger() {
        assertThrows(DataConversionException.class, "Unsupported conversion from BIGINT to java.time.LocalTime", () -> {
            LocalTimeValueFactoryTest.this.vf.createFromBigInteger(new BigInteger("2018"));
            return null;
        });
    }

    @Test
    public void testCreateFromDouble() {
        assertThrows(DataConversionException.class, "Unsupported conversion from DOUBLE to java.time.LocalTime", () -> {
            LocalTimeValueFactoryTest.this.vf.createFromDouble(new Double(2018));
            return null;
        });
    }

    @Test
    public void testCreateFromBigDecimal() {
        assertThrows(DataConversionException.class, "Unsupported conversion from DECIMAL to java.time.LocalTime", () -> {
            LocalTimeValueFactoryTest.this.vf.createFromBigDecimal(new BigDecimal("2018"));
            return null;
        });
    }

    @Test
    public void testCreateFromBytes() {
        assertThrows(DataConversionException.class, "Cannot convert string '1' to java.time.LocalTime value", () -> {
            Field f = new Field("test", "test", 33, "UTF-8", MysqlType.VARCHAR, 2);
            LocalTimeValueFactoryTest.this.vf.createFromBytes(new byte[] { '1' }, 0, 1, f);
            return null;
        });

        Field f = new Field("test", "test", 33, "UTF-8", MysqlType.VARCHAR, 10);

        this.pset.getBooleanProperty(PropertyKey.emptyStringsConvertToZero).setValue(true);
        assertThrows(DataConversionException.class, "Unsupported conversion from LONG to java.time.LocalTime", () -> {
            LocalTimeValueFactoryTest.this.vf.createFromBytes("".getBytes(), 0, 0, f);
            return null;
        });

        this.pset.getBooleanProperty(PropertyKey.emptyStringsConvertToZero).setValue(false);
        assertThrows(DataConversionException.class, "Cannot convert string '' to java.time.LocalTime value", () -> {
            LocalTimeValueFactoryTest.this.vf.createFromBytes("".getBytes(), 0, 0, f);
            return null;
        });

        assertEquals(LocalTime.of(3, 4, 5, 600000000), this.vf.createFromBytes("2018-01-02 03:04:05.6".getBytes(), 0, 21, f));
        assertEquals(LocalTime.of(3, 4, 5, 600000000), this.vf.createFromBytes("03:04:05.6".getBytes(), 0, 10, f));
        assertEquals(LocalTime.of(0, 0), this.vf.createFromBytes("2018-01-02".getBytes(), 0, 10, f));

        assertThrows(DataConversionException.class, "Cannot convert string '1' to java.time.LocalTime value", () -> {
            LocalTimeValueFactoryTest.this.vf.createFromBytes(new byte[] { '1' }, 0, 1, f);
            return null;
        });

        assertThrows(DataConversionException.class, "Cannot convert string '-1.0' to java.time.LocalTime value", () -> {
            LocalTimeValueFactoryTest.this.vf.createFromBytes("-1.0".getBytes(), 0, 4, f);
            return null;
        });

        assertThrows(DataConversionException.class, "Cannot convert string 'just a string' to java.time.LocalTime value", () -> {
            LocalTimeValueFactoryTest.this.vf.createFromBytes("just a string".getBytes(), 0, 13, f);
            return null;
        });
    }

    @Test
    public void testCreateFromBit() {
        assertThrows(DataConversionException.class, "Unsupported conversion from BIT to java.time.LocalTime", () -> {
            LocalTimeValueFactoryTest.this.vf.createFromBit(new byte[] { 1 }, 0, 2);
            return null;
        });
    }

    @Test
    public void testCreateFromNull() {
        assertNull(this.vf.createFromNull());
    }

}
