/*
 * Copyright (c) 2018, 2020, Oracle and/or its affiliates.
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
import java.time.DateTimeException;
import java.time.LocalDateTime;
import java.util.concurrent.Callable;

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

public class LocalDateTimeValueFactoryTest extends CommonAsserts {
    PropertySet pset = new DefaultPropertySet();
    ValueFactory<LocalDateTime> vf = new LocalDateTimeValueFactory(this.pset);

    @Test
    public void testBasics() {
        assertEquals("java.time.LocalDateTime", this.vf.getTargetTypeName());
    }

    @Test
    public void testCreateFromDate() {
        assertEquals(LocalDateTime.of(2018, 1, 1, 0, 0, 0, 0), this.vf.createFromDate(new InternalDate(2018, 1, 1)));
    }

    @Test
    public void testCreateFromTime() {
        assertThrows(DataReadException.class,
                "The value '-1:0:0' is an invalid TIME value. JDBC Time objects represent a wall-clock time and not a duration as MySQL treats them. If you are treating this type as a duration, consider retrieving this value as a string and dealing with it according to your requirements.",
                new Callable<Void>() {
                    @Override
                    public Void call() throws Exception {
                        LocalDateTimeValueFactoryTest.this.vf.createFromTime(new InternalTime(-1, 0, 0, 0, 0));
                        return null;
                    }
                });

        assertThrows(DataReadException.class,
                "The value '44:0:0' is an invalid TIME value. JDBC Time objects represent a wall-clock time and not a duration as MySQL treats them. If you are treating this type as a duration, consider retrieving this value as a string and dealing with it according to your requirements.",
                new Callable<Void>() {
                    @Override
                    public Void call() throws Exception {
                        LocalDateTimeValueFactoryTest.this.vf.createFromTime(new InternalTime(44, 0, 0, 0, 0));
                        return null;
                    }
                });

        assertEquals(LocalDateTime.of(1970, 1, 1, 1, 1, 1, 1), this.vf.createFromTime(new InternalTime(1, 1, 1, 1, 9)));
    }

    @Test
    public void testCreateFromTimestamp() {
        assertThrows(DataReadException.class, "Zero date value prohibited", new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                LocalDateTimeValueFactoryTest.this.vf.createFromTimestamp(new InternalTimestamp(0, 0, 0, 1, 1, 1, 1, 9));
                return null;
            }
        });
        assertThrows(DateTimeException.class, "Invalid value for MonthOfYear \\(valid values 1 - 12\\): 0", new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                LocalDateTimeValueFactoryTest.this.vf.createFromTimestamp(new InternalTimestamp(0, 0, 1, 1, 1, 1, 1, 9));
                return null;
            }
        });
        assertThrows(DateTimeException.class, "Invalid value for DayOfMonth \\(valid values 1 - 28/31\\): 0", new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                LocalDateTimeValueFactoryTest.this.vf.createFromTimestamp(new InternalTimestamp(0, 1, 0, 1, 1, 1, 1, 9));
                return null;
            }
        });

        assertEquals(LocalDateTime.of(0, 1, 1, 1, 1, 1, 1), this.vf.createFromTimestamp(new InternalTimestamp(0, 1, 1, 1, 1, 1, 1, 9)));

        assertThrows(DateTimeException.class, "Invalid value for MonthOfYear \\(valid values 1 - 12\\): 0", new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                LocalDateTimeValueFactoryTest.this.vf.createFromTimestamp(new InternalTimestamp(2018, 0, 0, 1, 1, 1, 1, 9));
                return null;
            }
        });
        assertThrows(DateTimeException.class, "Invalid value for MonthOfYear \\(valid values 1 - 12\\): 0", new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                LocalDateTimeValueFactoryTest.this.vf.createFromTimestamp(new InternalTimestamp(2018, 0, 1, 1, 1, 1, 1, 9));
                return null;
            }
        });
        assertThrows(DateTimeException.class, "Invalid value for DayOfMonth \\(valid values 1 - 28/31\\): 0", new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                LocalDateTimeValueFactoryTest.this.vf.createFromTimestamp(new InternalTimestamp(2018, 1, 0, 1, 1, 1, 1, 9));
                return null;
            }
        });
        assertEquals(LocalDateTime.of(2018, 1, 1, 1, 1, 1, 1), this.vf.createFromTimestamp(new InternalTimestamp(2018, 1, 1, 1, 1, 1, 1, 9)));
    }

    @Test
    public void testCreateFromLong() {
        assertThrows(DataConversionException.class, "Unsupported conversion from LONG to java.time.LocalDateTime", new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                LocalDateTimeValueFactoryTest.this.vf.createFromLong(22L);
                return null;
            }
        });
    }

    @Test
    public void testCreateFromBigInteger() {
        assertThrows(DataConversionException.class, "Unsupported conversion from BIGINT to java.time.LocalDateTime", new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                LocalDateTimeValueFactoryTest.this.vf.createFromBigInteger(new BigInteger("2018"));
                return null;
            }
        });
    }

    @Test
    public void testCreateFromDouble() {
        assertThrows(DataConversionException.class, "Unsupported conversion from DOUBLE to java.time.LocalDateTime", new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                LocalDateTimeValueFactoryTest.this.vf.createFromDouble(new Double(2018));
                return null;
            }
        });
    }

    @Test
    public void testCreateFromBigDecimal() {
        assertThrows(DataConversionException.class, "Unsupported conversion from DECIMAL to java.time.LocalDateTime", new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                LocalDateTimeValueFactoryTest.this.vf.createFromBigDecimal(new BigDecimal("2018"));
                return null;
            }
        });
    }

    @Test
    public void testCreateFromBytes() {

        Field f = new Field("test", "test", 33, "UTF-8", MysqlType.VARCHAR, 10);

        this.pset.getBooleanProperty(PropertyKey.emptyStringsConvertToZero).setValue(true);
        assertThrows(DataConversionException.class, "Unsupported conversion from LONG to java.time.LocalDateTime", new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                LocalDateTimeValueFactoryTest.this.vf.createFromBytes("".getBytes(), 0, 0, f);
                return null;
            }
        });

        this.pset.getBooleanProperty(PropertyKey.emptyStringsConvertToZero).setValue(false);
        assertThrows(DataConversionException.class, "Cannot convert string '' to java.time.LocalDateTime value", new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                LocalDateTimeValueFactoryTest.this.vf.createFromBytes("".getBytes(), 0, 0, f);
                return null;
            }
        });

        assertEquals(LocalDateTime.of(2018, 1, 2, 3, 4, 5, 600000000), this.vf.createFromBytes("2018-01-02 03:04:05.6".getBytes(), 0, 21, f));
        assertEquals(LocalDateTime.of(2018, 1, 2, 0, 0, 0, 0), this.vf.createFromBytes("2018-01-02".getBytes(), 0, 10, f));
        assertEquals(LocalDateTime.of(1970, 1, 1, 3, 4, 5, 600000000), this.vf.createFromBytes("03:04:05.6".getBytes(), 0, 10, f));

        assertThrows(DataConversionException.class, "Cannot convert string '1' to java.time.LocalDateTime value", new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                LocalDateTimeValueFactoryTest.this.vf.createFromBytes(new byte[] { '1' }, 0, 1, f);
                return null;
            }
        });

        assertThrows(DataConversionException.class, "Cannot convert string '-1.0' to java.time.LocalDateTime value", new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                LocalDateTimeValueFactoryTest.this.vf.createFromBytes("-1.0".getBytes(), 0, 4, f);
                return null;
            }
        });

        assertThrows(DataConversionException.class, "Cannot convert string 'just a string' to java.time.LocalDateTime value", new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                LocalDateTimeValueFactoryTest.this.vf.createFromBytes("just a string".getBytes(), 0, 13, f);
                return null;
            }
        });
    }

    @Test
    public void testCreateFromBit() {
        assertThrows(DataConversionException.class, "Unsupported conversion from BIT to java.time.LocalDateTime", new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                LocalDateTimeValueFactoryTest.this.vf.createFromBit(new byte[] { 1 }, 0, 2);
                return null;
            }
        });
    }

    @Test
    public void testCreateFromNull() {
        assertNull(this.vf.createFromNull());
    }
}
