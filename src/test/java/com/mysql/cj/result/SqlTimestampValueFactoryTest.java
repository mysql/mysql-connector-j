/*
 * Copyright (c) 2015, 2020, Oracle and/or its affiliates.
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
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.TimeZone;
import java.util.concurrent.Callable;

import org.junit.jupiter.api.Test;

import com.mysql.cj.MysqlType;
import com.mysql.cj.conf.DefaultPropertySet;
import com.mysql.cj.conf.PropertyKey;
import com.mysql.cj.conf.PropertySet;
import com.mysql.cj.exceptions.DataConversionException;
import com.mysql.cj.exceptions.DataReadException;
import com.mysql.cj.exceptions.WrongArgumentException;
import com.mysql.cj.protocol.InternalDate;
import com.mysql.cj.protocol.InternalTime;
import com.mysql.cj.protocol.InternalTimestamp;

/**
 * Tests for JDBC {@link java.sql.Timestamp} creation.
 * <p/>
 * Note: Timestamp.toString() is not locale-specific and is appropriate for use in these tests.
 */
public class SqlTimestampValueFactoryTest extends CommonAsserts {
    PropertySet pset = new DefaultPropertySet();
    SqlTimestampValueFactory vf = new SqlTimestampValueFactory(this.pset, null, TimeZone.getDefault(), TimeZone.getDefault());

    @Test
    public void testBasics() {
        assertEquals("java.sql.Timestamp", this.vf.getTargetTypeName());
    }

    @Test
    public void testCreateFromDate() {
        Timestamp ts = this.vf.createFromDate(new InternalDate(2015, 5, 1)); // May 1st
        // verify a midnight on may 1st timestamp
        assertEquals("2015-05-01 00:00:00.0", ts.toString());
        assertEquals(Timestamp.valueOf(LocalDateTime.of(2018, 1, 1, 0, 0, 0, 0)), this.vf.createFromDate(new InternalDate(2018, 1, 1)));
    }

    @Test
    public void testCreateFromTime() {
        Timestamp ts = this.vf.createFromTime(new InternalTime(12, 20, 02, 4, 9));
        assertEquals("1970-01-01 12:20:02.000000004", ts.toString());
        assertEquals(Timestamp.valueOf(LocalDateTime.of(1970, 1, 1, 1, 1, 1, 1)), this.vf.createFromTime(new InternalTime(1, 1, 1, 1, 9)));

        assertThrows(DataReadException.class,
                "The value '-1:0:0' is an invalid TIME value. JDBC Time objects represent a wall-clock time and not a duration as MySQL treats them. If you are treating this type as a duration, consider retrieving this value as a string and dealing with it according to your requirements.",
                new Callable<Void>() {
                    @Override
                    public Void call() throws Exception {
                        SqlTimestampValueFactoryTest.this.vf.createFromTime(new InternalTime(-1, 0, 0, 0, 9));
                        return null;
                    }
                });

        assertThrows(DataReadException.class,
                "The value '44:0:0' is an invalid TIME value. JDBC Time objects represent a wall-clock time and not a duration as MySQL treats them. If you are treating this type as a duration, consider retrieving this value as a string and dealing with it according to your requirements.",
                new Callable<Void>() {
                    @Override
                    public Void call() throws Exception {
                        SqlTimestampValueFactoryTest.this.vf.createFromTime(new InternalTime(44, 0, 0, 0, 9));
                        return null;
                    }
                });
    }

    @Test
    public void testCreateFromTimestamp() {
        Timestamp ts = this.vf.createFromTimestamp(new InternalTimestamp(2015, 05, 01, 12, 20, 02, 4, 9));
        // should be the same (in system timezone)
        assertEquals("2015-05-01 12:20:02.000000004", ts.toString());
        assertEquals(Timestamp.valueOf(LocalDateTime.of(2018, 1, 1, 1, 1, 1, 1)),
                this.vf.createFromTimestamp(new InternalTimestamp(2018, 1, 1, 1, 1, 1, 1, 9)));

        assertThrows(DataReadException.class, "Zero date value prohibited", new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                SqlTimestampValueFactoryTest.this.vf.createFromTimestamp(new InternalTimestamp(0, 0, 0, 1, 1, 1, 1, 9));
                return null;
            }
        });
        assertThrows(WrongArgumentException.class, "YEAR", new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                SqlTimestampValueFactoryTest.this.vf.createFromTimestamp(new InternalTimestamp(0, 0, 1, 1, 1, 1, 1, 9));
                return null;
            }
        });
        assertThrows(WrongArgumentException.class, "YEAR", new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                SqlTimestampValueFactoryTest.this.vf.createFromTimestamp(new InternalTimestamp(0, 1, 0, 1, 1, 1, 1, 9));
                return null;
            }
        });
        assertThrows(WrongArgumentException.class, "YEAR", new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                SqlTimestampValueFactoryTest.this.vf.createFromTimestamp(new InternalTimestamp(0, 1, 1, 1, 1, 1, 1, 9));
                return null;
            }
        });

        assertThrows(WrongArgumentException.class, "MONTH", new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                SqlTimestampValueFactoryTest.this.vf.createFromTimestamp(new InternalTimestamp(2018, 0, 0, 1, 1, 1, 1, 9));
                return null;
            }
        });
        assertThrows(WrongArgumentException.class, "MONTH", new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                SqlTimestampValueFactoryTest.this.vf.createFromTimestamp(new InternalTimestamp(2018, 0, 1, 1, 1, 1, 1, 9));
                return null;
            }
        });

        assertThrows(WrongArgumentException.class, "DAY_OF_MONTH", new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                SqlTimestampValueFactoryTest.this.vf.createFromTimestamp(new InternalTimestamp(2018, 1, 0, 1, 1, 1, 1, 9));
                return null;
            }
        });
    }

    @Test
    public void testCreateFromLong() {
        assertThrows(DataConversionException.class, "Unsupported conversion from LONG to java.sql.Timestamp", new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                SqlTimestampValueFactoryTest.this.vf.createFromLong(22L);
                return null;
            }
        });
    }

    @Test
    public void testCreateFromBigInteger() {
        assertThrows(DataConversionException.class, "Unsupported conversion from BIGINT to java.sql.Timestamp", new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                SqlTimestampValueFactoryTest.this.vf.createFromBigInteger(new BigInteger("2018"));
                return null;
            }
        });
    }

    @Test
    public void testCreateFromDouble() {
        assertThrows(DataConversionException.class, "Unsupported conversion from DOUBLE to java.sql.Timestamp", new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                SqlTimestampValueFactoryTest.this.vf.createFromDouble(new Double(2018));
                return null;
            }
        });
    }

    @Test
    public void testCreateFromBigDecimal() {
        assertThrows(DataConversionException.class, "Unsupported conversion from DECIMAL to java.sql.Timestamp", new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                SqlTimestampValueFactoryTest.this.vf.createFromBigDecimal(new BigDecimal("2018"));
                return null;
            }
        });
    }

    @Test
    public void testCreateFromBytes() {

        Field f = new Field("test", "test", 33, "UTF-8", MysqlType.VARCHAR, 10);

        this.pset.getBooleanProperty(PropertyKey.emptyStringsConvertToZero).setValue(true);
        assertThrows(DataConversionException.class, "Unsupported conversion from LONG to java.sql.Timestamp", new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                SqlTimestampValueFactoryTest.this.vf.createFromBytes("".getBytes(), 0, 0, f);
                return null;
            }
        });

        this.pset.getBooleanProperty(PropertyKey.emptyStringsConvertToZero).setValue(false);
        assertThrows(DataConversionException.class, "Cannot convert string '' to java.sql.Timestamp value", new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                SqlTimestampValueFactoryTest.this.vf.createFromBytes("".getBytes(), 0, 0, f);
                return null;
            }
        });

        assertEquals(Timestamp.valueOf(LocalDateTime.of(2018, 1, 2, 3, 4, 5, 600000000)),
                this.vf.createFromBytes("2018-01-02 03:04:05.6".getBytes(), 0, 21, f));
        assertEquals(Timestamp.valueOf(LocalDateTime.of(2018, 1, 2, 0, 0, 0, 0)), this.vf.createFromBytes("2018-01-02".getBytes(), 0, 10, f));
        assertEquals(Timestamp.valueOf(LocalDateTime.of(1970, 1, 1, 3, 4, 5, 600000000)), this.vf.createFromBytes("03:04:05.6".getBytes(), 0, 10, f));

        assertThrows(DataConversionException.class, "Cannot convert string '1' to java.sql.Timestamp value", new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                SqlTimestampValueFactoryTest.this.vf.createFromBytes(new byte[] { '1' }, 0, 1, f);
                return null;
            }
        });

        assertThrows(DataConversionException.class, "Cannot convert string '-1.0' to java.sql.Timestamp value", new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                SqlTimestampValueFactoryTest.this.vf.createFromBytes("-1.0".getBytes(), 0, 4, f);
                return null;
            }
        });

        assertThrows(DataConversionException.class, "Cannot convert string 'just a string' to java.sql.Timestamp value", new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                SqlTimestampValueFactoryTest.this.vf.createFromBytes("just a string".getBytes(), 0, 13, f);
                return null;
            }
        });
    }

    @Test
    public void testCreateFromBit() {
        assertThrows(DataConversionException.class, "Unsupported conversion from BIT to java.sql.Timestamp", new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                SqlTimestampValueFactoryTest.this.vf.createFromBit(new byte[] { 1 }, 0, 2);
                return null;
            }
        });
    }

    @Test
    public void testCreateFromNull() {
        assertNull(this.vf.createFromNull());
    }
}
