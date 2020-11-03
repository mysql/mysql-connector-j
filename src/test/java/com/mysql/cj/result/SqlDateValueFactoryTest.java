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
import java.sql.Date;
import java.time.LocalDate;
import java.util.TimeZone;
import java.util.concurrent.Callable;

import org.junit.jupiter.api.Test;

import com.mysql.cj.MysqlType;
import com.mysql.cj.conf.DefaultPropertySet;
import com.mysql.cj.conf.PropertyKey;
import com.mysql.cj.conf.PropertySet;
import com.mysql.cj.exceptions.DataConversionException;
import com.mysql.cj.exceptions.WrongArgumentException;
import com.mysql.cj.protocol.InternalDate;
import com.mysql.cj.protocol.InternalTime;
import com.mysql.cj.protocol.InternalTimestamp;

/**
 * Tests for JDBC {@link java.sql.Date} creation.
 */
public class SqlDateValueFactoryTest extends CommonAsserts {
    PropertySet pset = new DefaultPropertySet();
    SqlDateValueFactory vf = new SqlDateValueFactory(this.pset, null, TimeZone.getDefault());

    @Test
    public void testBasics() {
        assertEquals("java.sql.Date", this.vf.getTargetTypeName());
    }

    @Test
    public void testCreateFromDate() {
        assertEquals("2015-05-01", this.vf.createFromDate(new InternalDate(2015, 5, 1)).toString()); // May 1st
        assertEquals(Date.valueOf(LocalDate.of(2018, 1, 1)), this.vf.createFromDate(new InternalDate(2018, 1, 1)));
    }

    @Test
    public void testCreateFromTime() {
        assertEquals("1970-01-01", this.vf.createFromTime(new InternalTime(12, 20, 02, 4, 9)).toString());
        assertEquals("1970-01-01", this.vf.createFromTime(new InternalTime(1, 1, 1, 1, 9)).toString());
        assertEquals("1970-01-01", this.vf.createFromTime(new InternalTime(-1, 1, 1, 1, 9)).toString());
        assertEquals("1970-01-01", this.vf.createFromTime(new InternalTime(48, 1, 1, 1, 9)).toString());
    }

    @Test
    public void testCreateFromTimestamp() {
        Date d = this.vf.createFromTimestamp(new InternalTimestamp(2015, 05, 01, 12, 20, 02, 4, 9));
        // should be the same (in system timezone)
        assertEquals("2015-05-01", d.toString());
        assertEquals(Date.valueOf(LocalDate.of(2018, 1, 1)), this.vf.createFromTimestamp(new InternalTimestamp(2018, 1, 1, 1, 1, 1, 1, 9)));

        assertThrows(WrongArgumentException.class, "YEAR", new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                SqlDateValueFactoryTest.this.vf.createFromTimestamp(new InternalTimestamp(0, 0, 0, 1, 1, 1, 1, 9));
                return null;
            }
        });
        assertThrows(WrongArgumentException.class, "YEAR", new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                SqlDateValueFactoryTest.this.vf.createFromTimestamp(new InternalTimestamp(0, 0, 1, 1, 1, 1, 1, 9));
                return null;
            }
        });
        assertThrows(WrongArgumentException.class, "YEAR", new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                SqlDateValueFactoryTest.this.vf.createFromTimestamp(new InternalTimestamp(0, 1, 0, 1, 1, 1, 1, 9));
                return null;
            }
        });
        assertThrows(WrongArgumentException.class, "YEAR", new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                SqlDateValueFactoryTest.this.vf.createFromTimestamp(new InternalTimestamp(0, 1, 1, 1, 1, 1, 1, 9));
                return null;
            }
        });

        assertThrows(WrongArgumentException.class, "MONTH", new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                SqlDateValueFactoryTest.this.vf.createFromTimestamp(new InternalTimestamp(2018, 0, 0, 1, 1, 1, 1, 9));
                return null;
            }
        });
        assertThrows(WrongArgumentException.class, "MONTH", new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                SqlDateValueFactoryTest.this.vf.createFromTimestamp(new InternalTimestamp(2018, 0, 1, 1, 1, 1, 1, 9));
                return null;
            }
        });

        assertThrows(WrongArgumentException.class, "DAY_OF_MONTH", new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                SqlDateValueFactoryTest.this.vf.createFromTimestamp(new InternalTimestamp(2018, 1, 0, 1, 1, 1, 1, 9));
                return null;
            }
        });
    }

    @Test
    public void testCreateFromLong() {
        assertThrows(DataConversionException.class, "Unsupported conversion from LONG to java.sql.Date", new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                SqlDateValueFactoryTest.this.vf.createFromLong(22L);
                return null;
            }
        });
    }

    @Test
    public void testCreateFromBigInteger() {
        assertThrows(DataConversionException.class, "Unsupported conversion from BIGINT to java.sql.Date", new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                SqlDateValueFactoryTest.this.vf.createFromBigInteger(new BigInteger("2018"));
                return null;
            }
        });
    }

    @Test
    public void testCreateFromDouble() {
        assertThrows(DataConversionException.class, "Unsupported conversion from DOUBLE to java.sql.Date", new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                SqlDateValueFactoryTest.this.vf.createFromDouble(new Double(2018));
                return null;
            }
        });
    }

    @Test
    public void testCreateFromBigDecimal() {
        assertThrows(DataConversionException.class, "Unsupported conversion from DECIMAL to java.sql.Date", new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                SqlDateValueFactoryTest.this.vf.createFromBigDecimal(new BigDecimal("2018"));
                return null;
            }
        });
    }

    @Test
    public void testCreateFromBytes() {

        Field f = new Field("test", "test", 33, "UTF-8", MysqlType.VARCHAR, 10);

        this.pset.getBooleanProperty(PropertyKey.emptyStringsConvertToZero).setValue(true);
        assertThrows(DataConversionException.class, "Unsupported conversion from LONG to java.sql.Date", new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                SqlDateValueFactoryTest.this.vf.createFromBytes("".getBytes(), 0, 0, f);
                return null;
            }
        });

        this.pset.getBooleanProperty(PropertyKey.emptyStringsConvertToZero).setValue(false);
        assertThrows(DataConversionException.class, "Cannot convert string '' to java.sql.Date value", new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                SqlDateValueFactoryTest.this.vf.createFromBytes("".getBytes(), 0, 0, f);
                return null;
            }
        });

        assertEquals(Date.valueOf(LocalDate.of(2018, 1, 2)), this.vf.createFromBytes("2018-01-02 03:04:05.6".getBytes(), 0, 21, f));
        assertEquals(Date.valueOf(LocalDate.of(2018, 1, 2)), this.vf.createFromBytes("2018-01-02".getBytes(), 0, 10, f));
        assertEquals(Date.valueOf(LocalDate.of(1970, 1, 1)).toString(), this.vf.createFromBytes("03:04:05.6".getBytes(), 0, 10, f).toString());

        assertThrows(DataConversionException.class, "Cannot convert string '1' to java.sql.Date value", new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                SqlDateValueFactoryTest.this.vf.createFromBytes(new byte[] { '1' }, 0, 1, f);
                return null;
            }
        });

        assertThrows(DataConversionException.class, "Cannot convert string '-1.0' to java.sql.Date value", new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                SqlDateValueFactoryTest.this.vf.createFromBytes("-1.0".getBytes(), 0, 4, f);
                return null;
            }
        });

        assertThrows(DataConversionException.class, "Cannot convert string 'just a string' to java.sql.Date value", new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                SqlDateValueFactoryTest.this.vf.createFromBytes("just a string".getBytes(), 0, 13, f);
                return null;
            }
        });
    }

    @Test
    public void testCreateFromBit() {
        assertThrows(DataConversionException.class, "Unsupported conversion from BIT to java.sql.Date", new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                SqlDateValueFactoryTest.this.vf.createFromBit(new byte[] { 1 }, 0, 2);
                return null;
            }
        });
    }

    @Test
    public void testCreateFromNull() {
        assertNull(this.vf.createFromNull());
    }
}
