/*
 * Copyright (c) 2018, Oracle and/or its affiliates. All rights reserved.
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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.time.DateTimeException;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.concurrent.Callable;

import org.junit.Test;

import com.mysql.cj.exceptions.DataReadException;
import com.mysql.cj.result.LocalDateTimeValueFactory;
import com.mysql.cj.result.ZeroDateTimeToNullValueFactory;

public class ZeroDateTimeToNullValueFactoryTest extends CommonAsserts {

    @Test
    public void testBasics() {
        ZeroDateTimeToNullValueFactory<LocalDateTime> vf = new ZeroDateTimeToNullValueFactory<>(new LocalDateTimeValueFactory());

        assertNull(vf.createFromDate(0, 0, 0));
        assertEquals(LocalDateTime.of(2018, 1, 1, 0, 0, 0, 0), vf.createFromDate(2018, 1, 1));

        assertNotNull(vf.createFromTime(0, 0, 0, 0));
        assertEquals(LocalDateTime.ofEpochSecond(0, 0, ZoneOffset.UTC), vf.createFromTime(0, 0, 0, 0));

        assertThrows(DataReadException.class,
                "The value '-1:0:0' is an invalid TIME value. JDBC Time objects represent a wall-clock time and not a duration as MySQL treats them. If you are treating this type as a duration, consider retrieving this value as a string and dealing with it according to your requirements.",
                new Callable<Void>() {
                    @Override
                    public Void call() throws Exception {
                        vf.createFromTime(-1, 0, 0, 0);
                        return null;
                    }
                });

        assertThrows(DataReadException.class,
                "The value '44:0:0' is an invalid TIME value. JDBC Time objects represent a wall-clock time and not a duration as MySQL treats them. If you are treating this type as a duration, consider retrieving this value as a string and dealing with it according to your requirements.",
                new Callable<Void>() {
                    @Override
                    public Void call() throws Exception {
                        vf.createFromTime(44, 0, 0, 0);
                        return null;
                    }
                });

        assertEquals(LocalDateTime.of(1970, 1, 1, 1, 1, 1, 1), vf.createFromTime(1, 1, 1, 1));

        assertNull(vf.createFromTimestamp(0, 0, 0, 0, 0, 0, 0));

        assertThrows(DataReadException.class, "Zero date value prohibited", new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                vf.createFromTimestamp(0, 0, 0, 1, 1, 1, 1);
                return null;
            }
        });
        assertThrows(DateTimeException.class, "Invalid value for MonthOfYear \\(valid values 1 - 12\\): 0", new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                assertEquals(LocalDateTime.of(0, 0, 1, 1, 1, 1, 1), vf.createFromTimestamp(0, 0, 1, 1, 1, 1, 1));
                return null;
            }
        });
        assertThrows(DateTimeException.class, "Invalid value for DayOfMonth \\(valid values 1 - 28/31\\): 0", new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                assertEquals(LocalDateTime.of(0, 1, 0, 1, 1, 1, 1), vf.createFromTimestamp(0, 1, 0, 1, 1, 1, 1));
                return null;
            }
        });

        assertEquals(LocalDateTime.of(0, 1, 1, 1, 1, 1, 1), vf.createFromTimestamp(0, 1, 1, 1, 1, 1, 1));

        assertThrows(DateTimeException.class, "Invalid value for MonthOfYear \\(valid values 1 - 12\\): 0", new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                assertEquals(LocalDateTime.of(2018, 0, 0, 1, 1, 1, 1), vf.createFromTimestamp(2018, 0, 0, 1, 1, 1, 1));
                return null;
            }
        });
        assertThrows(DateTimeException.class, "Invalid value for MonthOfYear \\(valid values 1 - 12\\): 0", new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                assertEquals(LocalDateTime.of(2018, 0, 1, 1, 1, 1, 1), vf.createFromTimestamp(2018, 0, 1, 1, 1, 1, 1));
                return null;
            }
        });
        assertThrows(DateTimeException.class, "Invalid value for DayOfMonth \\(valid values 1 - 28/31\\): 0", new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                assertEquals(LocalDateTime.of(2018, 1, 0, 1, 1, 1, 1), vf.createFromTimestamp(2018, 1, 0, 1, 1, 1, 1));
                return null;
            }
        });
        assertEquals(LocalDateTime.of(2018, 1, 1, 1, 1, 1, 1), vf.createFromTimestamp(2018, 1, 1, 1, 1, 1, 1));

        assertEquals("java.time.LocalDateTime", vf.getTargetTypeName());
    }
}
