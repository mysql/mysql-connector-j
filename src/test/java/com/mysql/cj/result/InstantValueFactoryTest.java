/*
 * Copyright (c) 2018, 2021, Oracle and/or its affiliates.
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

import java.time.Instant;
import java.time.ZoneId;
import java.util.concurrent.Callable;

import com.mysql.cj.conf.DefaultPropertySet;
import com.mysql.cj.conf.PropertySet;
import com.mysql.cj.exceptions.DataConversionException;
import com.mysql.cj.protocol.InternalDate;
import com.mysql.cj.protocol.InternalTime;
import com.mysql.cj.protocol.InternalTimestamp;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

public class InstantValueFactoryTest extends CommonAsserts {
    PropertySet pset = new DefaultPropertySet();
    ValueFactory<Instant> vf = new InstantValueFactory(this.pset, ZoneId.of("UTC"), ZoneId.of("UTC"));

    @Test
    void testBasics() {
        assertEquals("java.time.Instant", vf.getTargetTypeName());
    }

    @Test
    void testCreateFromDate() {
        assertEquals(Instant.parse("2022-03-22T00:00:00Z"), vf.createFromDate(new InternalDate(2022, 3, 22)));
    }

    @Test
    void testCreateFromTime() {
        assertEquals(Instant.parse("1970-01-01T15:31:23Z"), vf.createFromTime(new InternalTime(15, 31, 23, 0, 0)));
    }

    @Test
    void testCreateFromTimestamp() {
        assertEquals(Instant.parse("2022-03-22T15:31:23Z"), vf.createFromTimestamp(new InternalTimestamp(2022, 3, 22, 15, 31, 23, 0, 0)));
    }

    @Test
    void testCreateFromLong() {
        Instant instant = Instant.parse("2022-03-22T15:31:23.123Z");
        assertEquals(instant, vf.createFromLong(instant.toEpochMilli()));
    }

    @Test
    public void testCreateFromDouble() {
        assertThrows(DataConversionException.class, "Unsupported conversion from DOUBLE to java.time.Instant", (Callable<Void>) () -> {
            vf.createFromDouble(2018.0);
            return null;
        });
    }

    @Test
    public void testCreateFromNull() {
        assertNull(this.vf.createFromNull());
    }
}