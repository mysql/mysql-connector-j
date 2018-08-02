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

import java.sql.Timestamp;
import java.util.TimeZone;

import org.junit.Test;

import com.mysql.cj.result.SqlTimestampValueFactory;

/**
 * Tests for JDBC {@link java.sql.Timestamp} creation.
 * <p/>
 * Note: Timestamp.toString() is not locale-specific and is appropriate for use in these tests.
 */
public class SqlTimestampValueFactoryTest {
    /**
     * Test basic timestamp creation.
     */
    @Test
    public void testBasicTimestamp() {
        SqlTimestampValueFactory vf = new SqlTimestampValueFactory(null, TimeZone.getDefault());
        Timestamp ts = vf.createFromTimestamp(2015, 05, 01, 12, 20, 02, 4);
        // should be the same (in system timezone)
        assertEquals("2015-05-01 12:20:02.000000004", ts.toString());
    }

    /**
     * Test that the default date (1970-01-01) is correctly assigned to the timestamp when only the time is given.
     */
    @Test
    public void testTimestampFromTime() {
        SqlTimestampValueFactory vf = new SqlTimestampValueFactory(null, TimeZone.getDefault());
        Timestamp ts = vf.createFromTime(12, 20, 02, 4);
        assertEquals("1970-01-01 12:20:02.000000004", ts.toString());
    }

    /**
     *
     */
    @Test
    public void testTimestampFromDate() {
        SqlTimestampValueFactory vf = new SqlTimestampValueFactory(null, TimeZone.getDefault());
        Timestamp ts = vf.createFromDate(2015, 5, 1); // May 1st
        // verify a midnight on may 1st timestamp
        assertEquals("2015-05-01 00:00:00.0", ts.toString());
    }
}
