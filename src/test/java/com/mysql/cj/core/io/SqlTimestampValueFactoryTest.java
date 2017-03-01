/*
  Copyright (c) 2015, 2017, Oracle and/or its affiliates. All rights reserved.

  The MySQL Connector/J is licensed under the terms of the GPLv2
  <http://www.gnu.org/licenses/old-licenses/gpl-2.0.html>, like most MySQL Connectors.
  There are special exceptions to the terms and conditions of the GPLv2 as it is applied to
  this software, see the FOSS License Exception
  <http://www.mysql.com/about/legal/licensing/foss-exception.html>.

  This program is free software; you can redistribute it and/or modify it under the terms
  of the GNU General Public License as published by the Free Software Foundation; version 2
  of the License.

  This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
  without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
  See the GNU General Public License for more details.

  You should have received a copy of the GNU General Public License along with this
  program; if not, write to the Free Software Foundation, Inc., 51 Franklin St, Fifth
  Floor, Boston, MA 02110-1301  USA

 */

package com.mysql.cj.core.io;

import static org.junit.Assert.assertEquals;

import java.sql.Timestamp;
import java.util.TimeZone;

import org.junit.Test;

import com.mysql.cj.core.io.SqlTimestampValueFactory;

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
        SqlTimestampValueFactory vf = new SqlTimestampValueFactory(TimeZone.getDefault());
        Timestamp ts = vf.createFromTimestamp(2015, 05, 01, 12, 20, 02, 4);
        // should be the same (in system timezone)
        assertEquals("2015-05-01 12:20:02.000000004", ts.toString());
    }

    /**
     * Test that the default date (1970-01-01) is correctly assigned to the timestamp when only the time is given.
     */
    @Test
    public void testTimestampFromTime() {
        SqlTimestampValueFactory vf = new SqlTimestampValueFactory(TimeZone.getDefault());
        Timestamp ts = vf.createFromTime(12, 20, 02, 4);
        assertEquals("1970-01-01 12:20:02.000000004", ts.toString());
    }

    /**
     *
     */
    @Test
    public void testTimestampFromDate() {
        SqlTimestampValueFactory vf = new SqlTimestampValueFactory(TimeZone.getDefault());
        Timestamp ts = vf.createFromDate(2015, 5, 1); // May 1st
        // verify a midnight on may 1st timestamp
        assertEquals("2015-05-01 00:00:00.0", ts.toString());
    }
}
