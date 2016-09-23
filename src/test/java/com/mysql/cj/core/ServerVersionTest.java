/*
  Copyright (c) 2015, 2016, Oracle and/or its affiliates. All rights reserved.

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

package com.mysql.cj.core;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

/**
 * Tests for ServerVersion.
 */
public class ServerVersionTest {
    @Test
    public void testNumericVersionToString() {
        ServerVersion v = new ServerVersion(12, 34, 67);
        assertEquals("12.34.67", v.toString());
    }

    @Test
    public void testParsedVersion() {
        String versionString = "5.6.14-enterprise-commercial-advanced";
        ServerVersion v = ServerVersion.parseVersion(versionString);
        assertEquals(5, v.getMajor());
        assertEquals(6, v.getMinor());
        assertEquals(14, v.getSubminor());
        assertEquals(versionString, v.toString());

        versionString = "5.7.5-m15-enterprise-commercial-advanced";
        v = ServerVersion.parseVersion(versionString);
        assertEquals(5, v.getMajor());
        assertEquals(7, v.getMinor());
        assertEquals(5, v.getSubminor());
        assertEquals(versionString, v.toString());
    }

    @Test
    public void testMeetsMinimum() {
        ServerVersion testVersion = new ServerVersion(5, 7, 5);

        // check versions where the test version meets the minimum
        String[] shouldMeet = new String[] { "5.6.1", "5.6.20", "5.7.1", "5.7.5", "0.0.0" };
        for (String min : shouldMeet) {
            assertTrue(testVersion.meetsMinimum(ServerVersion.parseVersion(min)));
        }

        // check versions where the test version does NOT meet the minimum
        String[] shouldntMeet = new String[] { "5.7.6", "5.8.0", "6.0.0", "99.99.99" };
        for (String min : shouldntMeet) {
            assertFalse(testVersion.meetsMinimum(ServerVersion.parseVersion(min)));
        }
    }

    @Test
    public void testNotParsable() {
        ServerVersion v = ServerVersion.parseVersion("something that's not mysql server");
        assertEquals("0.0.0", v.toString());
    }

    @Test
    public void testComparison() {
        ServerVersion v100 = new ServerVersion(1, 0, 0);
        ServerVersion v101 = new ServerVersion(1, 0, 1);
        ServerVersion v200 = new ServerVersion(2, 0, 0);
        ServerVersion v202 = new ServerVersion(2, 0, 2);
        assertTrue(v100.compareTo(v100) == 0);
        assertTrue(v100.compareTo(v101) < 0);
        assertTrue(v101.compareTo(v100) > 0);

        assertTrue(v200.compareTo(v101) > 0);
        assertTrue(v202.compareTo(v101) > 0);
        assertTrue(v202.compareTo(v200) > 0);
    }
}
