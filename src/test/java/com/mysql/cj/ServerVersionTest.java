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

package com.mysql.cj;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
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

    @Test
    public void testEqualsAndHash() {
        ServerVersion v123a = new ServerVersion(1, 2, 3);
        ServerVersion v123b = new ServerVersion(1, 2, 3);
        ServerVersion v123c = new ServerVersion("1.2.3", 1, 2, 3);
        ServerVersion v123d = new ServerVersion("1.2.3-something", 1, 2, 3);
        ServerVersion v123e = ServerVersion.parseVersion("1.2.3");
        ServerVersion v123f = ServerVersion.parseVersion("1.2.3-something");
        ServerVersion[] versions = new ServerVersion[] { v123a, v123b, v123c, v123d, v123e, v123f };
        ServerVersion v321a = new ServerVersion(3, 2, 1);
        ServerVersion v321b = ServerVersion.parseVersion("3.2.1");

        for (int i = 0; i < versions.length; i++) {
            ServerVersion v1 = versions[i];
            for (int j = 0; j < versions.length; j++) {
                ServerVersion v2 = versions[j];
                if (i == j) {
                    assertSame(v1, v2);
                } else {
                    assertNotSame(v1, v2);
                }
                assertEquals(v1, v2);
                assertTrue(v1.equals(v2));
                assertEquals(v1.hashCode(), v2.hashCode());

                assertNotSame(v1, v321a);
                assertNotEquals(v1, v321a);
                assertFalse(v1.equals(v321a));
                assertFalse(v321a.equals(v1));
                assertNotEquals(v1.hashCode(), v321a.hashCode());

                assertNotSame(v1, v321b);
                assertNotEquals(v1, v321b);
                assertFalse(v1.equals(v321b));
                assertFalse(v321b.equals(v1));
                assertNotEquals(v1.hashCode(), v321b.hashCode());
            }
        }
    }
}
