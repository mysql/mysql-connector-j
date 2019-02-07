/*
 * Copyright (c) 2019, Oracle and/or its affiliates. All rights reserved.
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

package testsuite.simple;

import org.junit.Test;

import com.mysql.cj.util.SequentialIdLease;

import testsuite.BaseTestCase;

public class SequentialIdLeaseTest extends BaseTestCase {

    public SequentialIdLeaseTest(String name) {
        super(name);
    }

    /**
     * Runs all test cases in this test suite
     * 
     * @param args
     */
    public static void main(String[] args) {
        junit.textui.TestRunner.run(StringUtilsTest.class);
    }

    /**
     * Tests the {@link SequentialIdLease} lease behavior.
     */
    @Test
    public void testSequentialIdLease() {
        SequentialIdLease seqIdLease;

        // Release first.
        seqIdLease = new SequentialIdLease();
        assertEquals(1, seqIdLease.allocateSequentialId());
        assertEquals(2, seqIdLease.allocateSequentialId());
        assertEquals(3, seqIdLease.allocateSequentialId());
        seqIdLease.releaseSequentialId(1);
        assertEquals(1, seqIdLease.allocateSequentialId());
        assertEquals(4, seqIdLease.allocateSequentialId());

        // Release single id in middle.
        seqIdLease = new SequentialIdLease();
        assertEquals(1, seqIdLease.allocateSequentialId());
        assertEquals(2, seqIdLease.allocateSequentialId());
        assertEquals(3, seqIdLease.allocateSequentialId());
        seqIdLease.releaseSequentialId(2);
        assertEquals(2, seqIdLease.allocateSequentialId());
        assertEquals(4, seqIdLease.allocateSequentialId());

        // Release last.
        seqIdLease = new SequentialIdLease();
        assertEquals(1, seqIdLease.allocateSequentialId());
        assertEquals(2, seqIdLease.allocateSequentialId());
        assertEquals(3, seqIdLease.allocateSequentialId());
        seqIdLease.releaseSequentialId(3);
        assertEquals(3, seqIdLease.allocateSequentialId());
        assertEquals(4, seqIdLease.allocateSequentialId());

        // Release multiple in the beginning.
        seqIdLease = new SequentialIdLease();
        assertEquals(1, seqIdLease.allocateSequentialId());
        assertEquals(2, seqIdLease.allocateSequentialId());
        assertEquals(3, seqIdLease.allocateSequentialId());
        assertEquals(4, seqIdLease.allocateSequentialId());
        seqIdLease.releaseSequentialId(1);
        seqIdLease.releaseSequentialId(2);
        assertEquals(1, seqIdLease.allocateSequentialId());
        assertEquals(2, seqIdLease.allocateSequentialId());
        assertEquals(5, seqIdLease.allocateSequentialId());

        // Release multiple in the middle.
        seqIdLease = new SequentialIdLease();
        assertEquals(1, seqIdLease.allocateSequentialId());
        assertEquals(2, seqIdLease.allocateSequentialId());
        assertEquals(3, seqIdLease.allocateSequentialId());
        assertEquals(4, seqIdLease.allocateSequentialId());
        seqIdLease.releaseSequentialId(2);
        seqIdLease.releaseSequentialId(3);
        assertEquals(2, seqIdLease.allocateSequentialId());
        assertEquals(3, seqIdLease.allocateSequentialId());
        assertEquals(5, seqIdLease.allocateSequentialId());

        // Release multiple in the end.
        seqIdLease = new SequentialIdLease();
        assertEquals(1, seqIdLease.allocateSequentialId());
        assertEquals(2, seqIdLease.allocateSequentialId());
        assertEquals(3, seqIdLease.allocateSequentialId());
        assertEquals(4, seqIdLease.allocateSequentialId());
        seqIdLease.releaseSequentialId(3);
        seqIdLease.releaseSequentialId(4);
        assertEquals(3, seqIdLease.allocateSequentialId());
        assertEquals(4, seqIdLease.allocateSequentialId());
        assertEquals(5, seqIdLease.allocateSequentialId());

        // Release interleaved.
        seqIdLease = new SequentialIdLease();
        assertEquals(1, seqIdLease.allocateSequentialId());
        assertEquals(2, seqIdLease.allocateSequentialId());
        assertEquals(3, seqIdLease.allocateSequentialId());
        assertEquals(4, seqIdLease.allocateSequentialId());
        assertEquals(5, seqIdLease.allocateSequentialId());
        assertEquals(6, seqIdLease.allocateSequentialId());
        assertEquals(7, seqIdLease.allocateSequentialId());
        seqIdLease.releaseSequentialId(1);
        seqIdLease.releaseSequentialId(3);
        seqIdLease.releaseSequentialId(5);
        seqIdLease.releaseSequentialId(7);
        assertEquals(1, seqIdLease.allocateSequentialId());
        assertEquals(3, seqIdLease.allocateSequentialId());
        assertEquals(5, seqIdLease.allocateSequentialId());
        assertEquals(7, seqIdLease.allocateSequentialId());
        assertEquals(8, seqIdLease.allocateSequentialId());

        // Release all.
        seqIdLease = new SequentialIdLease();
        assertEquals(1, seqIdLease.allocateSequentialId());
        assertEquals(2, seqIdLease.allocateSequentialId());
        assertEquals(3, seqIdLease.allocateSequentialId());
        seqIdLease.releaseSequentialId(1);
        seqIdLease.releaseSequentialId(2);
        seqIdLease.releaseSequentialId(3);
        assertEquals(1, seqIdLease.allocateSequentialId());
        assertEquals(2, seqIdLease.allocateSequentialId());
        assertEquals(3, seqIdLease.allocateSequentialId());

        // Release non-existing.
        seqIdLease = new SequentialIdLease();
        assertEquals(1, seqIdLease.allocateSequentialId());
        assertEquals(2, seqIdLease.allocateSequentialId());
        assertEquals(3, seqIdLease.allocateSequentialId());
        seqIdLease.releaseSequentialId(4);
        assertEquals(4, seqIdLease.allocateSequentialId());

        // Release from empty SequentialIdLease.
        seqIdLease = new SequentialIdLease();
        seqIdLease.releaseSequentialId(1);
    }
}
