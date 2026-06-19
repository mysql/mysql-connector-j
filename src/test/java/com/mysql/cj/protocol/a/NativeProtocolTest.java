/*
 * Copyright (c) 2026, Oracle and/or its affiliates.
 *
 * This program is free software; you can redistribute it and/or modify it under the terms of the GNU General Public License, version 2.0, as published by
 * the Free Software Foundation.
 *
 * This program is designed to work with certain software that is licensed under separate terms, as designated in a particular file or component or in
 * included license documentation. The authors of MySQL hereby grant you an additional permission to link the program and your derivative works with the
 * separately licensed software that they have either included with the program or referenced in the documentation.
 *
 * Without limiting anything contained in the foregoing, this file, which is part of MySQL Connector/J, is also subject to the Universal FOSS Exception,
 * version 1.0, a copy of which can be found at http://oss.oracle.com/licenses/universal-foss-exception.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License, version 2.0, for more details.
 *
 * You should have received a copy of the GNU General Public License along with this program; if not, write to the Free Software Foundation, Inc.,
 * 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA
 */

package com.mysql.cj.protocol.a;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.mysql.cj.util.Util;

import org.junit.jupiter.api.Test;

/**
 * Tests for native protocol behavior.
 */
public class NativeProtocolTest {

    @Test
    public void testLocalInfileUrlUncPathDetection() throws Exception {
        assertTrue(NativeProtocol.isLocalInfileUrlPathNetworkPath("////host/share"));
        assertTrue(NativeProtocol.isLocalInfileUrlPathNetworkPath("/////host/share"));
        assertTrue(NativeProtocol.isLocalInfileUrlPathNetworkPath("//host/share"));
        assertTrue(NativeProtocol.isLocalInfileUrlPathNetworkPath("/\\\\host\\share"));

        assertFalse(NativeProtocol.isLocalInfileUrlPathNetworkPath("/C:/tmp/file.txt"));
        if (Util.isRunningOnWindows()) {
            assertFalse(NativeProtocol.isLocalInfileUrlPathNetworkPath("////C:/tmp/file.txt"));
        } else {
            assertTrue(NativeProtocol.isLocalInfileUrlPathNetworkPath("////C:/tmp/file.txt"));
        }
        assertFalse(NativeProtocol.isLocalInfileUrlPathNetworkPath("/tmp/file.txt"));
    }

}
