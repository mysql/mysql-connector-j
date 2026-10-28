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

/**
 * Checks whether zstd compression of the classic protocol can be used. It uses zstd-jni (com.github.luben:zstd-jni), an optional dependency that must be
 * in the application's class path. This class does not refer to zstd-jni, so that it can be loaded without it.
 */
public final class ZstdCompression {

    private ZstdCompression() {
    }

    private static class Availability {

        static final boolean AVAILABLE = check();

        private static boolean check() {
            try {
                Class.forName("com.github.luben.zstd.ZstdCompressCtx", true, ZstdCompression.class.getClassLoader());
                return true;
            } catch (ClassNotFoundException | LinkageError e) {
                return false;
            }
        }

    }

    /**
     * Tells whether zstd-jni and its native library can be loaded.
     *
     * @return {@code true} if zstd can be negotiated.
     */
    public static boolean isAvailable() {
        return Availability.AVAILABLE;
    }

}
