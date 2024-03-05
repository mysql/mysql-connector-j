/*
 * Copyright (c) 2020, 2024, Oracle and/or its affiliates.
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

package com.mysql.cj.protocol.x;

import java.io.FilterOutputStream;
import java.io.OutputStream;

/**
 * An {@link OutputStream} wrapper that allows switching to different underlying {@link OutputStream}s under the same {@link OutputStream} instance.
 */
public class ReusableOutputStream extends FilterOutputStream {

    protected ReusableOutputStream(OutputStream out) {
        super(out);
    }

    /**
     * Sets a new underlying {@link OutputStream} in this {@link ReusableOutputStream}.
     *
     * @param newOut
     *            the new {@link OutputStream} to set.
     * @return
     *         the previous underlying {@link OutputStream}.
     */
    public OutputStream setOutputStream(OutputStream newOut) {
        OutputStream previousOut = this.out;
        this.out = newOut;
        return previousOut;
    }

}
