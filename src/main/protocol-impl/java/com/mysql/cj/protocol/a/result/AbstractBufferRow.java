/*
 * Copyright (c) 2016, 2020, Oracle and/or its affiliates.
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

package com.mysql.cj.protocol.a.result;

import com.mysql.cj.exceptions.ExceptionInterceptor;
import com.mysql.cj.protocol.a.NativePacketPayload;
import com.mysql.cj.protocol.result.AbstractResultsetRow;

/**
 * A ResultSetRow implementation that holds one row packet (which is re-used by the driver, and thus saves memory allocations), and tries when possible to avoid
 * allocations to break out the results as individual byte[]s.
 * 
 * (this isn't possible when doing things like reading floating point values).
 */
public abstract class AbstractBufferRow extends AbstractResultsetRow {

    protected NativePacketPayload rowFromServer;

    /**
     * The beginning of the row packet
     */
    protected int homePosition = 0;

    /**
     * The last-requested index, used as an optimization, if you ask for the same index, we won't seek to find it. If you ask for an index that is greater
     * than the last one requested, we start seeking from the last requested index.
     */
    protected int lastRequestedIndex = -1;

    /**
     * The position of the last-requested index, optimization in concert with lastRequestedIndex.
     */
    protected int lastRequestedPos;

    protected AbstractBufferRow(ExceptionInterceptor exceptionInterceptor) {
        super(exceptionInterceptor);
    }

    abstract int findAndSeekToOffset(int index);

}
