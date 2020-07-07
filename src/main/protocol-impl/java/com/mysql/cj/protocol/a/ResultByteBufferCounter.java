/*
 * Copyright (c) 2020, Oracle and/or its affiliates. All rights reserved.
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

package com.mysql.cj.protocol.a;

import com.mysql.cj.Messages;
import com.mysql.cj.exceptions.MaxResultBufferException;

public class ResultByteBufferCounter {

    private long resultByteBufferCounter;
    private final long maxResultBuffer;

    public ResultByteBufferCounter(long maxResultBuffer) {
        this.resultByteBufferCounter = 0;
        this.maxResultBuffer = maxResultBuffer;
    }

    /**
     * Increases counter of reading query result bytes
     * @param count
     *          count of query result bytes
     * @throws MaxResultBufferException
     *          throw, when query result is larger then threshold
     */
    public void increaseCounter(long count) {
        if (this.maxResultBuffer != -1) {
            this.resultByteBufferCounter += count;
            if (this.resultByteBufferCounter > this.maxResultBuffer) {
                throw new MaxResultBufferException(Messages.getString("ConnectionString.27",
                        new Object[]{this.resultByteBufferCounter, this.maxResultBuffer}));
            }
        }
    }

    /**
     * Reset counter to 0
     */
    public void resetCounter() {
        this.resultByteBufferCounter = 0;
    }

    public long getResultByteBufferCounter() {
        return this.resultByteBufferCounter;
    }
}
