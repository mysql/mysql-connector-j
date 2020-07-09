/*
  Copyright (c) 2020, Oracle and/or its affiliates. All rights reserved.

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

package com.mysql.jdbc.counters;


import java.io.IOException;

/**
 * Check if query result is not larger then max result buffer size threshold
 */
public class ResultByteBufferCounter implements Counter {

    private long resultByteBufferCounter;
    private long maxResultBuffer;

    public ResultByteBufferCounter(long maxResultBuffer) {
        this.resultByteBufferCounter = 0;
        this.maxResultBuffer = maxResultBuffer;
    }

    public void increaseCounter(long count) throws IOException {
        if (this.maxResultBuffer != -1) {
            this.resultByteBufferCounter += count;
            if (this.resultByteBufferCounter > this.maxResultBuffer) {
                long counterValue = this.resultByteBufferCounter;
                resetCounter();
                throw new IOException(new StringBuilder("Result set exceeded maxResultBuffer limit. Received:  ")
                    .append(counterValue)
                    .append("; Current limit: ")
                    .append(this.maxResultBuffer)
                    .toString());
            }
        }
    }

    public void resetCounter() {
        this.resultByteBufferCounter = 0;
    }

    public long getResultByteBufferCounter() {
        return this.resultByteBufferCounter;
    }
}
