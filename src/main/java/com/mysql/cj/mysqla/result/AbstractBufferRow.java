/*
  Copyright (c) 2016, Oracle and/or its affiliates. All rights reserved.

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

package com.mysql.cj.mysqla.result;

import com.mysql.cj.api.exceptions.ExceptionInterceptor;
import com.mysql.cj.api.mysqla.io.PacketPayload;

/**
 * A ResultSetRow implementation that holds one row packet (which is re-used by the driver, and thus saves memory allocations), and tries when possible to avoid
 * allocations to break out the results as individual byte[]s.
 * 
 * (this isn't possible when doing things like reading floating point values).
 */
public abstract class AbstractBufferRow extends AbstractResultsetRow {

    protected PacketPayload rowFromServer;

    /**
     * The beginning of the row packet
     */
    protected int homePosition = 0;

    /**
     * The last-requested index, used as an optimization, if you ask for the same index, we won't seek to find it. If you ask for an index that is >
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
