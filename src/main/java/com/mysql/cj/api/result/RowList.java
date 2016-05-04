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

package com.mysql.cj.api.result;

import java.util.Iterator;

import com.mysql.cj.core.Messages;
import com.mysql.cj.core.exceptions.CJOperationNotSupportedException;
import com.mysql.cj.core.exceptions.ExceptionFactory;

/**
 * A list of {@link Row}s.
 */
public interface RowList extends Iterator<Row> {

    /**
     * What's returned for the size of a row list when its size can not be
     * determined.
     */
    public static final int RESULT_SET_SIZE_UNKNOWN = -1;

    /**
     * Optionally iterate backwards on the list.
     */
    default Row previous() {
        throw ExceptionFactory.createException(CJOperationNotSupportedException.class, Messages.getString("OperationNotSupportedException.0"));
    }

    /**
     * Optionally retrieve Row at index <i>n</i>.
     * 
     * Only works on non dynamic row lists.
     * 
     * @param n
     * @return
     */
    default Row get(int n) {
        throw ExceptionFactory.createException(CJOperationNotSupportedException.class, Messages.getString("OperationNotSupportedException.0"));
    }

    /**
     * Returns the current position.
     * 
     * @return the current row number
     */
    default int getPosition() {
        throw ExceptionFactory.createException(CJOperationNotSupportedException.class, Messages.getString("OperationNotSupportedException.0"));
    }

    /**
     * Only works on non dynamic row lists.
     * 
     * @return the size of this row list
     */
    default int size() {
        return RESULT_SET_SIZE_UNKNOWN;
    }
}
