/*
  Copyright (c) 2015, Oracle and/or its affiliates. All rights reserved.

  The MySQL Connector/J is licensed under the terms of the GPLv2
  <http://www.gnu.org/licenses/old-licenses/gpl-2.0.html>, like most MySQL Connectors.
  There are special exceptions to the terms and conditions of the GPLv2 as it is applied to
  this software, see the FLOSS License Exception
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

package com.mysql.cj.core.result;

import java.util.Iterator;
import java.util.List;
import java.util.Spliterators;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import com.mysql.cj.api.result.Row;
import com.mysql.cj.api.result.RowInputStream;
import com.mysql.cj.api.result.RowList;

/**
 * @todo doc
 */
public class BufferedRowList implements RowList {
    private List<Row> rowList;
    private int position = -1;

    /**
     * Create a new instance by filling the internal buffer by draining the row stream.
     */
    public BufferedRowList(RowInputStream rowStream) {
        this.rowList = StreamSupport.stream(Spliterators.spliteratorUnknownSize(rowStream, 0), false).collect(Collectors.toList());
    }

    public Row next() {
        // TODO: validation
        return this.rowList.get(++this.position);
    }

    public Row previous() {
        // TODO: validation
        return this.rowList.get(--this.position);
    }

    public Row get(int n) {
        // TODO: validation
        return this.rowList.get(n);
    }

    public int getPosition() {
        return this.position;
    }

    public int size() {
        return this.rowList.size();
    }

    public Iterator<Row> iterator() {
        return this.rowList.iterator();
    }
}
