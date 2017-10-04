/*
  Copyright (c) 2017, Oracle and/or its affiliates. All rights reserved.

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

package com.mysql.cj.mysqla;

import java.util.Iterator;
import java.util.LinkedList;

import com.mysql.cj.api.mysqla.BatchVisitor;
import com.mysql.cj.core.util.StringUtils;

public class AppendingBatchVisitor implements BatchVisitor {
    LinkedList<byte[]> statementComponents = new LinkedList<>();

    public BatchVisitor append(byte[] values) {
        this.statementComponents.addLast(values);

        return this;
    }

    public BatchVisitor increment() {
        // no-op
        return this;
    }

    public BatchVisitor decrement() {
        this.statementComponents.removeLast();

        return this;
    }

    public BatchVisitor merge(byte[] front, byte[] back) {
        int mergedLength = front.length + back.length;
        byte[] merged = new byte[mergedLength];
        System.arraycopy(front, 0, merged, 0, front.length);
        System.arraycopy(back, 0, merged, front.length, back.length);
        this.statementComponents.addLast(merged);
        return this;
    }

    public byte[][] getStaticSqlStrings() {
        byte[][] asBytes = new byte[this.statementComponents.size()][];
        this.statementComponents.toArray(asBytes);

        return asBytes;
    }

    @Override
    public String toString() {
        StringBuilder buf = new StringBuilder();
        Iterator<byte[]> iter = this.statementComponents.iterator();
        while (iter.hasNext()) {
            buf.append(StringUtils.toString(iter.next()));
        }

        return buf.toString();
    }
}
