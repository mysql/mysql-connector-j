/*
 * Copyright (c) 2015, 2023, Oracle and/or its affiliates.
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

package com.mysql.cj.result;

import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Spliterators;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class BufferedRowList implements RowList {

    private List<Row> rowList;
    private int position = -1;

    public BufferedRowList(List<Row> rowList) {
        this.rowList = rowList;
    }

    /**
     * Create a new instance by filling the internal buffer by draining the row stream.
     *
     * @param ris
     *            {@link Row}s iterator
     */
    public BufferedRowList(Iterator<Row> ris) {
        this.rowList = StreamSupport.stream(Spliterators.spliteratorUnknownSize(ris, 0), false).collect(Collectors.toList());
    }

    @Override
    public Row next() {
        if (this.position + 1 == this.rowList.size()) {
            throw new NoSuchElementException("Can't next() when position=" + this.position + " and size=" + this.rowList.size());
        }
        return this.rowList.get(++this.position);
    }

    @Override
    public Row previous() {
        if (this.position < 1) {
            throw new NoSuchElementException("Can't previous() when position=" + this.position);
        }
        return this.rowList.get(--this.position);
    }

    @Override
    public Row get(int n) {
        if (n < 0 || n >= this.rowList.size()) {
            throw new NoSuchElementException("Can't get(" + n + ") when size=" + this.rowList.size());
        }
        return this.rowList.get(n);
    }

    @Override
    public int getPosition() {
        return this.position;
    }

    @Override
    public int size() {
        return this.rowList.size();
    }

    @Override
    public boolean hasNext() {
        return this.position + 1 < this.rowList.size();
    }

}
