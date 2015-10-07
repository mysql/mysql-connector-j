/*
  Copyright (c) 2015, Oracle and/or its affiliates. All rights reserved.

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

package com.mysql.cj.mysqlx.devapi;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.function.Supplier;

import com.mysql.cj.api.result.Row;
import com.mysql.cj.api.result.RowList;
import com.mysql.cj.api.x.Warning;
import com.mysql.cj.core.exceptions.WrongArgumentException;
import com.mysql.cj.core.io.StatementExecuteOk;
import com.mysql.cj.core.result.BufferedRowList;
import com.mysql.cj.mysqlx.io.ResultStreamer;
import com.mysql.cj.mysqlx.result.RowToElement;

/**
 * Base class for data set results.
 */
public abstract class AbstractDataResult<T> implements ResultStreamer, Iterator<T> {

    protected int position = -1;
    protected int count = -1;
    protected RowList rows;
    protected Supplier<StatementExecuteOk> completer;
    protected StatementExecuteOk ok;
    protected RowToElement<T> rowToData;
    /** List of all elements. <code>null</code> until requested via {@link fetchAll()}. */
    protected List<T> all;

    public AbstractDataResult(RowList rows, Supplier<StatementExecuteOk> completer, RowToElement<T> rowToData) {
        this.rows = rows;
        this.completer = completer;
        this.rowToData = rowToData;
    }

    public T next() {
        if (this.all != null) {
            throw new WrongArgumentException("Cannot iterate after fetchAll()");
        }

        Row r = this.rows.next();
        if (r == null) {
            throw new NoSuchElementException();
        }
        this.position++;
        return this.rowToData.apply(r);
    }

    public List<T> fetchAll() {
        if (this.position > -1) {
            throw new WrongArgumentException("Cannot fetchAll() after starting iteration");
        }

        if (this.all == null) {
            this.all = new ArrayList<>((int) count());
            this.rows.forEachRemaining(r -> this.all.add(this.rowToData.apply(r)));
            this.all = Collections.unmodifiableList(this.all);
        }
        return this.all;
    }

    public long count() {
        finishStreaming();
        return this.count;
    }

    public boolean hasNext() {
        return this.rows.hasNext();
    }

    public StatementExecuteOk getStatementExecuteOk() {
        finishStreaming();
        return this.ok;
    }

    /**
     * Finish the result streaming. This happens if a new command is started or the warnings/etc are requested. This is safe to call multiple times and only has
     * an effect the first time.
     * 
     * @todo better doc
     */
    public void finishStreaming() {
        if (this.ok == null) {
            BufferedRowList remainingRows = new BufferedRowList(this.rows);
            this.count = 1 + this.position + remainingRows.size();
            this.rows = remainingRows;
            this.ok = this.completer.get();
        }
    }

    public int getWarningsCount() {
        return getStatementExecuteOk().getWarnings().size();
    }

    public Iterator<Warning> getWarnings() {
        return getStatementExecuteOk().getWarnings().iterator();
    }
}
