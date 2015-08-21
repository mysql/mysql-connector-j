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

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.function.Supplier;
import java.util.function.Function;

import com.mysql.cj.api.result.Row;
import com.mysql.cj.api.result.RowList;
import com.mysql.cj.api.x.Warning;
import com.mysql.cj.core.io.StatementExecuteOk;
import com.mysql.cj.core.result.BufferedRowList;
import com.mysql.cj.mysqlx.io.ResultStreamer;

/**
 * @todo
 */
public abstract class AbstractDataResult<T> implements ResultStreamer {
    protected int position = -1;
    protected int count = -1;
    protected RowList rows;
    protected Supplier<StatementExecuteOk> completer;
    protected StatementExecuteOk ok;
    protected Function<Row, T> rowToData;

    public AbstractDataResult(RowList rows, Supplier<StatementExecuteOk> completer) {
        this.rows = rows;
        this.completer = completer;
    }

    protected void setRowToData(Function<Row, T> rowToData) {
        this.rowToData = rowToData;
    }

    public T first() {
        throw new UnsupportedOperationException("TODO: not yet implemented");
    }

    public T next() {
        Row r = rows.next();
        if (r == null) {
            throw new NoSuchElementException();
        }
        this.position++;
        return rowToData.apply(r);
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
