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

package com.mysql.cj.mysqlx.devapi;

import java.util.Iterator;
import java.util.List;
import java.util.function.Supplier;

import com.mysql.cj.api.x.Column;
import com.mysql.cj.api.x.Row;
import com.mysql.cj.api.x.SqlResult;
import com.mysql.cj.api.x.Warning;
import com.mysql.cj.core.exceptions.WrongArgumentException;
import com.mysql.cj.mysqlx.io.ResultStreamer;

/**
 * SQL result.
 */
public class SqlResultImpl implements SqlResult, ResultStreamer {
    private Supplier<SqlResult> resultStream;
    private SqlResult currentResult;

    public SqlResultImpl(Supplier<SqlResult> resultStream) {
        this.resultStream = resultStream;
        this.currentResult = resultStream.get();
    }

    private SqlResult getCurrentResult() {
        if (this.currentResult == null) {
            throw new WrongArgumentException("No active result");
        }
        return this.currentResult;
    }

    public boolean nextResult() {
        if (this.currentResult == null) {
            return false;
        }
        try {
            if (ResultStreamer.class.isAssignableFrom(this.currentResult.getClass())) {
                ((ResultStreamer) this.currentResult).finishStreaming();
            }
        } finally {
            // propagate any exception but clear the current result so we don't try to read any more results
            this.currentResult = null;
        }
        this.currentResult = this.resultStream.get();
        return this.currentResult != null;
    }

    public void finishStreaming() {
        while (nextResult()) {
            ;
        }
    }

    public boolean hasData() {
        return getCurrentResult().hasData();
    }

    public long getAffectedItemsCount() {
        return getCurrentResult().getAffectedItemsCount();
    }

    public Long getAutoIncrementValue() {
        return getCurrentResult().getAutoIncrementValue();
    }

    public List<String> getLastDocumentIds() {
        return getCurrentResult().getLastDocumentIds();
    }

    public int getWarningsCount() {
        return getCurrentResult().getWarningsCount();
    }

    public Iterator<Warning> getWarnings() {
        return getCurrentResult().getWarnings();
    }

    public int getColumnCount() {
        return getCurrentResult().getColumnCount();
    }

    public List<Column> getColumns() {
        return getCurrentResult().getColumns();
    }

    public List<String> getColumnNames() {
        return getCurrentResult().getColumnNames();
    }

    public long count() {
        return getCurrentResult().count();
    }

    public List<Row> fetchAll() {
        return getCurrentResult().fetchAll();
    }

    public Row next() {
        return getCurrentResult().next();
    }

    public boolean hasNext() {
        return getCurrentResult().hasNext();
    }
}
