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

package com.mysql.cj.xdevapi;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.function.Supplier;

import com.mysql.cj.exceptions.WrongArgumentException;
import com.mysql.cj.protocol.ResultStreamer;

/**
 * {@link SqlResult} representing a multiple result sets.
 */
public class SqlMultiResult implements SqlResult, ResultStreamer {

    private Supplier<SqlResult> resultStream;
    private List<SqlResult> pendingResults = new ArrayList<>();
    private SqlResult currentResult;

    /**
     * Constructor.
     *
     * @param resultStream
     *            Supplies the result stream depending on query type. Could be {@link SqlSingleResult}, {@link SqlUpdateResult} etc.
     */
    public SqlMultiResult(Supplier<SqlResult> resultStream) {
        this.resultStream = resultStream;
        this.currentResult = resultStream.get();
    }

    private SqlResult getCurrentResult() {
        if (this.currentResult == null) {
            throw new WrongArgumentException("No active result");
        }
        return this.currentResult;
    }

    @Override
    public boolean nextResult() {
        if (this.currentResult == null) {
            return false; // there was no first result thus we don't expect other ones
        }
        try {
            if (ResultStreamer.class.isAssignableFrom(this.currentResult.getClass())) {
                ((ResultStreamer) this.currentResult).finishStreaming();
            }
        } finally {
            // propagate any exception but clear the current result so we don't try to read any more results
            this.currentResult = null;
        }
        this.currentResult = this.pendingResults.size() > 0 ? this.pendingResults.remove(0) : this.resultStream.get();
        return this.currentResult != null;
    }

    @Override
    public void finishStreaming() {
        if (this.currentResult == null) {
            return; // there was no first result thus we don't expect other ones
        }
        if (ResultStreamer.class.isAssignableFrom(this.currentResult.getClass())) {
            ((ResultStreamer) this.currentResult).finishStreaming();
        }
        SqlResult pendingRs = null;
        while ((pendingRs = this.resultStream.get()) != null) {
            if (ResultStreamer.class.isAssignableFrom(pendingRs.getClass())) {
                ((ResultStreamer) pendingRs).finishStreaming();
            }
            this.pendingResults.add(pendingRs);
        }
    }

    @Override
    public boolean hasData() {
        return getCurrentResult().hasData();
    }

    @Override
    public long getAffectedItemsCount() {
        return getCurrentResult().getAffectedItemsCount();
    }

    @Override
    public Long getAutoIncrementValue() {
        return getCurrentResult().getAutoIncrementValue();
    }

    @Override
    public int getWarningsCount() {
        return getCurrentResult().getWarningsCount();
    }

    @Override
    public Iterator<Warning> getWarnings() {
        return getCurrentResult().getWarnings();
    }

    @Override
    public int getColumnCount() {
        return getCurrentResult().getColumnCount();
    }

    @Override
    public List<Column> getColumns() {
        return getCurrentResult().getColumns();
    }

    @Override
    public List<String> getColumnNames() {
        return getCurrentResult().getColumnNames();
    }

    @Override
    public long count() {
        return getCurrentResult().count();
    }

    @Override
    public List<Row> fetchAll() {
        return getCurrentResult().fetchAll();
    }

    @Override
    public Row next() {
        return getCurrentResult().next();
    }

    @Override
    public boolean hasNext() {
        return getCurrentResult().hasNext();
    }

}
