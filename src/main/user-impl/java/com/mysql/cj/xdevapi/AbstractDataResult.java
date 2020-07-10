/*
 * Copyright (c) 2015, 2020, Oracle and/or its affiliates.
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
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.function.Supplier;

import com.mysql.cj.exceptions.WrongArgumentException;
import com.mysql.cj.protocol.ProtocolEntity;
import com.mysql.cj.protocol.ProtocolEntityFactory;
import com.mysql.cj.protocol.ResultStreamer;
import com.mysql.cj.protocol.x.StatementExecuteOk;
import com.mysql.cj.protocol.x.XMessage;
import com.mysql.cj.result.BufferedRowList;
import com.mysql.cj.result.Row;
import com.mysql.cj.result.RowList;

/**
 * Base class for data set results.
 * 
 * @param <T>
 *            Result entry type
 */
public abstract class AbstractDataResult<T> implements ResultStreamer, Iterator<T>, Result {

    protected int position = -1;
    protected int count = -1;
    protected RowList rows;
    protected Supplier<ProtocolEntity> completer;
    protected StatementExecuteOk ok;
    protected ProtocolEntityFactory<T, XMessage> rowToData;
    /** List of all elements. <code>null</code> until requested via {@link #fetchAll()}. */
    protected List<T> all;

    /**
     * Constructor.
     * 
     * @param rows
     *            {@link RowList} object
     * @param completer
     *            Supplier for StatementExecuteOk object
     * @param rowToData
     *            {@link ProtocolEntityFactory}
     */
    public AbstractDataResult(RowList rows, Supplier<ProtocolEntity> completer, ProtocolEntityFactory<T, XMessage> rowToData) {
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
        return this.rowToData.createFromProtocolEntity(r);
    }

    /**
     * Create a list of all elements in the result forcing internal buffering.
     * 
     * @return list of result elements
     */
    public List<T> fetchAll() {
        if (this.position > -1) {
            throw new WrongArgumentException("Cannot fetchAll() after starting iteration");
        }

        if (this.all == null) {
            this.all = new ArrayList<>((int) count());
            this.rows.forEachRemaining(r -> this.all.add(this.rowToData.createFromProtocolEntity(r)));
            this.all = Collections.unmodifiableList(this.all);
        }
        return this.all;
    }

    /**
     * Return the number of items in this result. Forces internal buffering of the entire result.
     * 
     * @return number of elements in result
     */
    public long count() {
        finishStreaming();
        return this.count;
    }

    public boolean hasNext() {
        return this.rows.hasNext();
    }

    /**
     * Get StatementExecuteOk object finalizing the result transfer. Forces internal buffering of the entire result.
     * 
     * @return StatementExecuteOk object
     */
    public StatementExecuteOk getStatementExecuteOk() {
        finishStreaming();
        return this.ok;
    }

    @Override
    public void finishStreaming() {
        if (this.ok == null) {
            BufferedRowList remainingRows = new BufferedRowList(this.rows);
            this.count = 1 + this.position + remainingRows.size();
            this.rows = remainingRows;
            this.ok = (StatementExecuteOk) this.completer.get();
        }
    }

    @Override
    public long getAffectedItemsCount() {
        return getStatementExecuteOk().getAffectedItemsCount();
    }

    @Override
    public int getWarningsCount() {
        return getStatementExecuteOk().getWarningsCount();
    }

    @Override
    public Iterator<Warning> getWarnings() {
        return getStatementExecuteOk().getWarnings();
    }
}
