/*
 * Copyright (c) 2016, 2024, Oracle and/or its affiliates.
 *
 * This program is free software; you can redistribute it and/or modify it under the terms of the GNU General Public License, version 2.0, as published by
 * the Free Software Foundation.
 *
 * This program is designed to work with certain software that is licensed under separate terms, as designated in a particular file or component or in
 * included license documentation. The authors of MySQL hereby grant you an additional permission to link the program and your derivative works with the
 * separately licensed software that they have either included with the program or referenced in the documentation.
 *
 * Without limiting anything contained in the foregoing, this file, which is part of MySQL Connector/J, is also subject to the Universal FOSS Exception,
 * version 1.0, a copy of which can be found at http://oss.oracle.com/licenses/universal-foss-exception.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License, version 2.0, for more details.
 *
 * You should have received a copy of the GNU General Public License along with this program; if not, write to the Free Software Foundation, Inc.,
 * 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA
 */

package com.mysql.cj.protocol.a.result;

import java.util.HashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.mysql.cj.protocol.ColumnDefinition;
import com.mysql.cj.protocol.Resultset;
import com.mysql.cj.protocol.ResultsetRows;
import com.mysql.cj.result.DefaultColumnDefinition;
import com.mysql.cj.result.Field;
import com.mysql.cj.result.Row;

public class NativeResultset implements Resultset {

    /** The metadata for this result set */
    protected ColumnDefinition columnDefinition;

    /** The actual rows */
    protected ResultsetRows rowData;

    protected Resultset nextResultset = null;

    /** The id (used when profiling) to identify us */
    protected int resultId;

    /** How many rows were affected by UPDATE/INSERT/DELETE? */
    protected long updateCount;

    /** Value generated for AUTO_INCREMENT columns */
    protected long updateId = -1;

    /**
     * Any info message from the server that was created while generating this result set (if 'info parsing' is enabled for the connection).
     */
    protected String serverInfo = null;

    /** Pointer to current row data */
    protected Row thisRow = null; // Values for current row
    protected final Lock lock = new ReentrantLock();

    public NativeResultset() {
    }

    /**
     * Create a result set for an executeUpdate statement.
     *
     * @param ok
     *            {@link OkPacket}
     */
    public NativeResultset(OkPacket ok) {
        this.updateCount = ok.getUpdateCount();
        this.updateId = ok.getUpdateID();
        this.serverInfo = ok.getInfo();
        this.columnDefinition = new DefaultColumnDefinition(new Field[0]);
    }

    public NativeResultset(ResultsetRows rows) {
        this.columnDefinition = rows.getMetadata();
        this.rowData = rows;
        this.updateCount = this.rowData.size();

        // Check for no results
        if (this.rowData.size() > 0) {
            if (this.updateCount == 1) {
                if (this.thisRow == null) {
                    this.rowData.close(); // empty result set
                    this.updateCount = -1;
                }
            }
        } else {
            this.thisRow = null;
        }
    }

    @Override
    public void setColumnDefinition(ColumnDefinition metadata) {
        this.columnDefinition = metadata;
    }

    @Override
    public ColumnDefinition getColumnDefinition() {
        return this.columnDefinition;
    }

    @Override
    public boolean hasRows() {
        return this.rowData != null;
    }

    @Override
    public int getResultId() {
        return this.resultId;
    }

    @Override
    public void initRowsWithMetadata() {
        if (this.rowData != null) {
            this.rowData.setMetadata(this.columnDefinition);
        }
        this.columnDefinition.setColumnToIndexCache(new HashMap<>());
    }

    @Override
    public void setNextResultset(Resultset nextResultset) {
        this.lock.lock();
        try {
            this.nextResultset = nextResultset;
        } finally {
            this.lock.unlock();
        }
    }

    /**
     * @return the nextResultSet, if any, null if none exists.
     */
    @Override
    public Resultset getNextResultset() {
        this.lock.lock();
        try {
            // read next RS from streamer ?
            return this.nextResultset;
        } finally {
            this.lock.unlock();
        }
    }

    /**
     * We can't do this ourselves, otherwise the contract for
     * Statement.getMoreResults() won't work correctly.
     */
    @Override
    public void clearNextResultset() {
        this.lock.lock();
        try {
            // TODO release resources of nextResultset, close streamer
            this.nextResultset = null;
        } finally {
            this.lock.unlock();
        }
    }

    @Override
    public long getUpdateCount() {
        return this.updateCount;
    }

    @Override
    public long getUpdateID() {
        return this.updateId;
    }

    @Override
    public String getServerInfo() {
        return this.serverInfo;
    }

    @Override
    public ResultsetRows getRows() {
        return this.rowData;
    }

}
