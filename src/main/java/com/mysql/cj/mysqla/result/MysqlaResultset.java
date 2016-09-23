/*
  Copyright (c) 2016, Oracle and/or its affiliates. All rights reserved.

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

package com.mysql.cj.mysqla.result;

import java.util.HashMap;

import com.mysql.cj.api.mysqla.result.ColumnDefinition;
import com.mysql.cj.api.mysqla.result.Resultset;
import com.mysql.cj.api.mysqla.result.ResultsetRows;
import com.mysql.cj.api.result.Row;
import com.mysql.cj.core.result.Field;

public class MysqlaResultset implements Resultset {

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

    public MysqlaResultset() {
    }

    /**
     * Create a result set for an executeUpdate statement.
     * 
     * @param ok
     */
    public MysqlaResultset(OkPacket ok) {
        this.updateCount = ok.getUpdateCount();
        this.updateId = ok.getUpdateID();
        this.serverInfo = ok.getInfo();
        this.columnDefinition = new MysqlaColumnDefinition(new Field[0]);
    }

    public MysqlaResultset(ResultsetRows rows) {
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

    public boolean hasRows() {
        return this.rowData != null;
    }

    @Override
    public int getResultId() {
        return this.resultId;
    }

    public void initRowsWithMetadata() {
        this.rowData.setMetadata(this.columnDefinition);
        this.columnDefinition.setColumnToIndexCache(new HashMap<String, Integer>());
    }

    public synchronized void setNextResultset(Resultset nextResultset) {
        this.nextResultset = nextResultset;
    }

    /**
     * @return the nextResultSet, if any, null if none exists.
     */
    public synchronized Resultset getNextResultset() {
        // read next RS from streamer ?
        return this.nextResultset;
    }

    /**
     * We can't do this ourselves, otherwise the contract for
     * Statement.getMoreResults() won't work correctly.
     */
    public synchronized void clearNextResultset() {
        // TODO release resources of nextResultset, close streamer
        this.nextResultset = null;
    }

    public long getUpdateCount() {
        return this.updateCount;
    }

    public long getUpdateID() {
        return this.updateId;
    }

    public String getServerInfo() {
        return this.serverInfo;
    }
}
