/*
  Copyright (c) 2002, 2016, Oracle and/or its affiliates. All rights reserved.

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

import java.util.ArrayList;
import java.util.List;

import com.mysql.cj.api.mysqla.io.NativeProtocol.IntegerDataType;
import com.mysql.cj.api.mysqla.io.PacketPayload;
import com.mysql.cj.api.mysqla.result.ColumnDefinition;
import com.mysql.cj.api.mysqla.result.Resultset.Concurrency;
import com.mysql.cj.api.mysqla.result.ResultsetRow;
import com.mysql.cj.api.mysqla.result.ResultsetRows;
import com.mysql.cj.api.result.Row;
import com.mysql.cj.core.Messages;
import com.mysql.cj.core.exceptions.ExceptionFactory;
import com.mysql.cj.mysqla.MysqlaConstants;
import com.mysql.cj.mysqla.io.BinaryRowFactory;
import com.mysql.cj.mysqla.io.MysqlaProtocol;

/**
 * Model for result set data backed by a cursor (see http://dev.mysql.com/doc/refman/5.7/en/cursors.html and
 * SERVER_STATUS_CURSOR_EXISTS flag description on http://dev.mysql.com/doc/internals/en/status-flags.html).
 * Only works for forward-only result sets (but still works with updatable concurrency).
 */
public class ResultsetRowsCursor extends AbstractResultsetRows implements ResultsetRows {

    /**
     * The cache of rows we have retrieved from the server.
     */
    private List<Row> fetchedRows;

    /**
     * Where we are positionaly in the entire result set, used mostly to
     * facilitate easy 'isBeforeFirst()' and 'isFirst()' methods.
     */
    private int currentPositionInEntireResult = BEFORE_START_OF_ROWS;

    /**
     * Have we been told from the server that we have seen the last row?
     */
    private boolean lastRowFetched = false;

    /**
     * Communications channel to the server
     */
    private MysqlaProtocol protocol;

    /**
     * Have we attempted to fetch any rows yet?
     */
    private boolean firstFetchCompleted = false;

    /**
     * Creates a new cursor-backed row provider.
     * 
     * @param ioChannel
     *            connection to the server.
     * @param columnDefinition
     *            field-level metadata for the results that this cursor covers.
     */
    public ResultsetRowsCursor(MysqlaProtocol ioChannel, ColumnDefinition columnDefinition) {
        this.currentPositionInEntireResult = BEFORE_START_OF_ROWS;
        this.metadata = columnDefinition;
        this.protocol = ioChannel;
        this.rowFactory = new BinaryRowFactory(this.protocol, this.metadata, Concurrency.READ_ONLY, false);
    }

    @Override
    public boolean isAfterLast() {
        return this.lastRowFetched && this.currentPositionInFetchedRows > this.fetchedRows.size();
    }

    @Override
    public boolean isBeforeFirst() {
        return this.currentPositionInEntireResult < 0;
    }

    @Override
    public int getPosition() {
        return this.currentPositionInEntireResult + 1;
    }

    @Override
    public boolean isEmpty() {
        return this.isBeforeFirst() && this.isAfterLast();
    }

    @Override
    public boolean isFirst() {
        return this.currentPositionInEntireResult == 0;
    }

    @Override
    public boolean isLast() {
        return this.lastRowFetched && this.currentPositionInFetchedRows == (this.fetchedRows.size() - 1);
    }

    @Override
    public void close() {

        this.metadata = null;
        this.owner = null;
    }

    @Override
    public boolean hasNext() {

        if (this.fetchedRows != null && this.fetchedRows.size() == 0) {
            return false;
        }

        if (this.owner != null) {
            int maxRows = this.owner.getOwningStatementMaxRows();

            if (maxRows != -1 && this.currentPositionInEntireResult + 1 > maxRows) {
                return false;
            }
        }

        if (this.currentPositionInEntireResult != BEFORE_START_OF_ROWS) {
            // Case, we've fetched some rows, but are not at end of fetched block
            if (this.currentPositionInFetchedRows < (this.fetchedRows.size() - 1)) {
                return true;
            } else if (this.currentPositionInFetchedRows == this.fetchedRows.size() && this.lastRowFetched) {
                return false;
            } else {
                // need to fetch to determine
                fetchMoreRows();

                return (this.fetchedRows.size() > 0);
            }
        }

        // Okay, no rows _yet_, so fetch 'em

        fetchMoreRows();

        return this.fetchedRows.size() > 0;
    }

    @Override
    public Row next() {
        if (this.fetchedRows == null && this.currentPositionInEntireResult != BEFORE_START_OF_ROWS) {
            throw ExceptionFactory.createException(Messages.getString("ResultSet.Operation_not_allowed_after_ResultSet_closed_144"),
                    this.protocol.getExceptionInterceptor());
        }

        if (!hasNext()) {
            return null;
        }

        this.currentPositionInEntireResult++;
        this.currentPositionInFetchedRows++;

        // Catch the forced scroll-passed-end
        if (this.fetchedRows != null && this.fetchedRows.size() == 0) {
            return null;
        }

        if ((this.fetchedRows == null) || (this.currentPositionInFetchedRows > (this.fetchedRows.size() - 1))) {
            fetchMoreRows();
            this.currentPositionInFetchedRows = 0;
        }

        Row row = this.fetchedRows.get(this.currentPositionInFetchedRows);

        row.setMetadata(this.metadata);

        return row;
    }

    private void fetchMoreRows() {
        if (this.lastRowFetched) {
            this.fetchedRows = new ArrayList<Row>(0);
            return;
        }

        synchronized (this.owner.getConnection().getConnectionMutex()) {
            try {
                boolean oldFirstFetchCompleted = this.firstFetchCompleted;

                if (!this.firstFetchCompleted) {
                    this.firstFetchCompleted = true;
                }

                int numRowsToFetch = this.owner.getOwnerFetchSize();

                if (numRowsToFetch == 0) {
                    numRowsToFetch = this.owner.getOwningStatementFetchSize();
                }

                if (numRowsToFetch == Integer.MIN_VALUE) {
                    // Handle the case where the user used 'old' streaming result sets

                    numRowsToFetch = 1;
                }

                if (this.fetchedRows == null) {
                    this.fetchedRows = new ArrayList<Row>(numRowsToFetch);
                } else {
                    this.fetchedRows.clear();
                }

                // TODO this is not the right place for this code, should be in protocol
                PacketPayload sharedSendPacket = this.protocol.getSharedSendPacket();
                sharedSendPacket.setPosition(0);

                sharedSendPacket.writeInteger(IntegerDataType.INT1, MysqlaConstants.COM_STMT_FETCH);
                sharedSendPacket.writeInteger(IntegerDataType.INT4, this.owner.getOwningStatementServerId());
                sharedSendPacket.writeInteger(IntegerDataType.INT4, numRowsToFetch);

                this.protocol.sendCommand(MysqlaConstants.COM_STMT_FETCH, null, sharedSendPacket, true, null, 0);

                Row row = null;

                while ((row = this.protocol.read(ResultsetRow.class, this.rowFactory)) != null) {
                    this.fetchedRows.add(row);
                }

                this.currentPositionInFetchedRows = BEFORE_START_OF_ROWS;

                if (this.protocol.getServerSession().isLastRowSent()) {
                    this.lastRowFetched = true;

                    if (!oldFirstFetchCompleted && this.fetchedRows.size() == 0) {
                        this.wasEmpty = true;
                    }
                }
            } catch (Exception ex) {
                throw ExceptionFactory.createException(ex.getMessage(), ex);
            }
        }
    }

}
