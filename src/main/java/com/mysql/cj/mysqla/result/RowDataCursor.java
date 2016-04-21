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

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import com.mysql.cj.api.io.ServerSession;
import com.mysql.cj.api.jdbc.result.ResultSetInternalMethods;
import com.mysql.cj.api.mysqla.result.RowData;
import com.mysql.cj.core.Messages;
import com.mysql.cj.core.result.Field;
import com.mysql.cj.jdbc.ServerPreparedStatement;
import com.mysql.cj.jdbc.exceptions.OperationNotSupportedException;
import com.mysql.cj.jdbc.exceptions.SQLError;
import com.mysql.cj.jdbc.result.ResultSetImpl;
import com.mysql.cj.mysqla.io.MysqlaProtocol;

/**
 * Model for result set data backed by a cursor. Only works for forward-only result sets (but still works with updatable concurrency).
 */
public class RowDataCursor implements RowData {

    private final static int BEFORE_START_OF_ROWS = -1;

    /**
     * The cache of rows we have retrieved from the server.
     */
    private List<ResultSetRow> fetchedRows;

    /**
     * Where we are positionaly in the entire result set, used mostly to
     * facilitate easy 'isBeforeFirst()' and 'isFirst()' methods.
     */
    private int currentPositionInEntireResult = BEFORE_START_OF_ROWS;

    /**
     * Position in cache of rows, used to determine if we need to fetch more
     * rows from the server to satisfy a request for the next row.
     */
    private int currentPositionInFetchedRows = BEFORE_START_OF_ROWS;

    /**
     * The result set that we 'belong' to.
     */
    private ResultSetImpl owner;

    /**
     * Have we been told from the server that we have seen the last row?
     */
    private boolean lastRowFetched = false;

    /**
     * Field-level metadata from the server. We need this, because it is not
     * sent for each batch of rows, but we need the metadata to unpack the
     * results for each field.
     */
    private Field[] metadata;

    private ServerSession serverSession;

    /**
     * Communications channel to the server
     */
    private MysqlaProtocol mysql;

    /**
     * Identifier for the statement that created this cursor.
     */
    private long statementIdOnServer;

    /**
     * The prepared statement that created this cursor.
     */
    private ServerPreparedStatement prepStmt;

    /**
     * Have we attempted to fetch any rows yet?
     */
    private boolean firstFetchCompleted = false;

    private boolean wasEmpty = false;

    /**
     * Creates a new cursor-backed row provider.
     * 
     * @param serverSession
     *            session state
     * @param ioChannel
     *            connection to the server.
     * @param creatingStatement
     *            statement that opened the cursor.
     * @param metadata
     *            field-level metadata for the results that this cursor covers.
     */
    public RowDataCursor(ServerSession serverSession, MysqlaProtocol ioChannel, ServerPreparedStatement creatingStatement, Field[] metadata) {
        this.serverSession = serverSession;
        this.currentPositionInEntireResult = BEFORE_START_OF_ROWS;
        this.metadata = metadata;
        this.mysql = ioChannel;
        this.statementIdOnServer = creatingStatement.getServerStatementId();
        this.prepStmt = creatingStatement;
    }

    public boolean isAfterLast() {
        return this.lastRowFetched && this.currentPositionInFetchedRows > this.fetchedRows.size();
    }

    public ResultSetRow getAt(int index) throws SQLException {
        notSupported();

        return null;
    }

    public boolean isBeforeFirst() throws SQLException {
        return this.currentPositionInEntireResult < 0;
    }

    public void setCurrentRow(int rowNumber) throws SQLException {
        notSupported();
    }

    public int getCurrentRowNumber() throws SQLException {
        return this.currentPositionInEntireResult + 1;
    }

    public boolean isDynamic() {
        return true;
    }

    public boolean isEmpty() throws SQLException {
        return this.isBeforeFirst() && this.isAfterLast();
    }

    public boolean isFirst() throws SQLException {
        return this.currentPositionInEntireResult == 0;
    }

    public boolean isLast() throws SQLException {
        return this.lastRowFetched && this.currentPositionInFetchedRows == (this.fetchedRows.size() - 1);
    }

    public void addRow(ResultSetRow row) throws SQLException {
        notSupported();
    }

    public void afterLast() throws SQLException {
        notSupported();
    }

    public void beforeFirst() throws SQLException {
        notSupported();
    }

    public void beforeLast() throws SQLException {
        notSupported();
    }

    public void close() throws SQLException {

        this.metadata = null;
        this.owner = null;
    }

    public boolean hasNext() throws SQLException {

        if (this.fetchedRows != null && this.fetchedRows.size() == 0) {
            return false;
        }

        if (this.owner != null && this.owner.getOwningStatement() != null) {
            int maxRows = this.owner.getOwningStatement().maxRows;

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

    public void moveRowRelative(int rows) throws SQLException {
        notSupported();
    }

    public ResultSetRow next() throws SQLException {
        if (this.fetchedRows == null && this.currentPositionInEntireResult != BEFORE_START_OF_ROWS) {
            throw SQLError.createSQLException(Messages.getString("ResultSet.Operation_not_allowed_after_ResultSet_closed_144"),
                    SQLError.SQL_STATE_GENERAL_ERROR, this.mysql.getExceptionInterceptor());
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

        ResultSetRow row = this.fetchedRows.get(this.currentPositionInFetchedRows);

        row.setMetadata(this.metadata);

        return row;
    }

    private void fetchMoreRows() throws SQLException {
        if (this.lastRowFetched) {
            this.fetchedRows = new ArrayList<ResultSetRow>(0);
            return;
        }

        synchronized (this.owner.getConnection().getConnectionMutex()) {
            boolean oldFirstFetchCompleted = this.firstFetchCompleted;

            if (!this.firstFetchCompleted) {
                this.firstFetchCompleted = true;
            }

            int numRowsToFetch = this.owner.getFetchSize();

            if (numRowsToFetch == 0) {
                numRowsToFetch = this.prepStmt.getFetchSize();
            }

            if (numRowsToFetch == Integer.MIN_VALUE) {
                // Handle the case where the user used 'old' streaming result sets

                numRowsToFetch = 1;
            }

            this.fetchedRows = this.mysql.getResultsHandler().fetchRowsViaCursor(this.fetchedRows, this.statementIdOnServer, this.metadata, numRowsToFetch);
            this.currentPositionInFetchedRows = BEFORE_START_OF_ROWS;

            if (this.serverSession.isLastRowSent()) {
                this.lastRowFetched = true;

                if (!oldFirstFetchCompleted && this.fetchedRows.size() == 0) {
                    this.wasEmpty = true;
                }
            }
        }
    }

    public void removeRow(int ind) throws SQLException {
        notSupported();
    }

    public int size() {
        return RESULT_SET_SIZE_UNKNOWN;
    }

    private void notSupported() throws SQLException {
        throw new OperationNotSupportedException();
    }

    public void setOwner(ResultSetImpl rs) {
        this.owner = rs;
    }

    public ResultSetInternalMethods getOwner() {
        return this.owner;
    }

    public boolean wasEmpty() {
        return this.wasEmpty;
    }

    public void setMetadata(Field[] metadata) {
        this.metadata = metadata;
    }

}
