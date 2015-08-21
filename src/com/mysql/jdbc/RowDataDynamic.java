/*
  Copyright (c) 2002, 2015, Oracle and/or its affiliates. All rights reserved.

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

package com.mysql.jdbc;

import java.sql.SQLException;

import com.mysql.jdbc.profiler.ProfilerEvent;
import com.mysql.jdbc.profiler.ProfilerEventHandler;

/**
 * Allows streaming of MySQL data.
 */
public class RowDataDynamic implements RowData {

    private int columnCount;

    private Field[] metadata;

    private int index = -1;

    private MysqlIO io;

    private boolean isAfterEnd = false;

    private boolean noMoreRows = false;

    private boolean isBinaryEncoded = false;

    private ResultSetRow nextRow;

    private ResultSetImpl owner;

    private boolean streamerClosed = false;

    private boolean wasEmpty = false; // we don't know until we attempt to traverse

    private boolean useBufferRowExplicit;

    private boolean moreResultsExisted;

    private ExceptionInterceptor exceptionInterceptor;

    /**
     * Creates a new RowDataDynamic object.
     * 
     * @param io
     *            the connection to MySQL that this data is coming from
     * @param metadata
     *            the metadata that describe this data
     * @param isBinaryEncoded
     *            is this data in native format?
     * @param colCount
     *            the number of columns
     * @throws SQLException
     *             if the next record can not be found
     */
    public RowDataDynamic(MysqlIO io, int colCount, Field[] fields, boolean isBinaryEncoded) throws SQLException {
        this.io = io;
        this.columnCount = colCount;
        this.isBinaryEncoded = isBinaryEncoded;
        this.metadata = fields;
        this.exceptionInterceptor = this.io.getExceptionInterceptor();
        this.useBufferRowExplicit = MysqlIO.useBufferRowExplicit(this.metadata);
    }

    /**
     * Adds a row to this row data.
     * 
     * @param row
     *            the row to add
     * @throws SQLException
     *             if a database error occurs
     */
    public void addRow(ResultSetRow row) throws SQLException {
        notSupported();
    }

    /**
     * Moves to after last.
     * 
     * @throws SQLException
     *             if a database error occurs
     */
    public void afterLast() throws SQLException {
        notSupported();
    }

    /**
     * Moves to before first.
     * 
     * @throws SQLException
     *             if a database error occurs
     */
    public void beforeFirst() throws SQLException {
        notSupported();
    }

    /**
     * Moves to before last so next el is the last el.
     * 
     * @throws SQLException
     *             if a database error occurs
     */
    public void beforeLast() throws SQLException {
        notSupported();
    }

    /**
     * We're done.
     * 
     * @throws SQLException
     *             if a database error occurs
     */
    public void close() throws SQLException {
        // Belt and suspenders here - if we don't have a reference to the connection it's more than likely dead/gone and we won't be able to consume rows anyway

        Object mutex = this;

        MySQLConnection conn = null;

        if (this.owner != null) {
            conn = this.owner.connection;

            if (conn != null) {
                mutex = conn.getConnectionMutex();
            }
        }

        boolean hadMore = false;
        int howMuchMore = 0;

        synchronized (mutex) {
            // drain the rest of the records.
            while (next() != null) {
                hadMore = true;
                howMuchMore++;

                if (howMuchMore % 100 == 0) {
                    Thread.yield();
                }
            }

            if (conn != null) {
                if (!conn.getClobberStreamingResults() && conn.getNetTimeoutForStreamingResults() > 0) {
                    String oldValue = conn.getServerVariable("net_write_timeout");

                    if (oldValue == null || oldValue.length() == 0) {
                        oldValue = "60"; // the current default
                    }

                    this.io.clearInputStream();

                    java.sql.Statement stmt = null;

                    try {
                        stmt = conn.createStatement();
                        ((com.mysql.jdbc.StatementImpl) stmt).executeSimpleNonQuery(conn, "SET net_write_timeout=" + oldValue);
                    } finally {
                        if (stmt != null) {
                            stmt.close();
                        }
                    }
                }

                if (conn.getUseUsageAdvisor()) {
                    if (hadMore) {

                        ProfilerEventHandler eventSink = ProfilerEventHandlerFactory.getInstance(conn);

                        eventSink.consumeEvent(new ProfilerEvent(ProfilerEvent.TYPE_WARN, "", this.owner.owningStatement == null ? "N/A"
                                : this.owner.owningStatement.currentCatalog, this.owner.connectionId, this.owner.owningStatement == null ? -1
                                : this.owner.owningStatement.getId(), -1, System.currentTimeMillis(), 0, Constants.MILLIS_I18N, null, null, Messages
                                .getString("RowDataDynamic.2")
                                + howMuchMore
                                + Messages.getString("RowDataDynamic.3")
                                + Messages.getString("RowDataDynamic.4")
                                + Messages.getString("RowDataDynamic.5") + Messages.getString("RowDataDynamic.6") + this.owner.pointOfOrigin));
                    }
                }
            }
        }

        this.metadata = null;
        this.owner = null;
    }

    /**
     * Only works on non dynamic result sets.
     * 
     * @param index
     *            row number to get at
     * @return row data at index
     * @throws SQLException
     *             if a database error occurs
     */
    public ResultSetRow getAt(int ind) throws SQLException {
        notSupported();

        return null;
    }

    /**
     * Returns the current position in the result set as a row number.
     * 
     * @return the current row number
     * @throws SQLException
     *             if a database error occurs
     */
    public int getCurrentRowNumber() throws SQLException {
        notSupported();

        return -1;
    }

    /**
     * @see com.mysql.jdbc.RowData#getOwner()
     */
    public ResultSetInternalMethods getOwner() {
        return this.owner;
    }

    /**
     * Returns true if another row exsists.
     * 
     * @return true if more rows
     * @throws SQLException
     *             if a database error occurs
     */
    public boolean hasNext() throws SQLException {
        boolean hasNext = (this.nextRow != null);

        if (!hasNext && !this.streamerClosed) {
            this.io.closeStreamer(this);
            this.streamerClosed = true;
        }

        return hasNext;
    }

    /**
     * Returns true if we got the last element.
     * 
     * @return true if after last row
     * @throws SQLException
     *             if a database error occurs
     */
    public boolean isAfterLast() throws SQLException {
        return this.isAfterEnd;
    }

    /**
     * Returns if iteration has not occured yet.
     * 
     * @return true if before first row
     * @throws SQLException
     *             if a database error occurs
     */
    public boolean isBeforeFirst() throws SQLException {
        return this.index < 0;
    }

    /**
     * Returns true if the result set is dynamic.
     * 
     * This means that move back and move forward won't work because we do not
     * hold on to the records.
     * 
     * @return true if this result set is streaming from the server
     */
    public boolean isDynamic() {
        return true;
    }

    /**
     * Has no records.
     * 
     * @return true if no records
     * @throws SQLException
     *             if a database error occurs
     */
    public boolean isEmpty() throws SQLException {
        notSupported();

        return false;
    }

    /**
     * Are we on the first row of the result set?
     * 
     * @return true if on first row
     * @throws SQLException
     *             if a database error occurs
     */
    public boolean isFirst() throws SQLException {
        notSupported();

        return false;
    }

    /**
     * Are we on the last row of the result set?
     * 
     * @return true if on last row
     * @throws SQLException
     *             if a database error occurs
     */
    public boolean isLast() throws SQLException {
        notSupported();

        return false;
    }

    /**
     * Moves the current position relative 'rows' from the current position.
     * 
     * @param rows
     *            the relative number of rows to move
     * @throws SQLException
     *             if a database error occurs
     */
    public void moveRowRelative(int rows) throws SQLException {
        notSupported();
    }

    /**
     * Returns the next row.
     * 
     * @return the next row value
     * @throws SQLException
     *             if a database error occurs
     */
    public ResultSetRow next() throws SQLException {

        nextRecord();

        if (this.nextRow == null && !this.streamerClosed && !this.moreResultsExisted) {
            this.io.closeStreamer(this);
            this.streamerClosed = true;
        }

        if (this.nextRow != null) {
            if (this.index != Integer.MAX_VALUE) {
                this.index++;
            }
        }

        return this.nextRow;
    }

    private void nextRecord() throws SQLException {

        try {
            if (!this.noMoreRows) {
                this.nextRow = this.io.nextRow(this.metadata, this.columnCount, this.isBinaryEncoded, java.sql.ResultSet.CONCUR_READ_ONLY, true,
                        this.useBufferRowExplicit, true, null);

                if (this.nextRow == null) {
                    this.noMoreRows = true;
                    this.isAfterEnd = true;
                    this.moreResultsExisted = this.io.tackOnMoreStreamingResults(this.owner);

                    if (this.index == -1) {
                        this.wasEmpty = true;
                    }
                }
            } else {
                this.nextRow = null;
                this.isAfterEnd = true;
            }
        } catch (SQLException sqlEx) {
            if (sqlEx instanceof StreamingNotifiable) {
                ((StreamingNotifiable) sqlEx).setWasStreamingResults();
            }

            // There won't be any more rows
            this.noMoreRows = true;

            // don't wrap SQLExceptions
            throw sqlEx;
        } catch (Exception ex) {
            String exceptionType = ex.getClass().getName();
            String exceptionMessage = ex.getMessage();

            exceptionMessage += Messages.getString("RowDataDynamic.7");
            exceptionMessage += Util.stackTraceToString(ex);

            SQLException sqlEx = SQLError.createSQLException(Messages.getString("RowDataDynamic.8") + exceptionType + Messages.getString("RowDataDynamic.9")
                    + exceptionMessage, SQLError.SQL_STATE_GENERAL_ERROR, this.exceptionInterceptor);
            sqlEx.initCause(ex);

            throw sqlEx;
        }
    }

    private void notSupported() throws SQLException {
        throw new OperationNotSupportedException();
    }

    /**
     * Removes the row at the given index.
     * 
     * @param index
     *            the row to move to
     * @throws SQLException
     *             if a database error occurs
     */
    public void removeRow(int ind) throws SQLException {
        notSupported();
    }

    /**
     * Moves the current position in the result set to the given row number.
     * 
     * @param rowNumber
     *            row to move to
     * @throws SQLException
     *             if a database error occurs
     */
    public void setCurrentRow(int rowNumber) throws SQLException {
        notSupported();
    }

    /**
     * @see com.mysql.jdbc.RowData#setOwner(com.mysql.jdbc.ResultSetInternalMethods)
     */
    public void setOwner(ResultSetImpl rs) {
        this.owner = rs;
    }

    /**
     * Only works on non dynamic result sets.
     * 
     * @return the size of this row data
     */
    public int size() {
        return RESULT_SET_SIZE_UNKNOWN;
    }

    public boolean wasEmpty() {
        return this.wasEmpty;
    }

    public void setMetadata(Field[] metadata) {
        this.metadata = metadata;
    }
}
