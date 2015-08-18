/*
  Copyright (c) 2002, 2015, Oracle and/or its affiliates. All rights reserved.

  The MySQL Connector/J is licensed under the terms of the GPLv2
  <http://www.gnu.org/licenses/old-licenses/gpl-2.0.html>, like most MySQL Connectors.
  There are special exceptions to the terms and conditions of the GPLv2 as it is applied to
  this software, see the FLOSS License Exception
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

package com.mysql.cj.jdbc;

import java.sql.SQLException;

import com.mysql.cj.api.ProfilerEvent;
import com.mysql.cj.api.ProfilerEventHandler;
import com.mysql.cj.api.exceptions.ExceptionInterceptor;
import com.mysql.cj.api.exceptions.StreamingNotifiable;
import com.mysql.cj.api.jdbc.JdbcConnection;
import com.mysql.cj.api.jdbc.ResultSetInternalMethods;
import com.mysql.cj.api.jdbc.RowData;
import com.mysql.cj.core.Constants;
import com.mysql.cj.core.Messages;
import com.mysql.cj.core.conf.PropertyDefinitions;
import com.mysql.cj.core.exceptions.CJException;
import com.mysql.cj.core.profiler.ProfilerEventHandlerFactory;
import com.mysql.cj.core.profiler.ProfilerEventImpl;
import com.mysql.cj.core.util.Util;
import com.mysql.cj.jdbc.exceptions.OperationNotSupportedException;
import com.mysql.cj.jdbc.exceptions.SQLError;
import com.mysql.cj.jdbc.exceptions.SQLExceptionsMapping;
import com.mysql.cj.mysqla.io.MysqlaProtocol;

/**
 * Allows streaming of MySQL data.
 */
public class RowDataDynamic implements RowData {

    private int columnCount;

    private Field[] metadata;

    private int index = -1;

    private MysqlaProtocol io;

    private boolean isAfterEnd = false;

    private boolean noMoreRows = false;

    private boolean isBinaryEncoded = false;

    private ResultSetRow nextRow;

    private ResultSetImpl owner;

    private boolean streamerClosed = false;

    private boolean wasEmpty = false; // we don't know until we attempt to traverse

    private boolean moreResultsExisted;

    private ExceptionInterceptor exceptionInterceptor;

    /**
     * Creates a new RowDataDynamic object.
     * 
     * @param io
     *            the connection to MySQL that this data is coming from
     * @param colCount
     *            the number of columns
     * @param fields
     *            the metadata that describe this data
     * @param isBinaryEncoded
     *            is this data in native format?
     * @throws SQLException
     *             if the next record can not be found
     */
    public RowDataDynamic(MysqlaProtocol io, int colCount, Field[] fields, boolean isBinaryEncoded) throws SQLException {
        this.io = io;
        this.columnCount = colCount;
        this.isBinaryEncoded = isBinaryEncoded;
        this.metadata = fields;
        this.exceptionInterceptor = this.io.getExceptionInterceptor();
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
        // Belt and suspenders here - if we don't have a reference to the connection it's more than likely dead/gone and we won't be able to consume rows anyway

        Object mutex = this;

        JdbcConnection conn = null;

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
                if (!conn.getPropertySet().getBooleanReadableProperty(PropertyDefinitions.PNAME_clobberStreamingResults).getValue()
                        && conn.getPropertySet().getIntegerReadableProperty(PropertyDefinitions.PNAME_netTimeoutForStreamingResults).getValue() > 0) {
                    int oldValue = conn.getSession().getServerVariable("net_write_timeout", 60);

                    this.io.clearInputStream();

                    java.sql.Statement stmt = null;

                    try {
                        stmt = conn.createStatement();
                        ((com.mysql.cj.jdbc.StatementImpl) stmt).executeSimpleNonQuery(conn, "SET net_write_timeout=" + oldValue);
                    } finally {
                        if (stmt != null) {
                            stmt.close();
                        }
                    }
                }

                if (conn.getPropertySet().getBooleanReadableProperty(PropertyDefinitions.PNAME_useUsageAdvisor).getValue()) {
                    if (hadMore) {

                        try {
                            ProfilerEventHandler eventSink = ProfilerEventHandlerFactory.getInstance(conn);

                            eventSink.consumeEvent(new ProfilerEventImpl(ProfilerEvent.TYPE_WARN, "", this.owner.owningStatement == null ? "N/A"
                                    : this.owner.owningStatement.currentCatalog, this.owner.connectionId, this.owner.owningStatement == null ? -1
                                    : this.owner.owningStatement.getId(), -1, System.currentTimeMillis(), 0, Constants.MILLIS_I18N, null, null, Messages
                                    .getString("RowDataDynamic.2")
                                    + howMuchMore
                                    + Messages.getString("RowDataDynamic.3")
                                    + Messages.getString("RowDataDynamic.4")
                                    + Messages.getString("RowDataDynamic.5")
                                    + Messages.getString("RowDataDynamic.6")
                                    + this.owner.pointOfOrigin));
                        } catch (CJException e) {
                            throw SQLExceptionsMapping.translateException(e, conn.getExceptionInterceptor());
                        }
                    }
                }
            }
        }

        this.metadata = null;
        this.owner = null;
    }

    public ResultSetRow getAt(int ind) throws SQLException {
        notSupported();

        return null;
    }

    public int getCurrentRowNumber() throws SQLException {
        notSupported();

        return -1;
    }

    public ResultSetInternalMethods getOwner() {
        return this.owner;
    }

    public boolean hasNext() throws SQLException {
        boolean hasNext = (this.nextRow != null);

        if (!hasNext && !this.streamerClosed) {
            this.io.getResultsHandler().closeStreamer(this);
            this.streamerClosed = true;
        }

        return hasNext;
    }

    public boolean isAfterLast() throws SQLException {
        return this.isAfterEnd;
    }

    public boolean isBeforeFirst() throws SQLException {
        return this.index < 0;
    }

    public boolean isDynamic() {
        return true;
    }

    public boolean isEmpty() throws SQLException {
        notSupported();

        return false;
    }

    public boolean isFirst() throws SQLException {
        notSupported();

        return false;
    }

    public boolean isLast() throws SQLException {
        notSupported();

        return false;
    }

    public void moveRowRelative(int rows) throws SQLException {
        notSupported();
    }

    public ResultSetRow next() throws SQLException {

        nextRecord();

        if (this.nextRow == null && !this.streamerClosed && !this.moreResultsExisted) {
            this.io.getResultsHandler().closeStreamer(this);
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
                this.nextRow = this.io.getResultsHandler().nextRow(this.metadata, this.columnCount, this.isBinaryEncoded, java.sql.ResultSet.CONCUR_READ_ONLY,
                        true);

                if (this.nextRow == null) {
                    this.noMoreRows = true;
                    this.isAfterEnd = true;
                    this.moreResultsExisted = this.io.getResultsHandler().tackOnMoreStreamingResults(this.owner, this.isBinaryEncoded);

                    if (this.index == -1) {
                        this.wasEmpty = true;
                    }
                }
            } else {
                this.nextRow = null;
                this.isAfterEnd = true;
            }
        } catch (SQLException | CJException sqlEx) {
            SQLException cause = sqlEx instanceof SQLException ? (SQLException) sqlEx : SQLExceptionsMapping.translateException(sqlEx);

            if (cause instanceof StreamingNotifiable) {
                ((StreamingNotifiable) cause).setWasStreamingResults();
            }

            // There won't be any more rows
            this.noMoreRows = true;

            // don't wrap SQLExceptions
            throw cause;
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

    public void removeRow(int ind) throws SQLException {
        notSupported();
    }

    public void setCurrentRow(int rowNumber) throws SQLException {
        notSupported();
    }

    public void setOwner(ResultSetImpl rs) {
        this.owner = rs;
    }

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
