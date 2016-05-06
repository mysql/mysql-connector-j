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

import com.mysql.cj.api.MysqlConnection;
import com.mysql.cj.api.ProfilerEvent;
import com.mysql.cj.api.ProfilerEventHandler;
import com.mysql.cj.api.exceptions.ExceptionInterceptor;
import com.mysql.cj.api.exceptions.StreamingNotifiable;
import com.mysql.cj.api.jdbc.JdbcConnection;
import com.mysql.cj.api.mysqla.result.ResultsetRows;
import com.mysql.cj.api.result.Row;
import com.mysql.cj.core.Constants;
import com.mysql.cj.core.Messages;
import com.mysql.cj.core.conf.PropertyDefinitions;
import com.mysql.cj.core.exceptions.CJException;
import com.mysql.cj.core.exceptions.ExceptionFactory;
import com.mysql.cj.core.profiler.ProfilerEventHandlerFactory;
import com.mysql.cj.core.profiler.ProfilerEventImpl;
import com.mysql.cj.core.result.Field;
import com.mysql.cj.core.util.Util;
import com.mysql.cj.mysqla.io.MysqlaProtocol;

/**
 * Provides streaming of Resultset rows. Each next row is consumed from the
 * input stream only on {@link #next()} call. Consumed rows are not cached thus
 * we only stream result sets when they are forward-only, read-only, and the
 * fetch size has been set to Integer.MIN_VALUE (rows are read one by one).
 */
public class ResultsetRowsDynamic extends AbstractResultsetRows implements ResultsetRows {

    private int columnCount;

    private MysqlaProtocol io;

    private boolean isAfterEnd = false;

    private boolean noMoreRows = false;

    private boolean isBinaryEncoded = false;

    private Row nextRow;

    private boolean streamerClosed = false;

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
     */
    public ResultsetRowsDynamic(MysqlaProtocol io, int colCount, Field[] fields, boolean isBinaryEncoded) {
        this.io = io;
        this.columnCount = colCount;
        this.isBinaryEncoded = isBinaryEncoded;
        this.metadata = fields;
        this.exceptionInterceptor = this.io.getExceptionInterceptor();
    }

    @Override
    public void close() {

        Object mutex = this;

        MysqlConnection conn = null;

        if (this.owner != null) {
            conn = this.owner.getConnection();

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
                        try {
                            stmt = ((JdbcConnection) conn).createStatement();
                            ((com.mysql.cj.jdbc.StatementImpl) stmt).executeSimpleNonQuery((JdbcConnection) conn, "SET net_write_timeout=" + oldValue);
                        } finally {
                            if (stmt != null) {
                                stmt.close();
                            }
                        }
                    } catch (Exception ex) {
                        throw ExceptionFactory.createException(ex.getMessage(), ex, this.exceptionInterceptor);
                    }
                }

                if (conn.getPropertySet().getBooleanReadableProperty(PropertyDefinitions.PNAME_useUsageAdvisor).getValue()) {
                    if (hadMore) {

                        ProfilerEventHandler eventSink = ProfilerEventHandlerFactory.getInstance(conn.getSession());

                        eventSink.consumeEvent(new ProfilerEventImpl(ProfilerEvent.TYPE_WARN, "", this.owner.getCurrentCatalog(), this.owner.getConnectionId(),
                                this.owner.getOwningStatementId(), -1, System.currentTimeMillis(), 0, Constants.MILLIS_I18N, null, null,
                                Messages.getString("RowDataDynamic.2") + howMuchMore + Messages.getString("RowDataDynamic.3")
                                        + Messages.getString("RowDataDynamic.4") + Messages.getString("RowDataDynamic.5")
                                        + Messages.getString("RowDataDynamic.6") + this.owner.getPointOfOrigin()));
                    }
                }
            }
        }

        this.metadata = null;
        this.owner = null;
    }

    @Override
    public boolean hasNext() {
        boolean hasNext = (this.nextRow != null);

        if (!hasNext && !this.streamerClosed) {
            this.io.getResultsHandler().closeStreamer(this);
            this.streamerClosed = true;
        }

        return hasNext;
    }

    @Override
    public boolean isAfterLast() {
        return this.isAfterEnd;
    }

    @Override
    public boolean isBeforeFirst() {
        return this.currentPositionInFetchedRows < 0;
    }

    @Override
    public Row next() {

        nextRecord();

        if (this.nextRow == null && !this.streamerClosed && !this.moreResultsExisted) {
            this.io.getResultsHandler().closeStreamer(this);
            this.streamerClosed = true;
        }

        if (this.nextRow != null) {
            if (this.currentPositionInFetchedRows != Integer.MAX_VALUE) {
                this.currentPositionInFetchedRows++;
            }
        }

        return this.nextRow;
    }

    private void nextRecord() {

        try {
            if (!this.noMoreRows) {
                this.nextRow = this.io.getResultsHandler().nextRow(this.metadata, this.columnCount, this.isBinaryEncoded, java.sql.ResultSet.CONCUR_READ_ONLY,
                        true);

                if (this.nextRow == null) {
                    this.noMoreRows = true;
                    this.isAfterEnd = true;
                    this.moreResultsExisted = this.io.getResultsHandler().tackOnMoreStreamingResults(this.owner, this.isBinaryEncoded);

                    if (this.currentPositionInFetchedRows == -1) {
                        this.wasEmpty = true;
                    }
                }
            } else {
                this.nextRow = null;
                this.isAfterEnd = true;
            }
        } catch (CJException sqlEx) {

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

            CJException cjEx = ExceptionFactory.createException(
                    Messages.getString("RowDataDynamic.8") + exceptionType + Messages.getString("RowDataDynamic.9") + exceptionMessage, ex,
                    this.exceptionInterceptor);

            throw cjEx;
        }
    }
}
