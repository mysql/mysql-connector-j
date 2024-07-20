/*
 * Copyright (c) 2002, 2024, Oracle and/or its affiliates.
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

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.mysql.cj.Messages;
import com.mysql.cj.Session;
import com.mysql.cj.conf.PropertyKey;
import com.mysql.cj.exceptions.CJException;
import com.mysql.cj.exceptions.ExceptionFactory;
import com.mysql.cj.exceptions.ExceptionInterceptor;
import com.mysql.cj.exceptions.StreamingNotifiable;
import com.mysql.cj.log.ProfilerEvent;
import com.mysql.cj.protocol.ColumnDefinition;
import com.mysql.cj.protocol.ProtocolEntity;
import com.mysql.cj.protocol.ProtocolEntityFactory;
import com.mysql.cj.protocol.Resultset.Concurrency;
import com.mysql.cj.protocol.ResultsetRow;
import com.mysql.cj.protocol.ResultsetRows;
import com.mysql.cj.protocol.a.BinaryRowFactory;
import com.mysql.cj.protocol.a.NativeMessageBuilder;
import com.mysql.cj.protocol.a.NativePacketPayload;
import com.mysql.cj.protocol.a.NativeProtocol;
import com.mysql.cj.protocol.a.TextRowFactory;
import com.mysql.cj.result.Row;
import com.mysql.cj.telemetry.TelemetryAttribute;
import com.mysql.cj.telemetry.TelemetryScope;
import com.mysql.cj.telemetry.TelemetrySpan;
import com.mysql.cj.telemetry.TelemetrySpanName;
import com.mysql.cj.util.Util;

/**
 * Provides streaming of Resultset rows. Each next row is consumed from the input stream only on {@link #next()} call. Consumed rows are not cached thus result
 * sets are streamed only when they are forward-only, read-only, and the fetch size has been set to Integer.MIN_VALUE (rows are read one by one).
 *
 * @param <T>
 *            ProtocolEntity type
 */
public class ResultsetRowsStreaming<T extends ProtocolEntity> extends AbstractResultsetRows implements ResultsetRows {

    private NativeProtocol protocol;
    private NativeMessageBuilder commandBuilder = null;
    private ProtocolEntityFactory<T, NativePacketPayload> resultSetFactory;

    private final Lock lock = new ReentrantLock();

    private Row nextRow;

    private boolean isAfterEnd = false;
    private boolean noMoreRows = false;
    private boolean isBinaryEncoded = false;
    private boolean streamerClosed = false;

    private ExceptionInterceptor exceptionInterceptor;

    /**
     * Creates a new ResultsetRowsStreaming object.
     *
     * @param protocol
     *            the connection to MySQL that this data is coming from
     * @param columnDefinition
     *            the metadata that describe this data
     * @param isBinaryEncoded
     *            is this data in native format?
     * @param resultSetFactory
     *            {@link ProtocolEntityFactory}
     */
    public ResultsetRowsStreaming(NativeProtocol protocol, ColumnDefinition columnDefinition, boolean isBinaryEncoded,
            ProtocolEntityFactory<T, NativePacketPayload> resultSetFactory) {
        this.protocol = protocol;
        this.isBinaryEncoded = isBinaryEncoded;
        this.metadata = columnDefinition;
        this.exceptionInterceptor = this.protocol.getExceptionInterceptor();
        this.resultSetFactory = resultSetFactory;
        this.rowFactory = this.isBinaryEncoded ? new BinaryRowFactory(this.protocol, this.metadata, Concurrency.READ_ONLY, true)
                : new TextRowFactory(this.protocol, this.metadata, Concurrency.READ_ONLY, true);
        this.commandBuilder = new NativeMessageBuilder(this.protocol.getServerSession().supportsQueryAttributes());
    }

    @Override
    public void close() {
        boolean hadMore = false;
        int howMuchMore = 0;

        Lock localLock = this.owner != null && this.owner.getLock() != null ? this.owner.getLock() : this.lock;
        localLock.lock();
        try {
            // drain the rest of the records.
            while (next() != null) {
                hadMore = true;
                howMuchMore++;
                if (howMuchMore % 100 == 0) {
                    Thread.yield();
                }
            }

            if (this.protocol.getPropertySet().getIntegerProperty(PropertyKey.netTimeoutForStreamingResults).getValue() > 0) {
                Session session = this.owner.getSession();
                TelemetrySpan span = session.getTelemetryHandler().startSpan(TelemetrySpanName.SET_VARIABLE, "net_write_timeout");
                try (TelemetryScope scope = span.makeCurrent()) {
                    span.setAttribute(TelemetryAttribute.DB_NAME, session.getHostInfo().getDatabase());
                    span.setAttribute(TelemetryAttribute.DB_OPERATION, TelemetryAttribute.OPERATION_SET);
                    span.setAttribute(TelemetryAttribute.DB_STATEMENT, TelemetryAttribute.OPERATION_SET + TelemetryAttribute.STATEMENT_SUFFIX);
                    span.setAttribute(TelemetryAttribute.DB_SYSTEM, TelemetryAttribute.DB_SYSTEM_DEFAULT);
                    span.setAttribute(TelemetryAttribute.DB_USER, session.getHostInfo().getUser());
                    span.setAttribute(TelemetryAttribute.THREAD_ID, Thread.currentThread().getId());
                    span.setAttribute(TelemetryAttribute.THREAD_NAME, Thread.currentThread().getName());

                    int oldValue = this.protocol.getServerSession().getServerVariable("net_write_timeout", 60);

                    this.protocol.clearInputStream();

                    try {
                        this.protocol.sendCommand(
                                this.commandBuilder.buildComQuery(this.protocol.getSharedSendPacket(), session, "SET net_write_timeout=" + oldValue,
                                        this.protocol.getPropertySet().getStringProperty(PropertyKey.characterEncoding).getValue()),
                                false, 0);
                    } catch (Exception ex) {
                        throw ExceptionFactory.createException(ex.getMessage(), ex, this.exceptionInterceptor);
                    }
                } catch (Throwable t) {
                    span.setError(t);
                    throw t;
                } finally {
                    span.end();
                }
            }

            if (this.protocol.getPropertySet().getBooleanProperty(PropertyKey.useUsageAdvisor).getValue()) {
                if (hadMore) {
                    this.owner.getSession().getProfilerEventHandler().processEvent(ProfilerEvent.TYPE_USAGE, this.owner.getSession(),
                            this.owner.getOwningQuery(), null, 0, new Throwable(),
                            Messages.getString("RowDataDynamic.1", new String[] { String.valueOf(howMuchMore), this.owner.getPointOfOrigin() }));
                }
            }
        } finally {
            localLock.unlock();
        }

        this.metadata = null;
        this.owner = null;
    }

    @Override
    public boolean hasNext() {
        boolean hasNext = this.nextRow != null;

        if (!hasNext && !this.streamerClosed) {
            this.protocol.unsetStreamingData(this);
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
    public boolean isEmpty() {
        return this.wasEmpty;
    }

    @Override
    public boolean isFirst() {
        return this.currentPositionInFetchedRows == 0;
    }

    @Override
    public boolean isLast() {
        return !isBeforeFirst() && !isAfterLast() && this.noMoreRows;
    }

    @SuppressWarnings("unchecked")
    @Override
    public Row next() {
        try {
            if (!this.noMoreRows) {
                this.nextRow = this.protocol.read(ResultsetRow.class, this.rowFactory);

                if (this.nextRow == null) {
                    this.noMoreRows = true;
                    this.isAfterEnd = true;

                    if (this.currentPositionInFetchedRows == -1) {
                        this.wasEmpty = true;
                    }
                }
            } else {
                this.nextRow = null;
                this.isAfterEnd = true;
            }

            if (this.nextRow == null && !this.streamerClosed) {
                if (this.protocol.getServerSession().hasMoreResults()) {
                    this.protocol.readNextResultset((T) this.owner, this.owner.getOwningStatementMaxRows(), true, this.isBinaryEncoded, this.resultSetFactory);

                } else {
                    this.protocol.unsetStreamingData(this);
                    this.streamerClosed = true;
                }
            }

            if (this.nextRow != null) {
                if (this.currentPositionInFetchedRows != Integer.MAX_VALUE) {
                    this.currentPositionInFetchedRows++;
                }
            }

            return this.nextRow;

        } catch (CJException sqlEx) {

            if (sqlEx instanceof StreamingNotifiable) {
                ((StreamingNotifiable) sqlEx).setWasStreamingResults();
            }

            // There won't be any more rows
            this.noMoreRows = true;

            // don't wrap SQLExceptions
            throw sqlEx;
        } catch (Exception ex) {
            CJException cjEx = ExceptionFactory.createException(
                    Messages.getString("RowDataDynamic.2", new String[] { ex.getClass().getName(), ex.getMessage(), Util.stackTraceToString(ex) }), ex,
                    this.exceptionInterceptor);

            throw cjEx;
        }
    }

    @Override
    public int getPosition() {
        throw ExceptionFactory.createException(Messages.getString("ResultSet.ForwardOnly"));
    }

    @Override
    public void afterLast() {
        throw ExceptionFactory.createException(Messages.getString("ResultSet.ForwardOnly"));
    }

    @Override
    public void beforeFirst() {
        throw ExceptionFactory.createException(Messages.getString("ResultSet.ForwardOnly"));
    }

    @Override
    public void beforeLast() {
        throw ExceptionFactory.createException(Messages.getString("ResultSet.ForwardOnly"));
    }

    @Override
    public void moveRowRelative(int rows) {
        throw ExceptionFactory.createException(Messages.getString("ResultSet.ForwardOnly"));
    }

    @Override
    public void setCurrentRow(int rowNumber) {
        throw ExceptionFactory.createException(Messages.getString("ResultSet.ForwardOnly"));
    }

}
