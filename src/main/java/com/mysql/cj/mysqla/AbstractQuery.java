/*
 * Copyright (c) 2017, Oracle and/or its affiliates. All rights reserved.
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

package com.mysql.cj.mysqla;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import com.mysql.cj.api.ProfilerEventHandler;
import com.mysql.cj.api.Query;
import com.mysql.cj.api.conf.ReadableProperty;
import com.mysql.cj.api.mysqla.io.ProtocolEntityFactory;
import com.mysql.cj.api.mysqla.result.Resultset;
import com.mysql.cj.api.mysqla.result.Resultset.Type;
import com.mysql.cj.core.conf.PropertyDefinitions;
import com.mysql.cj.core.exceptions.CJException;
import com.mysql.cj.core.exceptions.CJTimeoutException;
import com.mysql.cj.core.exceptions.ExceptionFactory;
import com.mysql.cj.core.exceptions.OperationCancelledException;
import com.mysql.cj.mysqla.io.CommandBuilder;

public abstract class AbstractQuery implements Query {

    /** Used to generate IDs when profiling. */
    static int statementCounter = 1;

    public MysqlaSession session = null;

    /** Used to identify this statement when profiling. */
    protected int statementId;

    /** Should we profile? */
    protected boolean profileSQL = false;

    protected ReadableProperty<Integer> maxAllowedPacket;

    /** The character encoding to use (if available) */
    protected String charEncoding = null;

    protected CommandBuilder commandBuilder = new CommandBuilder(); // TODO use shared builder

    /** Mutex to prevent race between returning query results and noticing that query has been timed-out or cancelled. */
    protected Object cancelTimeoutMutex = new Object();

    private CancelStatus cancelStatus = CancelStatus.NOT_CANCELED;

    /** The timeout for a query */
    protected int timeoutInMillis = 0;

    /** Holds batched commands */
    protected List<Object> batchedArgs;

    protected boolean useCursorFetch = false;

    /** The type of this result set (scroll sensitive or in-sensitive) */
    protected Resultset.Type resultSetType = Type.FORWARD_ONLY;

    /** The number of rows to fetch at a time (currently ignored) */
    protected int fetchSize = 0;

    /** If profiling, where should events go to? */
    protected ProfilerEventHandler eventSink = null;

    /** Currently executing a statement? */
    protected final AtomicBoolean statementExecuting = new AtomicBoolean(false);

    /** The catalog in use */
    protected String currentCatalog = null;

    /** Has clearWarnings() been called? */
    protected boolean clearWarningsCalled = false;

    public AbstractQuery(MysqlaSession sess) {
        statementCounter++;
        this.session = sess;
        this.profileSQL = sess.getPropertySet().getBooleanReadableProperty(PropertyDefinitions.PNAME_profileSQL).getValue();
        this.maxAllowedPacket = sess.getPropertySet().getIntegerReadableProperty(PropertyDefinitions.PNAME_maxAllowedPacket);
        this.charEncoding = sess.getPropertySet().getStringReadableProperty(PropertyDefinitions.PNAME_characterEncoding).getValue();
        this.useCursorFetch = sess.getPropertySet().getBooleanReadableProperty(PropertyDefinitions.PNAME_useCursorFetch).getValue();
    }

    @Override
    public int getId() {
        return this.statementId;
    }

    @Override
    public void setCancelStatus(CancelStatus cs) {
        this.cancelStatus = cs;
    }

    @Override
    public void checkCancelTimeout() {
        synchronized (this.cancelTimeoutMutex) {
            if (this.cancelStatus != CancelStatus.NOT_CANCELED) {
                CJException cause = this.cancelStatus == CancelStatus.CANCELED_BY_TIMEOUT ? new CJTimeoutException() : new OperationCancelledException();
                resetCancelledState();
                throw cause;
            }
        }
    }

    public void resetCancelledState() {
        synchronized (this.cancelTimeoutMutex) {
            this.cancelStatus = CancelStatus.NOT_CANCELED;
        }
    }

    @Override
    public <T extends Resultset> ProtocolEntityFactory<T> getResultSetFactory() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public MysqlaSession getSession() {
        return this.session;
    }

    @Override
    public Object getCancelTimeoutMutex() {
        return this.cancelTimeoutMutex;
    }

    public void closeQuery() {
        this.session = null;
    }

    public void addBatch(Object batch) {
        if (this.batchedArgs == null) {
            this.batchedArgs = new ArrayList<>();
        }
        this.batchedArgs.add(batch);
    }

    public List<Object> getBatchedArgs() {
        return this.batchedArgs == null ? null : Collections.unmodifiableList(this.batchedArgs);
    }

    @Override
    public void clearBatchedArgs() {
        if (this.batchedArgs != null) {
            this.batchedArgs.clear();
        }
    }

    @Override
    public int getResultFetchSize() {
        return this.fetchSize;
    }

    @Override
    public void setResultFetchSize(int fetchSize) {
        this.fetchSize = fetchSize;
    }

    public Resultset.Type getResultType() {
        return this.resultSetType;
    }

    public void setResultType(Resultset.Type resultSetType) {
        this.resultSetType = resultSetType;
    }

    public int getTimeoutInMillis() {
        return this.timeoutInMillis;
    }

    public void setTimeoutInMillis(int timeoutInMillis) {
        this.timeoutInMillis = timeoutInMillis;
    }

    public CancelQueryTask startQueryTimer(Query stmtToCancel, int timeout) {
        if (this.session.getPropertySet().getBooleanReadableProperty(PropertyDefinitions.PNAME_enableQueryTimeouts).getValue() && timeout != 0) {
            CancelQueryTask timeoutTask = new CancelQueryTask(stmtToCancel);
            this.session.getCancelTimer().schedule(timeoutTask, timeout);
            return timeoutTask;
        }
        return null;
    }

    public void stopQueryTimer(CancelQueryTask timeoutTask, boolean rethrowCancelReason, boolean checkCancelTimeout) {
        if (timeoutTask != null) {
            timeoutTask.cancel();

            if (rethrowCancelReason && timeoutTask.getCaughtWhileCancelling() != null) {
                Throwable t = timeoutTask.getCaughtWhileCancelling();
                throw ExceptionFactory.createException(t.getMessage(), t);
            }

            this.session.getCancelTimer().purge();

            if (checkCancelTimeout) {
                checkCancelTimeout();
            }
        }
    }

    public ProfilerEventHandler getEventSink() {
        return this.eventSink;
    }

    public void setEventSink(ProfilerEventHandler eventSink) {
        this.eventSink = eventSink;
    }

    public AtomicBoolean getStatementExecuting() {
        return this.statementExecuting;
    }

    public String getCurrentCatalog() {
        return this.currentCatalog;
    }

    public void setCurrentCatalog(String currentCatalog) {
        this.currentCatalog = currentCatalog;
    }

    public boolean isClearWarningsCalled() {
        return this.clearWarningsCalled;
    }

    public void setClearWarningsCalled(boolean clearWarningsCalled) {
        this.clearWarningsCalled = clearWarningsCalled;
    }

    public void statementBegins() {
        this.clearWarningsCalled = false;
        this.statementExecuting.set(true);
    }

}
