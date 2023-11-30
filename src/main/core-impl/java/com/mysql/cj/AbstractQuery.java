/*
 * Copyright (c) 2017, 2023, Oracle and/or its affiliates.
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

package com.mysql.cj;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import com.mysql.cj.conf.PropertyKey;
import com.mysql.cj.conf.RuntimeProperty;
import com.mysql.cj.exceptions.CJException;
import com.mysql.cj.exceptions.CJTimeoutException;
import com.mysql.cj.exceptions.ExceptionFactory;
import com.mysql.cj.exceptions.OperationCancelledException;
import com.mysql.cj.protocol.Message;
import com.mysql.cj.protocol.ProtocolEntityFactory;
import com.mysql.cj.protocol.Resultset;
import com.mysql.cj.protocol.Resultset.Type;

public abstract class AbstractQuery implements Query {

    /** Used to generate IDs when profiling. */
    static int statementCounter = 1;

    public NativeSession session = null;

    /** Used to identify this statement when profiling. */
    protected int statementId;

    protected RuntimeProperty<Integer> maxAllowedPacket;

    /** The character encoding to use (if available) */
    protected String charEncoding = null;

    /** Mutex to prevent race between returning query results and noticing that query has been timed-out or cancelled. */
    protected Object cancelTimeoutMutex = new Object();

    private CancelStatus cancelStatus = CancelStatus.NOT_CANCELED;

    /** The timeout for a query */
    protected long timeoutInMillis = 0L;

    /** Holds batched commands */
    protected List<Object> batchedArgs;

    /** The type of this result set (scroll sensitive or in-sensitive) */
    protected Resultset.Type resultSetType = Type.FORWARD_ONLY;

    /** The number of rows to fetch at a time (currently ignored) */
    protected int fetchSize = 0;

    /** Currently executing a statement? */
    protected final AtomicBoolean statementExecuting = new AtomicBoolean(false);

    /** The database in use */
    protected String currentDb = null;

    /** Has clearWarnings() been called? */
    protected boolean clearWarningsCalled = false;

    /** Elapsed time of the execution */
    private long executeTime = -1;

    /** Query attributes bindings */
    protected QueryAttributesBindings queryAttributesBindings;

    public AbstractQuery(NativeSession sess) {
        statementCounter++;
        this.session = sess;
        this.maxAllowedPacket = sess.getPropertySet().getIntegerProperty(PropertyKey.maxAllowedPacket);
        this.charEncoding = sess.getPropertySet().getStringProperty(PropertyKey.characterEncoding).getValue();
        this.queryAttributesBindings = new NativeQueryAttributesBindings(sess);
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
    public long getExecuteTime() {
        return this.executeTime;
    }

    @Override
    public void setExecuteTime(long executeTime) {
        this.executeTime = executeTime;
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

    @Override
    public void resetCancelledState() {
        synchronized (this.cancelTimeoutMutex) {
            this.cancelStatus = CancelStatus.NOT_CANCELED;
        }
    }

    @Override
    public <T extends Resultset, M extends Message> ProtocolEntityFactory<T, M> getResultSetFactory() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public NativeSession getSession() {
        return this.session;
    }

    @Override
    public Object getCancelTimeoutMutex() {
        return this.cancelTimeoutMutex;
    }

    @Override
    public void closeQuery() {
        this.queryAttributesBindings = null;
        this.session = null;
    }

    @Override
    public void addBatch(Object batch) {
        if (this.batchedArgs == null) {
            this.batchedArgs = new ArrayList<>();
        }
        this.batchedArgs.add(batch);
    }

    @Override
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
    public QueryAttributesBindings getQueryAttributesBindings() {
        return this.queryAttributesBindings;
    }

    @Override
    public int getResultFetchSize() {
        return this.fetchSize;
    }

    @Override
    public void setResultFetchSize(int fetchSize) {
        this.fetchSize = fetchSize;
    }

    @Override
    public Resultset.Type getResultType() {
        return this.resultSetType;
    }

    @Override
    public void setResultType(Resultset.Type resultSetType) {
        this.resultSetType = resultSetType;
    }

    @Override
    public long getTimeoutInMillis() {
        return this.timeoutInMillis;
    }

    @Override
    public void setTimeoutInMillis(long timeoutInMillis) {
        this.timeoutInMillis = timeoutInMillis;
    }

    @Override
    public CancelQueryTask startQueryTimer(Query stmtToCancel, long timeout) {
        if (this.session.getPropertySet().getBooleanProperty(PropertyKey.enableQueryTimeouts).getValue() && timeout != 0) {
            CancelQueryTaskImpl timeoutTask = new CancelQueryTaskImpl(stmtToCancel);
            this.session.getCancelTimer().schedule(timeoutTask, timeout);
            return timeoutTask;
        }
        return null;
    }

    @Override
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

    @Override
    public AtomicBoolean getStatementExecuting() {
        return this.statementExecuting;
    }

    @Override
    public String getCurrentDatabase() {
        return this.currentDb;
    }

    @Override
    public void setCurrentDatabase(String currentDb) {
        this.currentDb = currentDb;
    }

    @Override
    public boolean isClearWarningsCalled() {
        return this.clearWarningsCalled;
    }

    @Override
    public void setClearWarningsCalled(boolean clearWarningsCalled) {
        this.clearWarningsCalled = clearWarningsCalled;
    }

    @Override
    public void statementBegins() {
        this.clearWarningsCalled = false;
        this.statementExecuting.set(true);
    }

}
