/*
 * Copyright (c) 2017, 2024, Oracle and/or its affiliates.
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

package com.mysql.cj;

import java.util.TimerTask;

import com.mysql.cj.Query.CancelStatus;
import com.mysql.cj.conf.HostInfo;
import com.mysql.cj.conf.PropertyKey;
import com.mysql.cj.exceptions.OperationCancelledException;
import com.mysql.cj.protocol.a.NativeMessageBuilder;
import com.mysql.cj.telemetry.TelemetryAttribute;
import com.mysql.cj.telemetry.TelemetryScope;
import com.mysql.cj.telemetry.TelemetrySpan;
import com.mysql.cj.telemetry.TelemetrySpanName;

//TODO should not be protocol-specific

/**
 * Thread used to implement query timeouts...Eventually we could be more
 * efficient and have one thread with timers, but this is a straightforward
 * and simple way to implement a feature that isn't used all that often.
 */
public class CancelQueryTaskImpl extends TimerTask implements CancelQueryTask {

    Query queryToCancel;
    Throwable caughtWhileCancelling = null;
    boolean queryTimeoutKillsConnection = false;

    public CancelQueryTaskImpl(Query cancellee) {
        this.queryToCancel = cancellee;
        NativeSession session = (NativeSession) cancellee.getSession();
        this.queryTimeoutKillsConnection = session.getPropertySet().getBooleanProperty(PropertyKey.queryTimeoutKillsConnection).getValue();
    }

    @Override
    public boolean cancel() {
        boolean res = super.cancel();
        this.queryToCancel = null;
        return res;
    }

    @Override
    public void run() {
        Thread cancelThread = new Thread() {

            @Override
            public void run() {
                Query localQueryToCancel = CancelQueryTaskImpl.this.queryToCancel;
                if (localQueryToCancel == null) {
                    return;
                }
                NativeSession session = (NativeSession) localQueryToCancel.getSession();
                if (session == null) {
                    return;
                }

                try {
                    if (CancelQueryTaskImpl.this.queryTimeoutKillsConnection) {
                        localQueryToCancel.setCancelStatus(CancelStatus.CANCELED_BY_TIMEOUT);
                        session.invokeCleanupListeners(new OperationCancelledException(Messages.getString("Statement.ConnectionKilledDueToTimeout")));
                    } else {
                        synchronized (localQueryToCancel.getCancelTimeoutMutex()) {
                            long origConnId = session.getThreadId();
                            HostInfo hostInfo = session.getHostInfo();
                            String database = hostInfo.getDatabase();
                            String user = hostInfo.getUser();
                            String password = hostInfo.getPassword();

                            NativeSession newSession = null;
                            try {
                                newSession = new NativeSession(hostInfo, session.getPropertySet());

                                TelemetrySpan span = newSession.getTelemetryHandler().startSpan(TelemetrySpanName.CANCEL_QUERY);
                                try (TelemetryScope scope = span.makeCurrent()) {
                                    span.setAttribute(TelemetryAttribute.DB_NAME, database);
                                    span.setAttribute(TelemetryAttribute.DB_OPERATION, TelemetryAttribute.OPERATION_KILL);
                                    span.setAttribute(TelemetryAttribute.DB_STATEMENT, TelemetryAttribute.OPERATION_KILL + TelemetryAttribute.STATEMENT_SUFFIX);
                                    span.setAttribute(TelemetryAttribute.DB_SYSTEM, TelemetryAttribute.DB_SYSTEM_DEFAULT);
                                    span.setAttribute(TelemetryAttribute.DB_USER, user);
                                    span.setAttribute(TelemetryAttribute.THREAD_ID, Thread.currentThread().getId());
                                    span.setAttribute(TelemetryAttribute.THREAD_NAME, Thread.currentThread().getName());

                                    newSession.connect(hostInfo, user, password, database, 30000, new TransactionEventHandler() {

                                        @Override
                                        public void transactionCompleted() {
                                        }

                                        @Override
                                        public void transactionBegun() {
                                        }

                                    });
                                    newSession.getProtocol().sendCommand(new NativeMessageBuilder(newSession.getServerSession().supportsQueryAttributes())
                                            .buildComQuery(newSession.getSharedSendPacket(), newSession, "KILL QUERY " + origConnId), false, 0);
                                } catch (Throwable t) {
                                    span.setError(t);
                                    throw t;
                                } finally {
                                    span.end();
                                }
                            } finally {
                                try {
                                    newSession.forceClose();
                                } catch (Throwable t) {
                                    // no-op.
                                }
                            }
                            localQueryToCancel.setCancelStatus(CancelStatus.CANCELED_BY_TIMEOUT);
                        }
                    }
                    // } catch (NullPointerException npe) {
                    // Case when connection closed while starting to cancel.
                    // We can't easily synchronize this, because then one thread can't cancel() a running query.
                    // Ignore, we shouldn't re-throw this, because the connection's already closed, so the statement has been timed out.
                } catch (Throwable t) {
                    CancelQueryTaskImpl.this.caughtWhileCancelling = t;
                } finally {
                    setQueryToCancel(null);
                }
            }

        };

        cancelThread.start();
    }

    @Override
    public Throwable getCaughtWhileCancelling() {
        return this.caughtWhileCancelling;
    }

    @Override
    public void setCaughtWhileCancelling(Throwable caughtWhileCancelling) {
        this.caughtWhileCancelling = caughtWhileCancelling;
    }

    @Override
    public Query getQueryToCancel() {
        return this.queryToCancel;
    }

    @Override
    public void setQueryToCancel(Query queryToCancel) {
        this.queryToCancel = queryToCancel;
    }

}
