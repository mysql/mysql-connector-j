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

import java.util.TimerTask;

import com.mysql.cj.api.Query;
import com.mysql.cj.api.Query.CancelStatus;
import com.mysql.cj.api.TransactionEventHandler;
import com.mysql.cj.core.Messages;
import com.mysql.cj.core.conf.PropertyDefinitions;
import com.mysql.cj.core.conf.url.HostInfo;
import com.mysql.cj.core.exceptions.OperationCancelledException;
import com.mysql.cj.core.util.StringUtils;
import com.mysql.cj.mysqla.io.CommandBuilder;

/**
 * Thread used to implement query timeouts...Eventually we could be more
 * efficient and have one thread with timers, but this is a straightforward
 * and simple way to implement a feature that isn't used all that often.
 */
public class CancelQueryTask extends TimerTask {

    Query queryToCancel;
    Throwable caughtWhileCancelling = null;
    boolean queryTimeoutKillsConnection = false;

    public CancelQueryTask(Query cancellee) {
        this.queryToCancel = cancellee;
        MysqlaSession session = (MysqlaSession) cancellee.getSession();
        this.queryTimeoutKillsConnection = session.getPropertySet().getBooleanReadableProperty(PropertyDefinitions.PNAME_queryTimeoutKillsConnection)
                .getValue();
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
                Query localQueryToCancel = CancelQueryTask.this.queryToCancel;
                if (localQueryToCancel == null) {
                    return;
                }
                MysqlaSession session = (MysqlaSession) localQueryToCancel.getSession();
                if (session == null) {
                    return;
                }

                try {
                    if (CancelQueryTask.this.queryTimeoutKillsConnection) {
                        localQueryToCancel.setCancelStatus(CancelStatus.CANCELED_BY_TIMEOUT);
                        session.invokeCleanupListeners(new OperationCancelledException(Messages.getString("Statement.ConnectionKilledDueToTimeout")));
                    } else {
                        synchronized (localQueryToCancel.getCancelTimeoutMutex()) {
                            long origConnId = session.getThreadId();
                            HostInfo hostInfo = session.getHostInfo();
                            String database = hostInfo.getDatabase();
                            String user = StringUtils.isNullOrEmpty(hostInfo.getUser()) ? "" : hostInfo.getUser();
                            String password = StringUtils.isNullOrEmpty(hostInfo.getPassword()) ? "" : hostInfo.getPassword();

                            MysqlaSession newSession = new MysqlaSession(hostInfo, session.getPropertySet());
                            newSession.connect(hostInfo, user, password, database, 30000, new TransactionEventHandler() {
                                @Override
                                public void transactionCompleted() {
                                }

                                public void transactionBegun() {
                                }
                            });
                            newSession.sendCommand(new CommandBuilder().buildComQuery(newSession.getSharedSendPacket(), "KILL QUERY " + origConnId), false, 0);

                            localQueryToCancel.setCancelStatus(CancelStatus.CANCELED_BY_TIMEOUT);
                        }
                    }
                    // } catch (NullPointerException npe) {
                    // Case when connection closed while starting to cancel.
                    // We can't easily synchronise this, because then one thread can't cancel() a running query.
                    // Ignore, we shouldn't re-throw this, because the connection's already closed, so the statement has been timed out.
                } catch (Throwable t) {
                    CancelQueryTask.this.caughtWhileCancelling = t;
                } finally {
                    setQueryToCancel(null);
                }
            }
        };

        cancelThread.start();
    }

    public Throwable getCaughtWhileCancelling() {
        return this.caughtWhileCancelling;
    }

    public void setCaughtWhileCancelling(Throwable caughtWhileCancelling) {
        this.caughtWhileCancelling = caughtWhileCancelling;
    }

    public Query getQueryToCancel() {
        return this.queryToCancel;
    }

    public void setQueryToCancel(Query queryToCancel) {
        this.queryToCancel = queryToCancel;
    }
}
