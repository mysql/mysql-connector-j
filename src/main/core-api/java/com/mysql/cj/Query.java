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

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;

import com.mysql.cj.protocol.Message;
import com.mysql.cj.protocol.ProtocolEntityFactory;
import com.mysql.cj.protocol.Resultset;

public interface Query {

    public enum CancelStatus {
        NOT_CANCELED, CANCELED_BY_USER, CANCELED_BY_TIMEOUT;
    }

    /**
     * Returns the query id used when profiling
     *
     * @return id
     */
    int getId();

    void setCancelStatus(CancelStatus cs);

    void checkCancelTimeout();

    <T extends Resultset, M extends Message> ProtocolEntityFactory<T, M> getResultSetFactory();

    Session getSession();

    Lock getCancelTimeoutLock();

    void resetCancelledState();

    void closeQuery();

    void addBatch(Object batch);

    /**
     * Get the batched args as added by the addBatch method(s).
     * The list is unmodifiable and might contain any combination of String, ClientPreparedQueryBindings, or ServerPreparedQueryBindings depending on how the
     * parameters were batched.
     *
     * @return an unmodifiable List of batched args
     */
    List<Object> getBatchedArgs();

    void clearBatchedArgs();

    QueryAttributesBindings getQueryAttributesBindings();

    int getResultFetchSize();

    void setResultFetchSize(int fetchSize);

    Resultset.Type getResultType();

    void setResultType(Resultset.Type resultSetType);

    long getTimeoutInMillis();

    void setTimeoutInMillis(long timeoutInMillis);

    void setExecuteTime(long executeTime);

    /**
     * Returns the elapsed time for the server to execute the query.
     *
     * @return the time it took for the server to execute the query.
     */
    long getExecuteTime();

    CancelQueryTask startQueryTimer(Query stmtToCancel, long timeout);

    AtomicBoolean getStatementExecuting();

    String getCurrentDatabase();

    void setCurrentDatabase(String currentDb);

    boolean isClearWarningsCalled();

    void setClearWarningsCalled(boolean clearWarningsCalled);

    void statementBegins();

    void stopQueryTimer(CancelQueryTask timeoutTask, boolean rethrowCancelReason, boolean checkCancelTimeout);

}
