/*
 * Copyright (c) 2019, Oracle and/or its affiliates. All rights reserved.
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

package com.mysql.cj.xdevapi;

import java.lang.ref.PhantomReference;
import java.lang.ref.ReferenceQueue;

import com.mysql.cj.MysqlxSession;
import com.mysql.cj.protocol.x.XMessage;
import com.mysql.cj.protocol.x.XMessageBuilder;
import com.mysql.cj.protocol.x.XProtocolError;

/**
 * Abstract class, common to all X DevAPI statement classes that can be prepared.
 * 
 * @param <RES_T>
 *            result interface
 */
public abstract class PreparableStatement<RES_T> {
    protected enum PreparedState {
        UNSUPPORTED, // Preparing statements is completely unsupported in the server currently being used.
        UNPREPARED, // Statement is not prepared yet, next execution will run unprepared.
        SUSPENDED, // Preparing statements is currently suspended but it is expected to resume sometime later.
        PREPARED, // The statement is prepared and ready for execution.
        PREPARE, // The statement shall be prepared on next execution.
        DEALLOCATE, // The statement shall be deallocated on next execution.
        REPREPARE; // The statement shall be deallocated and immediately re-prepared on next execution.
    }

    protected int preparedStatementId = 0;
    protected PreparedState preparedState = PreparedState.UNPREPARED;

    protected MysqlxSession mysqlxSession;

    /**
     * Helper method to return an {@link XMessageBuilder} instance from {@link MysqlxSession} in use.
     * 
     * @return
     *         the {@link XMessageBuilder} instance from current {@link MysqlxSession}
     */
    protected XMessageBuilder getMessageBuilder() {
        return (XMessageBuilder) this.mysqlxSession.<XMessage>getMessageBuilder();
    }

    /**
     * Mark this preparable statement to be deallocated on next execution, if it is currently prepared, or cancel the next prepare.
     */
    protected void resetPrepareState() {
        if (this.preparedState == PreparedState.PREPARED || this.preparedState == PreparedState.REPREPARE) {
            this.preparedState = PreparedState.DEALLOCATE;
        } else if (this.preparedState == PreparedState.PREPARE) {
            this.preparedState = PreparedState.UNPREPARED;
        }
    }

    /**
     * Mark this preparable statement to be deallocated and re-prepared on next execution, if it is currently prepared.
     */
    protected void setReprepareState() {
        if (this.preparedState == PreparedState.PREPARED) {
            this.preparedState = PreparedState.REPREPARE;
        }
    }

    /**
     * Executes synchronously this statement either directly or using prepared statements if:
     * 1. Prepared statements are supported by the server.
     * 2. The statement is executed repeatedly without changing its structure.
     * 
     * @return
     *         the object returned from the low level statement execution
     */
    public RES_T execute() {
        for (;;) {
            switch (this.preparedState) {
                case UNSUPPORTED:
                    // Fall-back to non-prepared statement execution.
                    return executeStatement();
                case UNPREPARED:
                    // Execute as non-prepared this time but mark as to be prepared on next execution.
                    RES_T result = executeStatement();
                    this.preparedState = PreparedState.PREPARE;
                    return result;
                case SUSPENDED:
                    // An error occurred in some previous prepare. If the server doesn't support prepared statements then mark it as unsupported, otherwise wait
                    // until the server is ready to accept new prepare attempts, executing non-prepared in the meantime.
                    if (!this.mysqlxSession.supportsPreparedStatements()) {
                        this.preparedState = PreparedState.UNSUPPORTED;
                    } else if (this.mysqlxSession.readyForPreparingStatements()) {
                        this.preparedState = PreparedState.PREPARE;
                    } else {
                        return executeStatement();
                    }
                    break;
                case PREPARE:
                    // Prepare this statement. If it succeeds then immediately follow with its execution, otherwise mark it as prepare suspended and let the
                    // following iteration to decide what to do next.
                    this.preparedState = prepareStatement() ? PreparedState.PREPARED : PreparedState.SUSPENDED;
                    break;
                case PREPARED:
                    // The statement is already prepared and can be executed safely.
                    return executePreparedStatement();
                case DEALLOCATE:
                    // Deallocate this statement and set it as unprepared so that it may be prepared later again.
                    deallocatePrepared();
                    this.preparedState = PreparedState.UNPREPARED;
                    break;
                case REPREPARE:
                    // Deallocate this statement and set it as to prepare so that it gets prepared and executed right away.
                    deallocatePrepared();
                    this.preparedState = PreparedState.PREPARE;
                    break;
            }
        }
    }

    /**
     * Executes the statement directly (non-prepared). Implementation is dependent on the statement type.
     * 
     * @return
     *         the object returned from the lower level statement execution
     */
    protected abstract RES_T executeStatement();

    /**
     * Returns the {@link XMessage} needed to prepare this statement. Implementation is dependent on the statement type.
     * 
     * @return
     *         the {@link XMessage} that prepares this statement
     */
    protected abstract XMessage getPrepareStatementXMessage();

    /**
     * Prepares a statement on the server to be later executed.
     * 
     * @return
     *         <code>true</code> if the statement was successfully prepared, <code>false</code> otherwise
     */
    private boolean prepareStatement() {
        if (!this.mysqlxSession.supportsPreparedStatements()) {
            return false;
        }
        try {
            this.preparedStatementId = this.mysqlxSession.getNewPreparedStatementId(this);
            this.mysqlxSession.sendMessage(getPrepareStatementXMessage(), this.mysqlxSession::readOk);
        } catch (XProtocolError e) {
            if (this.mysqlxSession.failedPreparingStatement(this.preparedStatementId, e)) {
                this.preparedStatementId = 0;
                return false;
            }
            this.preparedStatementId = 0;
            throw e;
        } catch (Throwable t) {
            this.preparedStatementId = 0;
            throw t;
        }
        return true;
    }

    /**
     * Executes a previously server-prepared statement. Implementation is dependent on the statement type.
     * 
     * @return
     *         the object returned from the lower level statement execution
     */
    protected abstract RES_T executePreparedStatement();

    /**
     * Deallocate this prepared statement from current {@link MysqlxSession}.
     */
    protected void deallocatePrepared() {
        if (this.preparedState == PreparedState.PREPARED || this.preparedState == PreparedState.DEALLOCATE || this.preparedState == PreparedState.REPREPARE) {
            try {
                this.mysqlxSession.sendMessage(getMessageBuilder().buildPrepareDeallocate(this.preparedStatementId), this.mysqlxSession::readOk);
            } finally {
                this.mysqlxSession.freePreparedStatementId(this.preparedStatementId);
                this.preparedStatementId = 0;
            }
        }
    }

    /**
     * {@link PhantomReference} to track prepared statement ids. An instance of this class must be kept until the prepared statement is properly deallocated. If
     * proper deallocation does not happen, this is used to identify abandoned prepared statements and proceed with its deallocation after the object is
     * destructed by using a {@link ReferenceQueue}.
     */
    public static class PreparableStatementFinalizer extends PhantomReference<PreparableStatement<?>> {
        int prepredStatementId;

        public PreparableStatementFinalizer(PreparableStatement<?> referent, ReferenceQueue<? super PreparableStatement<?>> q, int preparedStatementId) {
            super(referent, q);
            this.prepredStatementId = preparedStatementId;
        }

        public int getPreparedStatementId() {
            return this.prepredStatementId;
        }
    }
}
