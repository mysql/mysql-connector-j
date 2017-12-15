/*
 * Copyright (c) 2015, 2017, Oracle and/or its affiliates. All rights reserved.
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

package com.mysql.cj.x.io;

import java.util.TimeZone;
import java.util.concurrent.CompletableFuture;

import com.google.protobuf.GeneratedMessage;
import com.mysql.cj.api.x.core.ResultCtor;
import com.mysql.cj.api.x.io.ColToFieldTransformer;
import com.mysql.cj.api.x.io.MessageListener;
import com.mysql.cj.api.xdevapi.SqlResult;
import com.mysql.cj.core.exceptions.CJCommunicationsException;
import com.mysql.cj.x.core.StatementExecuteOk;
import com.mysql.cj.x.protobuf.Mysqlx.Error;
import com.mysql.cj.x.protobuf.MysqlxResultset.ColumnMetaData;
import com.mysql.cj.xdevapi.SqlDataResult;
import com.mysql.cj.xdevapi.SqlUpdateResult;

public class SqlResultMessageListener implements MessageListener {
    private static enum ResultType {
        UPDATE, DATA
    }

    private ResultType resultType;

    private CompletableFuture<SqlResult> resultF;

    /**
     * Delegate if we get an update result.
     */
    private StatementExecuteOkMessageListener okListener;

    /**
     * Delegate if we get a data result.
     */
    private ResultMessageListener resultListener;
    private ResultCreatingResultListener<SqlResult> resultCreator;

    public SqlResultMessageListener(CompletableFuture<SqlResult> resultF, ColToFieldTransformer colToField, TimeZone defaultTimeZone) {
        // compose with non-data future
        this.resultF = resultF;
        ResultCtor<SqlResult> resultCtor = metadata -> (rows, task) -> new SqlDataResult(metadata, defaultTimeZone, rows, task);
        this.resultCreator = new ResultCreatingResultListener<>(resultCtor, resultF);
        this.resultListener = new ResultMessageListener(colToField, this.resultCreator);
        // Propagate the ok packet (or exception) to the result promise
        CompletableFuture<StatementExecuteOk> okF = new CompletableFuture<>();
        // hope this doesn't get GC'd
        okF.whenComplete((ok, ex) -> {
            if (ex != null) {
                this.resultF.completeExceptionally(ex);
            } else {
                this.resultF.complete(new SqlUpdateResult(ok));
            }
        });
        this.okListener = new StatementExecuteOkMessageListener(okF);
    }

    public Boolean apply(Class<? extends GeneratedMessage> msgClass, GeneratedMessage msg) {
        if (this.resultType == null) {
            if (ColumnMetaData.class.equals(msgClass)) {
                this.resultType = ResultType.DATA;
            } else if (!Error.class.equals(msgClass)) {
                this.resultType = ResultType.UPDATE;
            }
        }

        if (this.resultType == ResultType.DATA) {
            // delegate to the result creation
            return this.resultListener.apply(msgClass, msg);
        }
        // done
        return this.okListener.apply(msgClass, msg);
    }

    public void closed() {
        this.resultF.completeExceptionally(new CJCommunicationsException("Socket was closed"));
    }

    public void error(Throwable ex) {
        this.resultF.completeExceptionally(ex);
    }
}
