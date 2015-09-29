/*
  Copyright (c) 2015, Oracle and/or its affiliates. All rights reserved.

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

package com.mysql.cj.mysqlx.io;

import java.util.concurrent.CompletableFuture;

import com.google.protobuf.GeneratedMessage;

import com.mysql.cj.api.x.SqlResult;
import com.mysql.cj.core.exceptions.CJCommunicationsException;
import com.mysql.cj.mysqlx.MysqlxSession.ResultCtor;
import com.mysql.cj.mysqlx.ResultCreatingResultListener;
import com.mysql.cj.mysqlx.devapi.SqlDataResult;
import com.mysql.cj.mysqlx.devapi.SqlUpdateResult;
import com.mysql.cj.mysqlx.io.AsyncMessageReader.MessageListener;
import com.mysql.cj.mysqlx.io.ResultMessageListener.ColToFieldTransformer;
import com.mysql.cj.mysqlx.protobuf.Mysqlx.Error;
import com.mysql.cj.mysqlx.protobuf.MysqlxResultset.ColumnMetaData;

public class SqlResultMessageListener implements MessageListener {
    private static enum ResultType {
        UPDATE, DATA
    }

    private static ResultCtor<SqlResult> RESULT_CTOR = metadata -> (rows, task) -> new SqlDataResult(metadata, rows, task);

    private ResultType resultType;

    private CompletableFuture<SqlResult> resultF = new CompletableFuture<>();

    /**
     * Delegate if we get an update result.
     */
    private StatementExecuteOkMessageListener okListener = new StatementExecuteOkMessageListener();

    /**
     * Delegate if we get a data result.
     */
    private ResultMessageListener resultListener;
    private ResultCreatingResultListener<SqlResult> resultCreator;

    public SqlResultMessageListener(CompletableFuture<SqlResult> resultF, ColToFieldTransformer colToField) {
        this.resultF = resultF;
        this.resultCreator = new ResultCreatingResultListener<>(RESULT_CTOR, resultF);
        this.resultListener = new ResultMessageListener(colToField, this.resultCreator);
    }

    public Boolean apply(Class<? extends GeneratedMessage> msgClass, GeneratedMessage msg) {
        if (this.resultType == null) {
            if (ColumnMetaData.class.equals(msgClass)) {
                this.resultType = ResultType.DATA;
            } else if (!Error.class.equals(msgClass)) {
                this.resultType = ResultType.UPDATE;
                this.okListener.getFuture().thenApply(SqlUpdateResult::new).thenAccept(this.resultF::complete);
            }
        }

        if (this.resultType == ResultType.DATA) {
            // delegate to the result creation
            return this.resultListener.apply(msgClass, msg);
        } else {
            // done
            return this.okListener.apply(msgClass, msg);
        }
    }

    public void closed() {
        this.resultF.completeExceptionally(new CJCommunicationsException("Sock was closed"));
    }

    public void error(Throwable ex) {
        this.resultF.completeExceptionally(ex);
    }
}
