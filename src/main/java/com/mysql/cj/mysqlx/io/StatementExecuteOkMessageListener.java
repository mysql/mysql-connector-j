/*
  Copyright (c) 2015, 2016, Oracle and/or its affiliates. All rights reserved.

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
import com.mysql.cj.core.exceptions.CJCommunicationsException;
import com.mysql.cj.core.exceptions.WrongArgumentException;
import com.mysql.cj.core.io.StatementExecuteOk;
import com.mysql.cj.mysqlx.MysqlxError;
import com.mysql.cj.mysqlx.io.AsyncMessageReader.MessageListener;
import com.mysql.cj.mysqlx.protobuf.Mysqlx.Error;
import com.mysql.cj.mysqlx.protobuf.MysqlxNotice.Frame;
import com.mysql.cj.mysqlx.protobuf.MysqlxResultset.FetchDone;
import com.mysql.cj.mysqlx.protobuf.MysqlxSql.StmtExecuteOk;

/**
 * Async message reader accumulating the status necessary to produce a {@link StatementExecuteOk} result.
 */
public class StatementExecuteOkMessageListener implements MessageListener {
    private StatementExecuteOkBuilder builder = new StatementExecuteOkBuilder();
    private CompletableFuture<StatementExecuteOk> future = new CompletableFuture<>();

    public StatementExecuteOkMessageListener(CompletableFuture<StatementExecuteOk> future) {
        this.future = future;
    }

    public Boolean apply(Class<? extends GeneratedMessage> msgClass, GeneratedMessage msg) {
        if (Frame.class.equals(msgClass)) {
            this.builder.addNotice(Frame.class.cast(msg));
            return false; /* done reading? */
        } else if (StmtExecuteOk.class.equals(msgClass)) {
            this.future.complete(this.builder.build());
            return true; /* done reading? */
        } else if (Error.class.equals(msgClass)) {
            this.future.completeExceptionally(new MysqlxError(Error.class.cast(msg)));
            return true; /* done reading? */
        } else if (FetchDone.class.equals(msgClass)) {
            return false; /* done reading? */
        }
        this.future.completeExceptionally(new WrongArgumentException("Unhandled msg class (" + msgClass + ") + msg=" + msg));
        return true; /* done reading? */
    }

    public void closed() {
        this.future.completeExceptionally(new CJCommunicationsException("Sock was closed"));
    }

    public void error(Throwable ex) {
        this.future.completeExceptionally(ex);
    }
}
