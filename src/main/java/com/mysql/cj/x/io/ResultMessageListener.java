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

import java.util.ArrayList;

import com.google.protobuf.GeneratedMessage;
import com.mysql.cj.api.x.io.ColToFieldTransformer;
import com.mysql.cj.api.x.io.MessageListener;
import com.mysql.cj.api.x.io.ResultListener;
import com.mysql.cj.core.exceptions.CJCommunicationsException;
import com.mysql.cj.core.exceptions.WrongArgumentException;
import com.mysql.cj.core.result.Field;
import com.mysql.cj.x.core.XDevAPIError;
import com.mysql.cj.x.protobuf.Mysqlx.Error;
import com.mysql.cj.x.protobuf.MysqlxNotice.Frame;
import com.mysql.cj.x.protobuf.MysqlxResultset.ColumnMetaData;
import com.mysql.cj.x.protobuf.MysqlxResultset.FetchDone;
import com.mysql.cj.x.protobuf.MysqlxResultset.Row;
import com.mysql.cj.x.protobuf.MysqlxSql.StmtExecuteOk;

/**
 * A {@link MessageListener} to handle result data and propagate it to a {@link ResultListener}.
 */
public class ResultMessageListener implements MessageListener {
    private ResultListener callbacks;
    private ColToFieldTransformer colToField;

    /**
     * Accumulate metadata before delivering to client.
     */
    private ArrayList<Field> metadata = new ArrayList<>();

    /**
     * Have we finished reading metadata and send it to client yet?
     */
    private boolean metadataSent = false;

    private StatementExecuteOkBuilder okBuilder = new StatementExecuteOkBuilder();

    public ResultMessageListener(ColToFieldTransformer colToField, ResultListener callbacks) {
        this.callbacks = callbacks;
        this.colToField = colToField;
    }

    private boolean handleColumn(ColumnMetaData col) {
        Field f = this.colToField.apply(col);
        this.metadata.add(f);
        return false; /* done reading? */
    }

    private boolean handleRow(Row r) {
        XProtocolRow row = new XProtocolRow(this.metadata, r);
        this.callbacks.onRow(row);
        return false; /* done reading? */
    }

    private boolean handleStmtExecuteOk() {
        this.callbacks.onComplete(this.okBuilder.build());
        return true; /* done reading? */
    }

    private boolean handleError(Error error) {
        XDevAPIError e = new XDevAPIError(error);
        this.callbacks.onError(e);
        return true; /* done reading? */
    }

    private void handleException(Throwable ex) {
        this.callbacks.onException(ex);
    }

    public Boolean apply(Class<? extends GeneratedMessage> msgClass, GeneratedMessage msg) {
        // accumulate metadata and deliver to listener on first non-metadata message
        if (ColumnMetaData.class.equals(msgClass)) {
            return handleColumn(ColumnMetaData.class.cast(msg));
        }
        if (!this.metadataSent) {
            this.callbacks.onMetadata(this.metadata);
            this.metadataSent = true;
        }

        if (StmtExecuteOk.class.equals(msgClass)) {
            return handleStmtExecuteOk();
        } else if (FetchDone.class.equals(msgClass)) {
            // ignored. wait for StmtExecuteOk
            return false; /* done reading? */
        } else if (Row.class.equals(msgClass)) {
            return handleRow(Row.class.cast(msg));
        } else if (Error.class.equals(msgClass)) {
            return handleError(Error.class.cast(msg));
        } else if (Frame.class.equals(msgClass)) {
            this.okBuilder.addNotice(Frame.class.cast(msg));
            return false; /* done reading? */
        }
        handleException(new WrongArgumentException("Unhandled msg class (" + msgClass + ") + msg=" + msg));
        return false; /* done reading? */ // note, this doesn't comply with the specified semantics ResultListener
    }

    public void closed() {
        this.callbacks.onException(new CJCommunicationsException("Socket was closed"));
    }

    public void error(Throwable ex) {
        handleException(ex);
    }
}
