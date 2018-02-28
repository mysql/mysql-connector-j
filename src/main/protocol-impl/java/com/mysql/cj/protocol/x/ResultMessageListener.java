/*
 * Copyright (c) 2015, 2018, Oracle and/or its affiliates. All rights reserved.
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

package com.mysql.cj.protocol.x;

import java.util.ArrayList;

import com.google.protobuf.GeneratedMessage;
import com.mysql.cj.exceptions.CJCommunicationsException;
import com.mysql.cj.exceptions.WrongArgumentException;
import com.mysql.cj.protocol.ColumnDefinition;
import com.mysql.cj.protocol.MessageListener;
import com.mysql.cj.protocol.ProtocolEntityFactory;
import com.mysql.cj.protocol.ResultListener;
import com.mysql.cj.result.DefaultColumnDefinition;
import com.mysql.cj.result.Field;
import com.mysql.cj.x.protobuf.Mysqlx.Error;
import com.mysql.cj.x.protobuf.MysqlxNotice.Frame;
import com.mysql.cj.x.protobuf.MysqlxResultset.ColumnMetaData;
import com.mysql.cj.x.protobuf.MysqlxResultset.FetchDone;
import com.mysql.cj.x.protobuf.MysqlxResultset.Row;
import com.mysql.cj.x.protobuf.MysqlxSql.StmtExecuteOk;

/**
 * A {@link MessageListener} to handle result data and propagate it to a {@link ResultListener}.
 */
public class ResultMessageListener implements MessageListener<XMessage> {
    private ResultListener<StatementExecuteOk> callbacks;
    private ProtocolEntityFactory<Field, XMessage> fieldFactory;
    private ProtocolEntityFactory<Notice, XMessage> noticeFactory;

    /**
     * Accumulate metadata before delivering to client.
     */
    private ArrayList<Field> fields = new ArrayList<>();
    private ColumnDefinition metadata = null;

    /**
     * Have we finished reading metadata and send it to client yet?
     */
    private boolean metadataSent = false;

    private StatementExecuteOkBuilder okBuilder = new StatementExecuteOkBuilder();

    public ResultMessageListener(ProtocolEntityFactory<Field, XMessage> colToField, ProtocolEntityFactory<Notice, XMessage> noticeFactory,
            ResultListener<StatementExecuteOk> callbacks) {
        this.callbacks = callbacks;
        this.fieldFactory = colToField;
        this.noticeFactory = noticeFactory;
    }

    public Boolean createFromMessage(XMessage message) {
        @SuppressWarnings("unchecked")
        Class<? extends GeneratedMessage> msgClass = (Class<? extends GeneratedMessage>) message.getMessage().getClass();

        // accumulate metadata and deliver to listener on first non-metadata message
        if (ColumnMetaData.class.equals(msgClass)) {
            Field f = this.fieldFactory.createFromMessage(message);
            this.fields.add(f);
            return false; /* done reading? */
        }
        if (!this.metadataSent) {
            if (this.metadata == null) {
                this.metadata = new DefaultColumnDefinition(this.fields.toArray(new Field[] {}));
            }
            this.callbacks.onMetadata(this.metadata);
            this.metadataSent = true;
        }

        if (StmtExecuteOk.class.equals(msgClass)) {
            this.callbacks.onComplete(this.okBuilder.build());
            return true; /* done reading? */

        } else if (FetchDone.class.equals(msgClass)) {
            // ignored. wait for StmtExecuteOk
            return false; /* done reading? */

        } else if (Row.class.equals(msgClass)) {
            if (this.metadata == null) {
                this.metadata = new DefaultColumnDefinition(this.fields.toArray(new Field[] {}));
            }
            XProtocolRow row = new XProtocolRow(this.metadata, Row.class.cast(message.getMessage()));
            this.callbacks.onRow(row);
            return false; /* done reading? */

        } else if (Error.class.equals(msgClass)) {
            XProtocolError e = new XProtocolError(Error.class.cast(message.getMessage()));
            this.callbacks.onException(e);
            return true; /* done reading? */

        } else if (Frame.class.equals(msgClass)) {
            this.okBuilder.addNotice(this.noticeFactory.createFromMessage(message));
            return false; /* done reading? */
        }

        this.callbacks.onException(new WrongArgumentException("Unhandled msg class (" + msgClass + ") + msg=" + message.getMessage()));
        return false; /* done reading? */ // note, this doesn't comply with the specified semantics ResultListener
    }

    public void closed() {
        this.callbacks.onException(new CJCommunicationsException("Socket was closed"));
    }

    public void error(Throwable ex) {
        this.callbacks.onException(ex);
    }

}
