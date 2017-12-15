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
import java.util.List;

import com.mysql.cj.api.x.io.MessageReader;
import com.mysql.cj.core.exceptions.WrongArgumentException;
import com.mysql.cj.x.core.CoreWarning;
import com.mysql.cj.x.core.StatementExecuteOk;
import com.mysql.cj.x.protobuf.MysqlxNotice.Frame;
import com.mysql.cj.x.protobuf.MysqlxNotice.SessionStateChanged;
import com.mysql.cj.x.protobuf.MysqlxNotice.Warning;

/**
 * Handle state necessary to accumulate noticed and build a {@link StatementExecuteOk} response.
 */
public class StatementExecuteOkBuilder {
    private long rowsAffected = 0;
    private Long lastInsertId = null;
    private List<com.mysql.cj.api.x.core.Warning> warnings = new ArrayList<>();

    public void addNotice(Frame notice) {
        if (notice.getType() == XProtocol.XProtocolNoticeFrameType_WARNING) {
            // TODO: shouldn't use DevApi WarningImpl class here
            this.warnings.add(new CoreWarning(MessageReader.parseNotice(notice.getPayload(), Warning.class)));
            // } else if (notice.getType() == MysqlxNoticeFrameType_SESS_VAR_CHANGED) {
            //     // TODO: ignored for now
            //     throw new RuntimeException("Got a session variable changed: " + notice);
        } else if (notice.getType() == XProtocol.XProtocolNoticeFrameType_SESS_STATE_CHANGED) {
            SessionStateChanged changeMsg = MessageReader.parseNotice(notice.getPayload(), SessionStateChanged.class);
            switch (changeMsg.getParam()) {
                case GENERATED_INSERT_ID:
                    // TODO: handle > 2^63-1?
                    this.lastInsertId = changeMsg.getValue().getVUnsignedInt();
                    break;
                case ROWS_AFFECTED:
                    // TODO: handle > 2^63-1?
                    this.rowsAffected = changeMsg.getValue().getVUnsignedInt();
                    break;
                case PRODUCED_MESSAGE:
                    // TODO do something with notices. expose them to client
                    //System.err.println("Ignoring NOTICE message: " + msg.getValue().getVString().getValue().toStringUtf8());
                    break;
                case CURRENT_SCHEMA:
                case ACCOUNT_EXPIRED:
                case ROWS_FOUND:
                case ROWS_MATCHED:
                case TRX_COMMITTED:
                case TRX_ROLLEDBACK:
                    // TODO: propagate state
                default:
                    // TODO: log warning normally instead of sysout
                    new WrongArgumentException("unhandled SessionStateChanged notice! " + notice).printStackTrace();
            }
        } else {
            // TODO log error normally instead of sysout
            new WrongArgumentException("Got an unknown notice: " + notice).printStackTrace();
        }
    }

    public StatementExecuteOk build() {
        return new StatementExecuteOk(this.rowsAffected, this.lastInsertId, this.warnings);
    }
}
