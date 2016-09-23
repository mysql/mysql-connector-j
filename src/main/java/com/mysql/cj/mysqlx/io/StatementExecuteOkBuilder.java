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

import java.util.ArrayList;
import java.util.List;

import com.mysql.cj.core.exceptions.WrongArgumentException;
import com.mysql.cj.core.io.StatementExecuteOk;
import com.mysql.cj.mysqlx.devapi.WarningImpl;
import com.mysql.cj.mysqlx.protobuf.MysqlxNotice.Frame;
import com.mysql.cj.mysqlx.protobuf.MysqlxNotice.SessionStateChanged;
import com.mysql.cj.mysqlx.protobuf.MysqlxNotice.Warning;

/**
 * Handle state necessary to accumulate noticed and build a {@link StatementExecuteOk} response.
 */
public class StatementExecuteOkBuilder {
    private long rowsAffected = 0;
    private Long lastInsertId = null;
    // TODO: don't use DevApi interfaces here!
    private List<com.mysql.cj.api.x.Warning> warnings = new ArrayList<>();

    public void addNotice(Frame notice) {
        if (notice.getType() == MysqlxProtocol.MysqlxNoticeFrameType_WARNING) {
            // TODO: shouldn't use DevApi WarningImpl class here
            warnings.add(new WarningImpl(MessageReader.parseNotice(notice.getPayload(), Warning.class)));
            // } else if (notice.getType() == MysqlxNoticeFrameType_SESS_VAR_CHANGED) {
            //     // TODO: ignored for now
            //     throw new RuntimeException("Got a session variable changed: " + notice);
        } else if (notice.getType() == MysqlxProtocol.MysqlxNoticeFrameType_SESS_STATE_CHANGED) {
            SessionStateChanged changeMsg = MessageReader.parseNotice(notice.getPayload(), SessionStateChanged.class);
            switch (changeMsg.getParam()) {
                case GENERATED_INSERT_ID:
                    // TODO: handle > 2^63-1?
                    lastInsertId = changeMsg.getValue().getVUnsignedInt();
                    break;
                case ROWS_AFFECTED:
                    // TODO: handle > 2^63-1?
                    rowsAffected = changeMsg.getValue().getVUnsignedInt();
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
                    // TODO: log warning
                    new WrongArgumentException("unhandled SessionStateChanged notice! " + notice).printStackTrace();
            }
        } else {
            // TODO: error?
            new WrongArgumentException("Got an unknown notice: " + notice).printStackTrace();
        }
    }

    public StatementExecuteOk build() {
        return new StatementExecuteOk(this.rowsAffected, this.lastInsertId, this.warnings);
    }
}
