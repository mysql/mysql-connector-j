/*
 * Copyright (c) 2018, Oracle and/or its affiliates. All rights reserved.
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

import com.google.protobuf.ByteString;
import com.google.protobuf.GeneratedMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Parser;
import com.mysql.cj.exceptions.CJCommunicationsException;
import com.mysql.cj.exceptions.WrongArgumentException;
import com.mysql.cj.protocol.ProtocolEntityFactory;
import com.mysql.cj.x.protobuf.MysqlxNotice.Frame;
import com.mysql.cj.x.protobuf.MysqlxNotice.SessionStateChanged;
import com.mysql.cj.x.protobuf.MysqlxNotice.SessionVariableChanged;

public class NoticeFactory implements ProtocolEntityFactory<Notice, XMessage> {

    @Override
    public Notice createFromMessage(XMessage message) {
        Frame notice = (Frame) message.getMessage();
        switch (notice.getType()) {
            case Notice.XProtocolNoticeFrameType_WARNING:
                com.mysql.cj.x.protobuf.MysqlxNotice.Warning warn = parseNotice((notice).getPayload(), com.mysql.cj.x.protobuf.MysqlxNotice.Warning.class);
                return new Notice(warn.getLevel().getNumber(), Integer.toUnsignedLong(warn.getCode()), warn.getMsg());

            case Notice.XProtocolNoticeFrameType_SESS_VAR_CHANGED:
                SessionVariableChanged svmsg = parseNotice(notice.getPayload(), SessionVariableChanged.class);
                return new Notice(svmsg.getParam(), svmsg.getValue());

            case Notice.XProtocolNoticeFrameType_SESS_STATE_CHANGED:
                SessionStateChanged ssmsg = parseNotice(notice.getPayload(), SessionStateChanged.class);
                return new Notice(ssmsg.getParam().getNumber(), ssmsg.getValueList());

            default:
                // TODO log error normally instead of sysout
                new WrongArgumentException("Got an unknown notice: " + notice).printStackTrace();
                break;
        }
        return null;
    }

    @SuppressWarnings("unchecked")
    private <T extends GeneratedMessage> T parseNotice(ByteString payload, Class<T> noticeClass) {
        try {
            Parser<T> parser = (Parser<T>) MessageConstants.MESSAGE_CLASS_TO_PARSER.get(noticeClass);
            return parser.parseFrom(payload);
        } catch (InvalidProtocolBufferException ex) {
            throw new CJCommunicationsException(ex);
        }
    }
}
