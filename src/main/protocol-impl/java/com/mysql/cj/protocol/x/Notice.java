/*
 * Copyright (c) 2018, 2023, Oracle and/or its affiliates.
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

import java.util.List;

import com.google.protobuf.ByteString;
import com.google.protobuf.GeneratedMessageV3;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Parser;
import com.mysql.cj.exceptions.CJCommunicationsException;
import com.mysql.cj.protocol.ProtocolEntity;
import com.mysql.cj.protocol.Warning;
import com.mysql.cj.x.protobuf.MysqlxDatatypes.Scalar;
import com.mysql.cj.x.protobuf.MysqlxNotice.Frame;
import com.mysql.cj.x.protobuf.MysqlxNotice.SessionStateChanged;
import com.mysql.cj.x.protobuf.MysqlxNotice.SessionVariableChanged;

/**
 * ProtocolEntity representing a {@link Notice} message.
 */
public class Notice implements ProtocolEntity {

    public static Notice getInstance(XMessage message) {
        Frame notice = (Frame) message.getMessage();
        switch (notice.getType()) {
            case Frame.Type.WARNING_VALUE:
                return new XWarning(notice);

            case Frame.Type.SESSION_VARIABLE_CHANGED_VALUE:
                return new XSessionVariableChanged(notice);

            case Frame.Type.SESSION_STATE_CHANGED_VALUE:
                return new XSessionStateChanged(notice);

            case Frame.Type.GROUP_REPLICATION_STATE_CHANGED_VALUE:
                // TODO
                break;
            default:
                break;
        }
        return new Notice(notice);
    }

    public static final int NoticeScope_Global = 1;
    public static final int NoticeScope_Local = 2;

    public static final int NoticeType_WARNING = 1;
    public static final int NoticeType_SESSION_VARIABLE_CHANGED = 2;
    public static final int NoticeType_SESSION_STATE_CHANGED = 3;
    public static final int NoticeType_GROUP_REPLICATION_STATE_CHANGED = 4;

    public static final int SessionStateChanged_CURRENT_SCHEMA = 1;
    public static final int SessionStateChanged_ACCOUNT_EXPIRED = 2;
    public static final int SessionStateChanged_GENERATED_INSERT_ID = 3;
    public static final int SessionStateChanged_ROWS_AFFECTED = 4;
    public static final int SessionStateChanged_ROWS_FOUND = 5;
    public static final int SessionStateChanged_ROWS_MATCHED = 6;
    public static final int SessionStateChanged_TRX_COMMITTED = 7;
    public static final int SessionStateChanged_TRX_ROLLEDBACK = 9;
    public static final int SessionStateChanged_PRODUCED_MESSAGE = 10;
    public static final int SessionStateChanged_CLIENT_ID_ASSIGNED = 11;
    public static final int SessionStateChanged_GENERATED_DOCUMENT_IDS = 12;

    protected int scope = 0;
    protected int type = 0;

    public Notice(Frame frm) {
        this.scope = frm.getScope().getNumber();
        this.type = frm.getType();
    }

    public int getType() {
        return this.type;
    }

    public int getScope() {
        return this.scope;
    }

    @SuppressWarnings("unchecked")
    static <T extends GeneratedMessageV3> T parseNotice(ByteString payload, Class<T> noticeClass) {
        try {
            Parser<T> parser = (Parser<T>) MessageConstants.MESSAGE_CLASS_TO_PARSER.get(noticeClass);
            return parser.parseFrom(payload);
        } catch (InvalidProtocolBufferException ex) {
            throw new CJCommunicationsException(ex);
        }
    }

    public static class XWarning extends Notice implements Warning {

        private int level;
        private long code;
        private String message;

        public XWarning(Frame frm) {
            super(frm);
            com.mysql.cj.x.protobuf.MysqlxNotice.Warning warn = parseNotice(frm.getPayload(), com.mysql.cj.x.protobuf.MysqlxNotice.Warning.class);
            this.level = warn.getLevel().getNumber();
            this.code = Integer.toUnsignedLong(warn.getCode());
            this.message = warn.getMsg();
        }

        @Override
        public int getLevel() {
            return this.level;
        }

        @Override
        public long getCode() {
            return this.code;
        }

        @Override
        public String getMessage() {
            return this.message;
        }

    }

    public static class XSessionVariableChanged extends Notice {

        private String paramName = null;
        private Scalar value = null;

        public XSessionVariableChanged(Frame frm) {
            super(frm);
            SessionVariableChanged svmsg = parseNotice(frm.getPayload(), SessionVariableChanged.class);
            this.paramName = svmsg.getParam();
            this.value = svmsg.getValue();
        }

        public String getParamName() {
            return this.paramName;
        }

        public Scalar getValue() {
            return this.value;
        }

    }

    public static class XSessionStateChanged extends Notice {

        private Integer paramType = null;
        private List<Scalar> valueList = null;

        public XSessionStateChanged(Frame frm) {
            super(frm);
            SessionStateChanged ssmsg = parseNotice(frm.getPayload(), SessionStateChanged.class);
            this.paramType = ssmsg.getParam().getNumber();
            this.valueList = ssmsg.getValueList();
        }

        public Integer getParamType() {
            return this.paramType;
        }

        public List<Scalar> getValueList() {
            return this.valueList;
        }

        public Scalar getValue() {
            if (this.valueList != null && !this.valueList.isEmpty()) {
                return this.valueList.get(0);
            }
            return null;
        }

    }

}
