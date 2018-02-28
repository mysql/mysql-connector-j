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

import com.mysql.cj.protocol.Warning;
import com.mysql.cj.x.protobuf.MysqlxDatatypes.Scalar;

public class Notice implements Warning {

    public static final int XProtocolNoticeFrameType_WARNING = 1;
    public static final int XProtocolNoticeFrameType_SESS_VAR_CHANGED = 2;
    public static final int XProtocolNoticeFrameType_SESS_STATE_CHANGED = 3;

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

    private int noticeType = 0;

    private int level;
    private long code;
    private String message;

    private Integer paramType = null;
    private String paramName = null;
    private Scalar value = null;

    /**
     * Constructor for XProtocolNoticeFrameType_WARNING
     * 
     * @param level
     * @param code
     * @param message
     */
    public Notice(int level, long code, String message) {
        this.noticeType = XProtocolNoticeFrameType_WARNING;
        this.level = level;
        this.code = code;
        this.message = message;
    }

    /**
     * Constructor for XProtocolNoticeFrameType_SESS_STATE_CHANGED
     * 
     * @param paramType
     * @param value
     */
    public Notice(int paramType, Scalar value) {
        this.noticeType = XProtocolNoticeFrameType_SESS_STATE_CHANGED;
        this.paramType = paramType;
        this.value = value;
    }

    /**
     * Constructor for XProtocolNoticeFrameType_SESS_VAR_CHANGED
     * 
     * @param paramName
     * @param value
     */
    public Notice(String paramName, Scalar value) {
        this.noticeType = XProtocolNoticeFrameType_SESS_VAR_CHANGED;
        this.paramName = paramName;
        this.value = value;
    }

    public int getType() {
        return this.noticeType;
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

    public Integer getParamType() {
        return this.paramType;
    }

    public String getParamName() {
        return this.paramName;
    }

    public Scalar getValue() {
        return this.value;
    }

}
