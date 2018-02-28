/*
 * Copyright (c) 2017, 2018, Oracle and/or its affiliates. All rights reserved.
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

package com.mysql.cj.protocol.a;

import java.util.List;

import com.mysql.cj.MessageBuilder;
import com.mysql.cj.exceptions.CJOperationNotSupportedException;
import com.mysql.cj.exceptions.ExceptionFactory;
import com.mysql.cj.protocol.a.NativeConstants.IntegerDataType;
import com.mysql.cj.protocol.a.NativeConstants.StringLengthDataType;
import com.mysql.cj.util.StringUtils;

public class NativeMessageBuilder implements MessageBuilder<NativePacketPayload> {

    @Override
    public NativePacketPayload buildSqlStatement(String statement) {
        throw ExceptionFactory.createException(CJOperationNotSupportedException.class, "Not supported");
    }

    @Override
    public NativePacketPayload buildSqlStatement(String statement, List<Object> args) {
        throw ExceptionFactory.createException(CJOperationNotSupportedException.class, "Not supported");
    }

    @Override
    public NativePacketPayload buildClose() {
        throw ExceptionFactory.createException(CJOperationNotSupportedException.class, "Not supported");
    }

    public NativePacketPayload buildComQuery(NativePacketPayload sharedPacket, byte[] query) {
        NativePacketPayload packet = sharedPacket != null ? sharedPacket : new NativePacketPayload(query.length + 1);
        packet.writeInteger(IntegerDataType.INT1, NativeConstants.COM_QUERY);
        packet.writeBytes(StringLengthDataType.STRING_FIXED, query);
        return packet;
    }

    public NativePacketPayload buildComQuery(NativePacketPayload sharedPacket, String query) {
        return buildComQuery(sharedPacket, StringUtils.getBytes(query));
    }

    public NativePacketPayload buildComQuery(NativePacketPayload sharedPacket, String query, String encoding) {
        return buildComQuery(sharedPacket, StringUtils.getBytes(query, encoding));
    }

    public NativePacketPayload buildComInitDb(NativePacketPayload sharedPacket, byte[] dbName) {
        NativePacketPayload packet = sharedPacket != null ? sharedPacket : new NativePacketPayload(dbName.length + 1);
        packet.writeInteger(IntegerDataType.INT1, NativeConstants.COM_INIT_DB);
        packet.writeBytes(StringLengthDataType.STRING_FIXED, dbName);
        return packet;
    }

    public NativePacketPayload buildComInitDb(NativePacketPayload sharedPacket, String dbName) {
        return buildComInitDb(sharedPacket, StringUtils.getBytes(dbName));
    }

    public NativePacketPayload buildComShutdown(NativePacketPayload sharedPacket) {
        NativePacketPayload packet = sharedPacket != null ? sharedPacket : new NativePacketPayload(1);
        packet.writeInteger(IntegerDataType.INT1, NativeConstants.COM_SHUTDOWN);
        return packet;
    }

    public NativePacketPayload buildComSetOption(NativePacketPayload sharedPacket, int val) {
        NativePacketPayload packet = sharedPacket != null ? sharedPacket : new NativePacketPayload(3);
        packet.writeInteger(IntegerDataType.INT1, NativeConstants.COM_SET_OPTION);
        packet.writeInteger(IntegerDataType.INT2, val);
        return packet;
    }

    public NativePacketPayload buildComPing(NativePacketPayload sharedPacket) {
        NativePacketPayload packet = sharedPacket != null ? sharedPacket : new NativePacketPayload(1);
        packet.writeInteger(IntegerDataType.INT1, NativeConstants.COM_PING);
        return packet;
    }

    public NativePacketPayload buildComQuit(NativePacketPayload sharedPacket) {
        NativePacketPayload packet = sharedPacket != null ? sharedPacket : new NativePacketPayload(1);
        packet.writeInteger(IntegerDataType.INT1, NativeConstants.COM_QUIT);
        return packet;
    }

    public NativePacketPayload buildComStmtPrepare(NativePacketPayload sharedPacket, byte[] query) {
        NativePacketPayload packet = sharedPacket != null ? sharedPacket : new NativePacketPayload(query.length + 1);
        packet.writeInteger(IntegerDataType.INT1, NativeConstants.COM_STMT_PREPARE);
        packet.writeBytes(StringLengthDataType.STRING_FIXED, query);
        return packet;
    }

    public NativePacketPayload buildComStmtPrepare(NativePacketPayload sharedPacket, String queryString, String characterEncoding) {
        return buildComStmtPrepare(sharedPacket, StringUtils.getBytes(queryString, characterEncoding));
    }

    public NativePacketPayload buildComStmtClose(NativePacketPayload sharedPacket, long serverStatementId) {
        NativePacketPayload packet = sharedPacket != null ? sharedPacket : new NativePacketPayload(5);
        packet.writeInteger(IntegerDataType.INT1, NativeConstants.COM_STMT_CLOSE);
        packet.writeInteger(IntegerDataType.INT4, serverStatementId);
        return packet;
    }

    public NativePacketPayload buildComStmtReset(NativePacketPayload sharedPacket, long serverStatementId) {
        NativePacketPayload packet = sharedPacket != null ? sharedPacket : new NativePacketPayload(5);
        packet.writeInteger(IntegerDataType.INT1, NativeConstants.COM_STMT_RESET);
        packet.writeInteger(IntegerDataType.INT4, serverStatementId);
        return packet;
    }

    public NativePacketPayload buildComStmtFetch(NativePacketPayload sharedPacket, long serverStatementId, long numRowsToFetch) {
        NativePacketPayload packet = sharedPacket != null ? sharedPacket : new NativePacketPayload(9);
        packet.writeInteger(IntegerDataType.INT1, NativeConstants.COM_STMT_FETCH);
        packet.writeInteger(IntegerDataType.INT4, serverStatementId);
        packet.writeInteger(IntegerDataType.INT4, numRowsToFetch);
        return packet;
    }

    public NativePacketPayload buildComStmtSendLongData(NativePacketPayload sharedPacket, long serverStatementId, int parameterIndex, byte[] longData) {
        NativePacketPayload packet = sharedPacket != null ? sharedPacket : new NativePacketPayload(9);
        packet.writeInteger(IntegerDataType.INT1, NativeConstants.COM_STMT_SEND_LONG_DATA);
        packet.writeInteger(IntegerDataType.INT4, serverStatementId);
        packet.writeInteger(IntegerDataType.INT2, parameterIndex);
        packet.writeBytes(StringLengthDataType.STRING_FIXED, longData);
        return packet;
    }

}
