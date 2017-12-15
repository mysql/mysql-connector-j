/*
 * Copyright (c) 2017, Oracle and/or its affiliates. All rights reserved.
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

package com.mysql.cj.mysqla.io;

import com.mysql.cj.api.mysqla.io.NativeProtocol.IntegerDataType;
import com.mysql.cj.api.mysqla.io.NativeProtocol.StringLengthDataType;
import com.mysql.cj.api.mysqla.io.PacketPayload;
import com.mysql.cj.core.util.StringUtils;
import com.mysql.cj.mysqla.MysqlaConstants;

public class CommandBuilder {

    // TODO unify CommandBuilder and MessageBuilder
    public CommandBuilder() {
    }

    public PacketPayload buildComQuery(PacketPayload sharedPacket, byte[] query) {
        PacketPayload packet = sharedPacket != null ? sharedPacket : new Buffer(query.length + 1);
        packet.writeInteger(IntegerDataType.INT1, MysqlaConstants.COM_QUERY);
        packet.writeBytes(StringLengthDataType.STRING_FIXED, query);
        return packet;
    }

    public PacketPayload buildComQuery(PacketPayload sharedPacket, String query) {
        return buildComQuery(sharedPacket, StringUtils.getBytes(query));
    }

    public PacketPayload buildComQuery(PacketPayload sharedPacket, String query, String encoding) {
        return buildComQuery(sharedPacket, StringUtils.getBytes(query, encoding));
    }

    public PacketPayload buildComInitDb(PacketPayload sharedPacket, byte[] dbName) {
        PacketPayload packet = sharedPacket != null ? sharedPacket : new Buffer(dbName.length + 1);
        packet.writeInteger(IntegerDataType.INT1, MysqlaConstants.COM_INIT_DB);
        packet.writeBytes(StringLengthDataType.STRING_FIXED, dbName);
        return packet;
    }

    public PacketPayload buildComInitDb(PacketPayload sharedPacket, String dbName) {
        return buildComInitDb(sharedPacket, StringUtils.getBytes(dbName));
    }

    public PacketPayload buildComShutdown(PacketPayload sharedPacket) {
        PacketPayload packet = sharedPacket != null ? sharedPacket : new Buffer(1);
        packet.writeInteger(IntegerDataType.INT1, MysqlaConstants.COM_SHUTDOWN);
        return packet;
    }

    public PacketPayload buildComSetOption(PacketPayload sharedPacket, int val) {
        PacketPayload packet = sharedPacket != null ? sharedPacket : new Buffer(3);
        packet.writeInteger(IntegerDataType.INT1, MysqlaConstants.COM_SET_OPTION);
        packet.writeInteger(IntegerDataType.INT2, val);
        return packet;
    }

    public PacketPayload buildComPing(PacketPayload sharedPacket) {
        PacketPayload packet = sharedPacket != null ? sharedPacket : new Buffer(1);
        packet.writeInteger(IntegerDataType.INT1, MysqlaConstants.COM_PING);
        return packet;
    }

    public PacketPayload buildComQuit(PacketPayload sharedPacket) {
        PacketPayload packet = sharedPacket != null ? sharedPacket : new Buffer(1);
        packet.writeInteger(IntegerDataType.INT1, MysqlaConstants.COM_QUIT);
        return packet;
    }

    public PacketPayload buildComStmtPrepare(PacketPayload sharedPacket, byte[] query) {
        PacketPayload packet = sharedPacket != null ? sharedPacket : new Buffer(query.length + 1);
        packet.writeInteger(IntegerDataType.INT1, MysqlaConstants.COM_STMT_PREPARE);
        packet.writeBytes(StringLengthDataType.STRING_FIXED, query);
        return packet;
    }

    public PacketPayload buildComStmtPrepare(PacketPayload sharedPacket, String queryString, String characterEncoding) {
        return buildComStmtPrepare(sharedPacket, StringUtils.getBytes(queryString, characterEncoding));
    }

    public PacketPayload buildComStmtClose(PacketPayload sharedPacket, long serverStatementId) {
        PacketPayload packet = sharedPacket != null ? sharedPacket : new Buffer(5);
        packet.writeInteger(IntegerDataType.INT1, MysqlaConstants.COM_STMT_CLOSE);
        packet.writeInteger(IntegerDataType.INT4, serverStatementId);
        return packet;
    }

    public PacketPayload buildComStmtReset(PacketPayload sharedPacket, long serverStatementId) {
        PacketPayload packet = sharedPacket != null ? sharedPacket : new Buffer(5);
        packet.writeInteger(IntegerDataType.INT1, MysqlaConstants.COM_STMT_RESET);
        packet.writeInteger(IntegerDataType.INT4, serverStatementId);
        return packet;
    }

    public PacketPayload buildComStmtFetch(PacketPayload sharedPacket, long serverStatementId, long numRowsToFetch) {
        PacketPayload packet = sharedPacket != null ? sharedPacket : new Buffer(9);
        packet.writeInteger(IntegerDataType.INT1, MysqlaConstants.COM_STMT_FETCH);
        packet.writeInteger(IntegerDataType.INT4, serverStatementId);
        packet.writeInteger(IntegerDataType.INT4, numRowsToFetch);
        return packet;
    }

    public PacketPayload buildComStmtSendLongData(PacketPayload sharedPacket, long serverStatementId, int parameterIndex, byte[] longData) {
        PacketPayload packet = sharedPacket != null ? sharedPacket : new Buffer(9);
        packet.writeInteger(IntegerDataType.INT1, MysqlaConstants.COM_STMT_SEND_LONG_DATA);
        packet.writeInteger(IntegerDataType.INT4, serverStatementId);
        packet.writeInteger(IntegerDataType.INT2, parameterIndex);
        packet.writeBytes(StringLengthDataType.STRING_FIXED, longData);
        return packet;
    }

}
