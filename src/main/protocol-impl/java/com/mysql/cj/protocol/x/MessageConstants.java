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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import com.google.protobuf.GeneratedMessage;
import com.google.protobuf.MessageLite;
import com.google.protobuf.Parser;
import com.mysql.cj.exceptions.AssertionFailedException;
import com.mysql.cj.exceptions.WrongArgumentException;
import com.mysql.cj.x.protobuf.Mysqlx.ClientMessages;
import com.mysql.cj.x.protobuf.Mysqlx.Error;
import com.mysql.cj.x.protobuf.Mysqlx.Ok;
import com.mysql.cj.x.protobuf.Mysqlx.ServerMessages;
import com.mysql.cj.x.protobuf.MysqlxConnection.Capabilities;
import com.mysql.cj.x.protobuf.MysqlxConnection.CapabilitiesGet;
import com.mysql.cj.x.protobuf.MysqlxConnection.CapabilitiesSet;
import com.mysql.cj.x.protobuf.MysqlxCrud.CreateView;
import com.mysql.cj.x.protobuf.MysqlxCrud.Delete;
import com.mysql.cj.x.protobuf.MysqlxCrud.DropView;
import com.mysql.cj.x.protobuf.MysqlxCrud.Find;
import com.mysql.cj.x.protobuf.MysqlxCrud.Insert;
import com.mysql.cj.x.protobuf.MysqlxCrud.ModifyView;
import com.mysql.cj.x.protobuf.MysqlxCrud.Update;
import com.mysql.cj.x.protobuf.MysqlxNotice.Frame;
import com.mysql.cj.x.protobuf.MysqlxNotice.SessionStateChanged;
import com.mysql.cj.x.protobuf.MysqlxNotice.SessionVariableChanged;
import com.mysql.cj.x.protobuf.MysqlxNotice.Warning;
import com.mysql.cj.x.protobuf.MysqlxResultset.ColumnMetaData;
import com.mysql.cj.x.protobuf.MysqlxResultset.FetchDone;
import com.mysql.cj.x.protobuf.MysqlxResultset.FetchDoneMoreResultsets;
import com.mysql.cj.x.protobuf.MysqlxResultset.Row;
import com.mysql.cj.x.protobuf.MysqlxSession.AuthenticateContinue;
import com.mysql.cj.x.protobuf.MysqlxSession.AuthenticateOk;
import com.mysql.cj.x.protobuf.MysqlxSession.AuthenticateStart;
import com.mysql.cj.x.protobuf.MysqlxSession.Close;
import com.mysql.cj.x.protobuf.MysqlxSession.Reset;
import com.mysql.cj.x.protobuf.MysqlxSql.StmtExecute;
import com.mysql.cj.x.protobuf.MysqlxSql.StmtExecuteOk;

/**
 * Constants related to X Protocol messages.
 */
public class MessageConstants {
    /**
     * Store a mapping of "ServerMessages" class to message parsers. This is used to get the de-serializer after reading the type tag.
     */
    public static final Map<Class<? extends GeneratedMessage>, Parser<? extends GeneratedMessage>> MESSAGE_CLASS_TO_PARSER;

    /**
     * Map of class to "ServerMessages" type tag for validation of parsed message class.
     */
    // TODO Find a clever way to generate both maps with a single set of input pairs.
    public static final Map<Class<? extends GeneratedMessage>, Integer> MESSAGE_CLASS_TO_TYPE;

    /**
     * Map of "ServerMessages" type tag to class.
     */
    public static final Map<Integer, Class<? extends GeneratedMessage>> MESSAGE_TYPE_TO_CLASS;

    /**
     * Store a mapping of message class to "ClientMessages" type tag. This is used to generate the header when sending a message.
     */
    public static final Map<Class<? extends MessageLite>, Integer> MESSAGE_CLASS_TO_CLIENT_MESSAGE_TYPE;

    static {
        /*
         * ServerMessages mappings (including embedded noticed messages with no entry in ServerMessages)
         */
        Map<Class<? extends GeneratedMessage>, Parser<? extends GeneratedMessage>> messageClassToParser = new HashMap<>();
        Map<Class<? extends GeneratedMessage>, Integer> messageClassToType = new HashMap<>();
        Map<Integer, Class<? extends GeneratedMessage>> messageTypeToClass = new HashMap<>();
        // To add support for new messages, add an entry to both maps
        messageClassToParser.put(Error.class, Error.getDefaultInstance().getParserForType());
        messageClassToParser.put(Ok.class, Ok.getDefaultInstance().getParserForType());
        messageClassToParser.put(AuthenticateContinue.class, AuthenticateContinue.getDefaultInstance().getParserForType());
        messageClassToParser.put(AuthenticateOk.class, AuthenticateOk.getDefaultInstance().getParserForType());
        messageClassToParser.put(Capabilities.class, Capabilities.getDefaultInstance().getParserForType());
        messageClassToParser.put(ColumnMetaData.class, ColumnMetaData.getDefaultInstance().getParserForType());
        messageClassToParser.put(FetchDone.class, FetchDone.getDefaultInstance().getParserForType());
        messageClassToParser.put(FetchDoneMoreResultsets.class, FetchDoneMoreResultsets.getDefaultInstance().getParserForType());
        messageClassToParser.put(Frame.class, Frame.getDefaultInstance().getParserForType());
        messageClassToParser.put(Row.class, Row.getDefaultInstance().getParserForType());
        messageClassToParser.put(StmtExecuteOk.class, StmtExecuteOk.getDefaultInstance().getParserForType());
        // embedded notices (no type tags)
        messageClassToParser.put(SessionStateChanged.class, SessionStateChanged.getDefaultInstance().getParserForType());
        messageClassToParser.put(SessionVariableChanged.class, SessionVariableChanged.getDefaultInstance().getParserForType());
        messageClassToParser.put(Warning.class, Warning.getDefaultInstance().getParserForType());

        messageClassToType.put(Error.class, ServerMessages.Type.ERROR_VALUE);
        messageClassToType.put(Ok.class, ServerMessages.Type.OK_VALUE);
        messageClassToType.put(AuthenticateContinue.class, ServerMessages.Type.SESS_AUTHENTICATE_CONTINUE_VALUE);
        messageClassToType.put(AuthenticateOk.class, ServerMessages.Type.SESS_AUTHENTICATE_OK_VALUE);
        messageClassToType.put(Capabilities.class, ServerMessages.Type.CONN_CAPABILITIES_VALUE);
        messageClassToType.put(ColumnMetaData.class, ServerMessages.Type.RESULTSET_COLUMN_META_DATA_VALUE);
        messageClassToType.put(FetchDone.class, ServerMessages.Type.RESULTSET_FETCH_DONE_VALUE);
        messageClassToType.put(FetchDoneMoreResultsets.class, ServerMessages.Type.RESULTSET_FETCH_DONE_MORE_RESULTSETS_VALUE);
        messageClassToType.put(Frame.class, ServerMessages.Type.NOTICE_VALUE);
        messageClassToType.put(Row.class, ServerMessages.Type.RESULTSET_ROW_VALUE);
        messageClassToType.put(StmtExecuteOk.class, ServerMessages.Type.SQL_STMT_EXECUTE_OK_VALUE);
        for (Map.Entry<Class<? extends GeneratedMessage>, Integer> entry : messageClassToType.entrySet()) {
            messageTypeToClass.put(entry.getValue(), entry.getKey());
        }
        MESSAGE_CLASS_TO_PARSER = Collections.unmodifiableMap(messageClassToParser);
        MESSAGE_CLASS_TO_TYPE = Collections.unmodifiableMap(messageClassToType);
        MESSAGE_TYPE_TO_CLASS = Collections.unmodifiableMap(messageTypeToClass);

        /*
         * ClientMessages mappings
         */
        Map<Class<? extends MessageLite>, Integer> messageClassToClientMessageType = new HashMap<>();
        messageClassToClientMessageType.put(AuthenticateStart.class, ClientMessages.Type.SESS_AUTHENTICATE_START_VALUE);
        messageClassToClientMessageType.put(AuthenticateContinue.class, ClientMessages.Type.SESS_AUTHENTICATE_CONTINUE_VALUE);
        messageClassToClientMessageType.put(CapabilitiesGet.class, ClientMessages.Type.CON_CAPABILITIES_GET_VALUE);
        messageClassToClientMessageType.put(CapabilitiesSet.class, ClientMessages.Type.CON_CAPABILITIES_SET_VALUE);
        messageClassToClientMessageType.put(Close.class, ClientMessages.Type.SESS_CLOSE_VALUE);
        messageClassToClientMessageType.put(Delete.class, ClientMessages.Type.CRUD_DELETE_VALUE);
        messageClassToClientMessageType.put(Find.class, ClientMessages.Type.CRUD_FIND_VALUE);
        messageClassToClientMessageType.put(Insert.class, ClientMessages.Type.CRUD_INSERT_VALUE);
        messageClassToClientMessageType.put(Reset.class, ClientMessages.Type.SESS_RESET_VALUE);
        messageClassToClientMessageType.put(StmtExecute.class, ClientMessages.Type.SQL_STMT_EXECUTE_VALUE);
        messageClassToClientMessageType.put(Update.class, ClientMessages.Type.CRUD_UPDATE_VALUE);
        messageClassToClientMessageType.put(CreateView.class, ClientMessages.Type.CRUD_CREATE_VIEW_VALUE);
        messageClassToClientMessageType.put(ModifyView.class, ClientMessages.Type.CRUD_MODIFY_VIEW_VALUE);
        messageClassToClientMessageType.put(DropView.class, ClientMessages.Type.CRUD_DROP_VIEW_VALUE);
        MESSAGE_CLASS_TO_CLIENT_MESSAGE_TYPE = Collections.unmodifiableMap(messageClassToClientMessageType);
    }

    /**
     * Lookup the "ClientMessages" type tag for a protobuf message class.
     * 
     * @param msgClass
     *            message class extending {@link MessageLite}
     * @return type tag for this message class
     */
    public static int getTypeForMessageClass(Class<? extends MessageLite> msgClass) {
        Integer tag = MESSAGE_CLASS_TO_CLIENT_MESSAGE_TYPE.get(msgClass);
        if (tag == null) {
            throw new WrongArgumentException("No mapping to ClientMessages for message class " + msgClass.getSimpleName());
        }
        return tag;
    }

    public static Class<? extends GeneratedMessage> getMessageClassForType(int type) {
        Class<? extends GeneratedMessage> messageClass = MessageConstants.MESSAGE_TYPE_TO_CLASS.get(type);
        if (messageClass == null) {
            // check if there's a mapping that we don't explicitly handle
            ServerMessages.Type serverMessageMapping = ServerMessages.Type.valueOf(type);
            throw AssertionFailedException.shouldNotHappen("Unknown message type: " + type + " (server messages mapping: " + serverMessageMapping + ")");
        }
        return messageClass;
    }
}
