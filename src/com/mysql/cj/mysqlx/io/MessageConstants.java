/*
  Copyright (c) 2015, Oracle and/or its affiliates. All rights reserved.

  The MySQL Connector/J is licensed under the terms of the GPLv2
  <http://www.gnu.org/licenses/old-licenses/gpl-2.0.html>, like most MySQL Connectors.
  There are special exceptions to the terms and conditions of the GPLv2 as it is applied to
  this software, see the FLOSS License Exception
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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import com.google.protobuf.GeneratedMessage;
import com.google.protobuf.MessageLite;
import com.google.protobuf.Parser;

import static com.mysql.cj.mysqlx.protobuf.Mysqlx.ClientMessages;
import static com.mysql.cj.mysqlx.protobuf.Mysqlx.Error;
import static com.mysql.cj.mysqlx.protobuf.Mysqlx.Ok;
import static com.mysql.cj.mysqlx.protobuf.Mysqlx.ServerMessages;
import static com.mysql.cj.mysqlx.protobuf.MysqlxCrud.Delete;
import static com.mysql.cj.mysqlx.protobuf.MysqlxCrud.Find;
import static com.mysql.cj.mysqlx.protobuf.MysqlxCrud.Insert;
import static com.mysql.cj.mysqlx.protobuf.MysqlxCrud.Update;
import static com.mysql.cj.mysqlx.protobuf.MysqlxSession.AuthenticateContinue;
import static com.mysql.cj.mysqlx.protobuf.MysqlxSession.AuthenticateFail;
import static com.mysql.cj.mysqlx.protobuf.MysqlxSession.AuthenticateOk;
import static com.mysql.cj.mysqlx.protobuf.MysqlxSession.Close;
import static com.mysql.cj.mysqlx.protobuf.MysqlxSession.Reset;
import static com.mysql.cj.mysqlx.protobuf.MysqlxSql.ColumnMetaData;
import static com.mysql.cj.mysqlx.protobuf.MysqlxSql.ResultFetchDone;
import static com.mysql.cj.mysqlx.protobuf.MysqlxSql.Row;
import static com.mysql.cj.mysqlx.protobuf.MysqlxSql.StmtExecute;
import static com.mysql.cj.mysqlx.protobuf.MysqlxSql.StmtExecuteOk;
import static com.mysql.cj.mysqlx.protobuf.MysqlxSession.AuthenticateStart;

/**
 * Constants related to MySQL-X messages.
 */
public class MessageConstants {
    /**
     * Store a mapping of "ServerMessages" type tag to message parsers. This is used to get the de-serializer after reading the type tag.
     */
    public static final Map<Integer, Parser<? extends GeneratedMessage>> MESSAGE_TYPE_TO_PARSER;

    /**
     * Map of class to "ServerMessages" type tag for validation of parsed message class.
     * @todo Find a clever way to generate both maps with a single set of input pairs.
     */
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
         * ServerMessages mappings
         */
        Map<Integer, Parser<? extends GeneratedMessage>> messageTypeToParser = new HashMap<>();
        Map<Class<? extends GeneratedMessage>, Integer> messageClassToType = new HashMap<>();
        Map<Integer, Class<? extends GeneratedMessage>> messageTypeToClass = new HashMap<>();
        // To add support for new messages, add an entry to both maps
        messageTypeToParser.put(ServerMessages.Type.ERROR_VALUE, Error.getDefaultInstance().getParserForType());
        messageTypeToParser.put(ServerMessages.Type.OK_VALUE, Ok.getDefaultInstance().getParserForType());
        messageTypeToParser.put(ServerMessages.Type.SESS_AUTHENTICATE_CONTINUE_VALUE, AuthenticateContinue.getDefaultInstance().getParserForType());
        messageTypeToParser.put(ServerMessages.Type.SESS_AUTHENTICATE_FAIL_VALUE, AuthenticateFail.getDefaultInstance().getParserForType());
        messageTypeToParser.put(ServerMessages.Type.SESS_AUTHENTICATE_OK_VALUE, AuthenticateOk.getDefaultInstance().getParserForType());
        messageTypeToParser.put(ServerMessages.Type.SQL_COLUMN_META_DATA_VALUE, ColumnMetaData.getDefaultInstance().getParserForType());
        messageTypeToParser.put(ServerMessages.Type.SQL_RESULT_FETCH_DONE_VALUE, ResultFetchDone.getDefaultInstance().getParserForType());
        messageTypeToParser.put(ServerMessages.Type.SQL_ROW_VALUE, Row.getDefaultInstance().getParserForType());
        messageTypeToParser.put(ServerMessages.Type.SQL_STMT_EXECUTE_OK_VALUE, StmtExecuteOk.getDefaultInstance().getParserForType());
        messageClassToType.put(Error.class, ServerMessages.Type.ERROR_VALUE);
        messageClassToType.put(Ok.class, ServerMessages.Type.OK_VALUE);
        messageClassToType.put(AuthenticateFail.class, ServerMessages.Type.SESS_AUTHENTICATE_FAIL_VALUE);
        messageClassToType.put(AuthenticateOk.class, ServerMessages.Type.SESS_AUTHENTICATE_OK_VALUE);
        messageClassToType.put(ColumnMetaData.class, ServerMessages.Type.SQL_COLUMN_META_DATA_VALUE);
        messageClassToType.put(ResultFetchDone.class, ServerMessages.Type.SQL_RESULT_FETCH_DONE_VALUE);
        messageClassToType.put(Row.class, ServerMessages.Type.SQL_ROW_VALUE);
        messageClassToType.put(StmtExecuteOk.class, ServerMessages.Type.SQL_STMT_EXECUTE_OK_VALUE);
        for (Map.Entry<Class<? extends GeneratedMessage>, Integer> entry : messageClassToType.entrySet()) {
            messageTypeToClass.put(entry.getValue(), entry.getKey());
        }
        MESSAGE_TYPE_TO_PARSER = Collections.unmodifiableMap(messageTypeToParser);
        MESSAGE_CLASS_TO_TYPE = Collections.unmodifiableMap(messageClassToType);
        MESSAGE_TYPE_TO_CLASS = Collections.unmodifiableMap(messageTypeToClass);

        /*
         * ClientMessages mappings
         */
        Map<Class<? extends MessageLite>, Integer> messageClassToClientMessageType = new HashMap<>();
        messageClassToClientMessageType.put(AuthenticateStart.class, ClientMessages.Type.SESS_AUTHENTICATE_START_VALUE);
        messageClassToClientMessageType.put(AuthenticateContinue.class, ClientMessages.Type.SESS_AUTHENTICATE_CONTINUE_VALUE);
        messageClassToClientMessageType.put(Close.class, ClientMessages.Type.SESS_CLOSE_VALUE);
        messageClassToClientMessageType.put(Delete.class, ClientMessages.Type.CRUD_DELETE_VALUE);
        messageClassToClientMessageType.put(Find.class, ClientMessages.Type.CRUD_FIND_VALUE);
        messageClassToClientMessageType.put(Insert.class, ClientMessages.Type.CRUD_INSERT_VALUE);
        messageClassToClientMessageType.put(Reset.class, ClientMessages.Type.SESS_RESET_VALUE);
        messageClassToClientMessageType.put(StmtExecute.class, ClientMessages.Type.SQL_STMT_EXECUTE_VALUE);
        messageClassToClientMessageType.put(Update.class, ClientMessages.Type.CRUD_UPDATE_VALUE);
        MESSAGE_CLASS_TO_CLIENT_MESSAGE_TYPE = Collections.unmodifiableMap(messageClassToClientMessageType);
    }
}
