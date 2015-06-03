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

package com.mysql.cj.mysqlx;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import com.google.protobuf.GeneratedMessage;
import com.google.protobuf.Parser;

import static com.mysql.cj.mysqlx.protobuf.Mysqlx.Error;
import static com.mysql.cj.mysqlx.protobuf.Mysqlx.Ok;
import static com.mysql.cj.mysqlx.protobuf.Mysqlx.ServerMessages;
import static com.mysql.cj.mysqlx.protobuf.MysqlxSession.AuthenticateOk;
import static com.mysql.cj.mysqlx.protobuf.MysqlxSql.ColumnMetaData;
import static com.mysql.cj.mysqlx.protobuf.MysqlxSql.CursorFetchDone;
import static com.mysql.cj.mysqlx.protobuf.MysqlxSql.Row;
import static com.mysql.cj.mysqlx.protobuf.MysqlxSql.StmtExecuteOk;
import com.mysql.cj.core.io.FullReadInputStream;

/**
 * Low-level message reader for MySQL-X protocol.
 */
public class MessageReader {
    /**
     * Store a mapping of "ServerMessages" type tag to message parsers. This is used to get the de-serializer after reading the type tag.
     */
    private static Map<Integer, Parser<? extends GeneratedMessage>> messageTypeToParser = new HashMap<>();

    static {
        messageTypeToParser.put(ServerMessages.Type.ERROR_VALUE, Error.getDefaultInstance().getParserForType());
        messageTypeToParser.put(ServerMessages.Type.OK_VALUE, Ok.getDefaultInstance().getParserForType());
        messageTypeToParser.put(ServerMessages.Type.SESS_AUTHENTICATE_OK_VALUE, AuthenticateOk.getDefaultInstance().getParserForType());
        messageTypeToParser.put(ServerMessages.Type.SQL_COLUMN_META_DATA_VALUE, ColumnMetaData.getDefaultInstance().getParserForType());
        messageTypeToParser.put(ServerMessages.Type.SQL_CURSOR_FETCH_DONE_VALUE, CursorFetchDone.getDefaultInstance().getParserForType());
        messageTypeToParser.put(ServerMessages.Type.SQL_ROW_VALUE, Row.getDefaultInstance().getParserForType());
        messageTypeToParser.put(ServerMessages.Type.SQL_STMT_EXECUTE_OK_VALUE, StmtExecuteOk.getDefaultInstance().getParserForType());
    }

    private FullReadInputStream inputStream;
    /** Have we already read the header for the next message? */
    private boolean hasReadHeader = false;
    /** Message type from header. */
    private int type = -1;
    /** Message size from header. */
    private int size = -1;

    public MessageReader(FullReadInputStream inputStream) {
        this.inputStream = inputStream;
    }

    /**
     * Read the header for the next message.
     */
    private void readHeader() throws IOException {
        byte[] len = new byte[4];
        this.inputStream.readFully(len);
        this.size = ByteBuffer.wrap(len).getInt();
        this.type = this.inputStream.read();
        this.hasReadHeader = true;
    }

    /**
     * Clear the stored header.
     */
    private void clearHeader() {
        this.hasReadHeader = false;
        this.type = -1;
        this.size = -1;
    }

    /**
     * Get the message type of the next message, possibly blocking indefinitely until the message is received.
     */
    public int getNextMessageType() throws IOException {
        if (!this.hasReadHeader) {
            readHeader();
        }
        return this.type;
    }

    /**
     * Throw an exception in response to an <i>Error</i> message received from the server.
     */
    private void throwErrorFromServer(Error msg) {
        throw new MysqlxError(msg);
    }

    /**
     * Read the next message in the stream. Block until the message is read fully.
     *
     * @return the next message of type T
     * @throws ClassCastException if the expected message type is not the next message (exception will be thrown in *caller* context)
     */
    public <T extends GeneratedMessage> T read() throws IOException {
        int type = getNextMessageType();
        byte[] packet = new byte[size - MessageWriter.HEADER_LEN];
        this.inputStream.readFully(packet);
        Parser<? extends GeneratedMessage> parser = messageTypeToParser.get(type);
        GeneratedMessage msg = parser.parseFrom(packet);
        if (type == ServerMessages.Type.ERROR_VALUE) {
            throwErrorFromServer((Error) msg);
        }
        clearHeader();
        return (T) msg;
    }
}
