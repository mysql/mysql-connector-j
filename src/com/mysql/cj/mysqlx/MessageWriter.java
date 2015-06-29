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

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.HashMap;
import java.util.Map;

import com.google.protobuf.MessageLite;

import static com.mysql.cj.mysqlx.protobuf.Mysqlx.ClientMessages;
import static com.mysql.cj.mysqlx.protobuf.MysqlxAdmin.CommandExecute;
import static com.mysql.cj.mysqlx.protobuf.MysqlxCrud.Delete;
import static com.mysql.cj.mysqlx.protobuf.MysqlxCrud.Find;
import static com.mysql.cj.mysqlx.protobuf.MysqlxCrud.Insert;
import static com.mysql.cj.mysqlx.protobuf.MysqlxCrud.Update;
import static com.mysql.cj.mysqlx.protobuf.MysqlxSession.AuthenticateStart;

import com.mysql.cj.core.exceptions.WrongArgumentException;

/**
 * Low-level message writer for protobuf messages.
 */
public class MessageWriter {
    /**
     * Header length of MySQL-X packet.
     */
    static final int HEADER_LEN = 5;

    /**
     * Store a mapping of message class to "ClientMessages" type tag. This is used to generate the header when sending a message.
     */
    private static Map<Class, Integer> messageClassToClientMessageType = new HashMap<>();

    static {
        messageClassToClientMessageType.put(AuthenticateStart.class, ClientMessages.Type.SESS_AUTHENTICATE_START_VALUE);
        messageClassToClientMessageType.put(CommandExecute.class, ClientMessages.Type.ADMIN_COMMAND_EXECUTE_VALUE);
        messageClassToClientMessageType.put(Delete.class, ClientMessages.Type.CRUD_DELETE_VALUE);
        messageClassToClientMessageType.put(Find.class, ClientMessages.Type.CRUD_FIND_VALUE);
        messageClassToClientMessageType.put(Insert.class, ClientMessages.Type.CRUD_INSERT_VALUE);
        messageClassToClientMessageType.put(Update.class, ClientMessages.Type.CRUD_UPDATE_VALUE);
    }

    private BufferedOutputStream outputStream;

    public MessageWriter(BufferedOutputStream os) {
        this.outputStream = os;
    }

    /**
     * Looking the type tag for a protobuf message class.
     */
    private static int getTypeForMessageClass(Class msgClass) {
        Integer tag = messageClassToClientMessageType.get(msgClass);
        if (tag == null) {
            throw new WrongArgumentException("Invalid message class " + msgClass.getName());
        }
        return tag;
    }

    /**
     * Send a message.
     */
    public void write(MessageLite msg) throws IOException {
        int type = getTypeForMessageClass(msg.getClass());
        int size = HEADER_LEN + msg.getSerializedSize();
        byte[] sizeHeader = ByteBuffer.allocate(4).putInt(size).array();
        this.outputStream.write(sizeHeader);
        this.outputStream.write(type);
        msg.writeTo(this.outputStream);
        this.outputStream.flush();
    }
}
