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

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.sasl.Sasl;
import javax.security.sasl.SaslClient;
import javax.security.sasl.SaslException;

import com.google.protobuf.ByteString;

import static com.mysql.cj.mysqlx.protobuf.Mysqlx.Ok;
import static com.mysql.cj.mysqlx.protobuf.MysqlxCrud.Collection;
import static com.mysql.cj.mysqlx.protobuf.MysqlxCrud.Find;
import static com.mysql.cj.mysqlx.protobuf.MysqlxCrud.Insert;
import static com.mysql.cj.mysqlx.protobuf.MysqlxDatatypes.Any;
import static com.mysql.cj.mysqlx.protobuf.MysqlxExpr.Expr;
import static com.mysql.cj.mysqlx.protobuf.MysqlxSession.AuthenticateContinue;
import static com.mysql.cj.mysqlx.protobuf.MysqlxSession.AuthenticateFail;
import static com.mysql.cj.mysqlx.protobuf.MysqlxSession.AuthenticateOk;
import static com.mysql.cj.mysqlx.protobuf.MysqlxSession.AuthenticateStart;
import static com.mysql.cj.mysqlx.protobuf.MysqlxSql.ColumnMetaData;
import static com.mysql.cj.mysqlx.protobuf.MysqlxSql.ColumnMetaData.FieldType;
import static com.mysql.cj.mysqlx.protobuf.MysqlxSql.Row;
import static com.mysql.cj.mysqlx.protobuf.MysqlxSql.StmtExecute;
import static com.mysql.cj.mysqlx.protobuf.MysqlxSql.StmtExecuteOk;

import com.mysql.cj.api.MysqlConnection;
import com.mysql.cj.api.Session;
import com.mysql.cj.api.authentication.AuthenticationProvider;
import com.mysql.cj.api.conf.PropertySet;
import com.mysql.cj.api.exceptions.ExceptionInterceptor;
import com.mysql.cj.api.io.PacketBuffer;
import com.mysql.cj.api.io.PacketSentTimeHolder;
import com.mysql.cj.api.io.Protocol;
import com.mysql.cj.api.io.ResultsHandler;
import com.mysql.cj.api.io.ServerCapabilities;
import com.mysql.cj.api.io.ServerSession;
import com.mysql.cj.api.io.SocketConnection;
import com.mysql.cj.core.CharsetMapping;
import com.mysql.cj.core.authentication.Security;
import com.mysql.cj.core.exceptions.CJCommunicationsException;
import com.mysql.cj.core.exceptions.WrongArgumentException;
import com.mysql.cj.core.io.StatementExecuteOk;
import com.mysql.cj.core.util.StringUtils;
import com.mysql.cj.core.util.LazyString;
import com.mysql.cj.jdbc.Field;
import com.mysql.cj.mysqla.MysqlaConstants;
import com.mysql.cj.mysqla.io.Buffer;
import com.mysql.cj.mysqlx.ExprParser;
import com.mysql.cj.mysqlx.ExprUtil;
import com.mysql.cj.mysqlx.MysqlxSession;

/**
 * Low-level interface to communications with a MySQL-X server.
 */
public class MysqlxProtocol implements Protocol {
    // "xplugin" namespace for StmtExecute messages
    private static final String XPLUGIN_NAMESPACE = "xplugin";

    private static enum XpluginStatementCommand {
        XPLUGIN_STMT_CREATE_COLLECTION("create_collection"),
        XPLUGIN_STMT_DROP_COLLECTION("drop_collection");

        public String commandName;
        private XpluginStatementCommand(String commandName) {
            this.commandName = commandName;
        }
    }

    /**
     * Content-type used in type mapping.
     * c.f. plugin/x/ngs/include/ngs/protocol.h
     */
    private static final int MYSQLX_COLUMN_BYTES_CONTENT_TYPE_GEOMETRY = 0x0001;
    private static final int MYSQLX_COLUMN_BYTES_CONTENT_TYPE_JSON = 0x0002;

    private static final int MYSQLX_COLUMN_FLAGS_UINT_ZEROFILL = 0x0001;
    private static final int MYSQLX_COLUMN_FLAGS_DOUBLE_UNSIGNED = 0x0001;
    private static final int MYSQLX_COLUMN_FLAGS_FLOAT_UNSIGNED = 0x0001;
    private static final int MYSQLX_COLUMN_FLAGS_BYTES_RIGHTPAD = 0x0001;

    private MessageReader reader;
    private MessageWriter writer;

    public MysqlxProtocol(MessageReader reader, MessageWriter writer) {
        this.reader = reader;
        this.writer = writer;
    }

    public void init(MysqlConnection conn, int socketTimeout, SocketConnection socketConnection, PropertySet propertySet) {
        throw new NullPointerException("TODO: this implementation uses a constructor");
    }

    public PropertySet getPropertySet() {
        throw new NullPointerException("TODO");
    }

    public void setPropertySet(PropertySet propertySet) {
        throw new NullPointerException("TODO");
    }

    public ServerCapabilities readServerCapabilities() {
        throw new NullPointerException("TODO");
    }

    public ServerSession getServerSession() {
        throw new NullPointerException("TODO");
    }

    public MysqlConnection getConnection() {
        throw new NullPointerException("TODO");
    }

    public void setConnection(MysqlConnection connection) {
        throw new NullPointerException("TODO");
    }

    public SocketConnection getSocketConnection() {
        throw new NullPointerException("TODO");
    }

    public AuthenticationProvider getAuthenticationProvider() {
        throw new NullPointerException("TODO");
    }

    public ExceptionInterceptor getExceptionInterceptor() {
        throw new NullPointerException("TODO");
    }

    public ResultsHandler getResultsHandler() {
        throw new NullPointerException("TODO");
    }

    public PacketSentTimeHolder getPacketSentTimeHolder() {
        throw new NullPointerException("TODO");
    }

    public void setPacketSentTimeHolder(PacketSentTimeHolder packetSentTimeHolder) {
        throw new NullPointerException("TODO");
    }

    public long getLastPacketReceivedTimeMs() {
        throw new NullPointerException("TODO");
    }

    /**
     * @todo docs?
     */
    public Session getSession(String user, String password, String database) {
        sendSaslAuthStart(user, password, database);
        // TODO: expired password handling
        readAuthenticateOk();
        return new MysqlxSession(this);
    }

    public void sendSaslMysql41AuthStart() {
        AuthenticateStart.Builder builder = AuthenticateStart.newBuilder().setMechName("MYSQL41");
        this.writer.write(builder.build());
    }

    public void sendSaslMysql41AuthContinue(String user, String password, byte[] salt, String database) {
        // TODO: encoding for all this?
        String encoding = "UTF8";
        byte[] userBytes = StringUtils.getBytes(user, encoding);
        byte[] passwordBytes = StringUtils.getBytes(password, encoding);
        byte[] databaseBytes = StringUtils.getBytes(database, encoding);

        byte[] hashedPassword = Security.scramble411(passwordBytes, salt);
        // need convert to hex (for now) as server doesn't want to deal with possibility of embedded NULL
        // need to prefix with unused byte (*) because the server code is treating it like the hash from `mysql.user'
        hashedPassword = String.format("*%040x", new java.math.BigInteger(1, hashedPassword)).getBytes();

        // this is what would happen in the SASL provider but we don't need the overhead of all the plumbing.
        byte[] reply = new byte[databaseBytes.length + userBytes.length + hashedPassword.length + 2];

        // reply is length-prefixed when sent so we just separate fields by \0
        System.arraycopy(databaseBytes, 0, reply, 0, databaseBytes.length);
        int pos = databaseBytes.length;
        reply[pos++] = 0;
        System.arraycopy(userBytes, 0, reply, pos, userBytes.length);
        pos += userBytes.length;
        reply[pos++] = 0;
        System.arraycopy(hashedPassword, 0, reply, pos, hashedPassword.length);

        AuthenticateContinue.Builder builder = AuthenticateContinue.newBuilder();
        builder.setAuthData(ByteString.copyFrom(reply));

        this.writer.write(builder.build());
    }

    /**
     * @todo very MySQL-X specific method.
     */
    public void sendSaslAuthStart(String user, String password, String database) {
        // SASL requests information from the app through callbacks. We provide the username and password by these callbacks. This implementation works for
        // PLAIN and would also work for CRAM-MD5. Additional standardized methods may require additional callbacks.
        CallbackHandler callbackHandler = new CallbackHandler() {
                public void handle(Callback[] callbacks) throws UnsupportedCallbackException {
                    for (Callback c : callbacks) {
                        if (NameCallback.class.isAssignableFrom(c.getClass())) {
                            // we get a name callback and provide the username
                            ((NameCallback) c).setName(user);
                        } else if (PasswordCallback.class.isAssignableFrom(c.getClass())) {
                            // we get  password callback and provide the password
                            ((PasswordCallback) c).setPassword(password.toCharArray());
                        } else {
                            // otherwise, thrown an exception
                            throw new UnsupportedCallbackException(c);
                        }
                    }
                }
            };
        try {
            // now we create the client object we use which can handle PLAIN mechanism for "MySQL-X" protocol to "serverName"
            String[] mechanisms = new String[] {"PLAIN"};
            String authorizationId = database; // as per protocol spec
            String protocol = "MySQL-X";
            Map<String, ?> props = null;
            // TODO: >> serverName. Is this of any use in our MySQL-X exchange? Should be defined to be blank or something.
            String serverName = "<unknown>";
            SaslClient saslClient = Sasl.createSaslClient(mechanisms, authorizationId, protocol, serverName, props, callbackHandler);

            // now just pass the details to the X-protocol auth start message
            AuthenticateStart.Builder authStartBuilder = AuthenticateStart.newBuilder();
            authStartBuilder.setMechName("PLAIN");
            // saslClient will build the SASL response message
            authStartBuilder.setAuthData(ByteString.copyFrom(saslClient.evaluateChallenge(null)));

            this.writer.write(authStartBuilder.build());
        } catch (SaslException ex) {
            // TODO: better exception, should introduce a new exception class for auth?
            throw new RuntimeException(ex);
        }
    }

    public void negotiateSSLConnection(int packLength) {
        throw new NullPointerException("TODO: SSL is not yet supported in this MySQL-X client");
    }

    public void rejectConnection(String message) {
        throw new NullPointerException("TODO");
    }

    public void rejectProtocol(Buffer buf) {
        throw new NullPointerException("TODO");
    }

    public void beforeHandshake() {
        throw new NullPointerException("TODO");
    }

    public void afterHandshake() {
        throw new NullPointerException("TODO");
    }

    public void changeDatabase(String database) {
        throw new NullPointerException("TODO: Figure out how this is relevant for MySQL-X Session");
    }

    public void changeUser(String user, String password, String database) {
        // TODO: implement change user. Do we need to Close the session first?
        throw new NullPointerException("TODO");
    }

    public Buffer readPacket() {
        throw new NullPointerException("TODO: This shouldn't be a protocol method. MySQL-X doesn't use buffers");
    }

    public Buffer readNextPacket() {
        throw new NullPointerException("TODO: This shouldn't be a protocol method. MySQL-X doesn't use buffers");
    }

    public void send(PacketBuffer packet, int packetLen) {
        throw new NullPointerException("TODO: This shouldn't be a protocol method. MySQL-X doesn't use buffers");
    }

    public Buffer sendCommand(int command, String extraData, Buffer queryPacket, boolean skipCheck, String extraDataCharEncoding, int timeoutMillis) {
        throw new NullPointerException("TODO: This shouldn't be a protocol method. MySQL-X doesn't use buffers or command tags in the same way");
    }

    public String getPasswordCharacterEncoding() {
        throw new NullPointerException("TODO");
    }

    public boolean versionMeetsMinimum(int major, int minor, int subminor) {
        throw new NullPointerException("TODO: expose this via ServerVersion so calls look like x.getServerVersion().meetsMinimum(major, minor, subminor)");
    }

    /**
     * @todo not sure how to best expose this in a cross-protocol way. It should expose the state returned in an OK message.
     */
    public void readOk() {
        this.reader.read(Ok.class);
    }

    public void readAuthenticateOk() {
        // if (this.reader.getNextMessageClass() == AuthenticateFail.class) {
        //     AuthenticateFail msg = this.reader.read(AuthenticateFail.class);
        // }
        this.reader.read(AuthenticateOk.class);
    }

    public byte[] readAuthenticateContinue() {
        AuthenticateContinue msg = this.reader.read(AuthenticateContinue.class);
        byte[] data = msg.getAuthData().toByteArray();
        if (data.length == 21 && data[20] == 0) {
            // TODO: mailed the team about changing this
            System.err.println("WARNING: server returned salt with terminating NULL byte, removing it");
            byte[] salt = new byte[20];
            System.arraycopy(data, 0, salt, 0, 20);
            return salt;
        }
        return data;
    }

    /**
     * Convenience method to send a {@link StmtExecute} message with namespace "xplugin".
     *
     * @param command the xplugin command to send
     * @param args the arguments to the command
     */
    private void sendXpluginCommand(XpluginStatementCommand command, Any... args) {
        StmtExecute.Builder builder = StmtExecute.newBuilder();

        builder.setNamespace(XPLUGIN_NAMESPACE);
        // TODO: encoding (character_set_client)
        builder.setStmt(ByteString.copyFromUtf8(command.commandName));
        Arrays.stream(args).forEach(a -> builder.addArgs(a));

        this.writer.write(builder.build());
    }

    // TODO: the follow methods should be expose via a different interface such as CrudProtocol
    public void sendCreateCollection(String schemaName, String collectionName) {
        sendXpluginCommand(XpluginStatementCommand.XPLUGIN_STMT_CREATE_COLLECTION, ExprUtil.buildAny(schemaName), ExprUtil.buildAny(collectionName));
    }

    /**
     * @todo this works for tables too
     */
    public void sendDropCollection(String schemaName, String collectionName) {
        sendXpluginCommand(XpluginStatementCommand.XPLUGIN_STMT_DROP_COLLECTION, ExprUtil.buildAny(schemaName), ExprUtil.buildAny(collectionName));
    }

    /**
     * @todo see how this feels after continued use
     */
    public StatementExecuteOk readStatementExecuteOk() {
        StmtExecuteOk msg = this.reader.read(StmtExecuteOk.class);
        return new StatementExecuteOk(msg.getRowsAffected(), msg.getLastInsertId());
    }

    /**
     * @todo option for brief metadata (types only)
     */
    public void sendSqlStatement(String statement) {
        StmtExecute.Builder builder = StmtExecute.newBuilder();
        // TODO: encoding (character_set_client)
        builder.setStmt(ByteString.copyFromUtf8(statement));
        this.writer.write(builder.build());
    }

    public boolean hasResults() {
        return this.reader.getNextMessageClass() == ColumnMetaData.class;
    }

    /**
     * Map a MySQL-X type code from `ColumnMetaData.FieldType' to a MySQL type constant.
     */
    private static int mysqlxTypeToMysqlType(FieldType type, int contentType) {
        // TODO: check if the signedness is represented in field flags
        switch (type) {
            case SINT:
                // TODO: figure out ranges in detail and test them
                return MysqlaConstants.FIELD_TYPE_LONG;
            case UINT:
                return MysqlaConstants.FIELD_TYPE_LONG;
            case FLOAT:
                return MysqlaConstants.FIELD_TYPE_FLOAT;
            case DOUBLE:
                return MysqlaConstants.FIELD_TYPE_DOUBLE;
            case DECIMAL:
                return MysqlaConstants.FIELD_TYPE_DECIMAL;
            case BYTES:
                switch (contentType) {
                    case MYSQLX_COLUMN_BYTES_CONTENT_TYPE_GEOMETRY:
                        return MysqlaConstants.FIELD_TYPE_GEOMETRY;
                    case MYSQLX_COLUMN_BYTES_CONTENT_TYPE_JSON:
                        return MysqlaConstants.FIELD_TYPE_JSON;
                    default:
                        return MysqlaConstants.FIELD_TYPE_VARCHAR;
                }
            case TIME:
                return MysqlaConstants.FIELD_TYPE_TIME;
            case DATETIME:
                return MysqlaConstants.FIELD_TYPE_DATETIME;
            case SET:
                return MysqlaConstants.FIELD_TYPE_SET;
            case ENUM:
                return MysqlaConstants.FIELD_TYPE_ENUM;
            case BIT:
                return MysqlaConstants.FIELD_TYPE_BIT;
                // TODO: longlong
        }
        throw new WrongArgumentException("TODO: unknown field type: " + type);
    }

    // TODO: put this in CharsetMapping..
    public static Map<String, Integer> COLLATION_NAME_TO_COLLATION_INDEX = new java.util.HashMap<>();
    static {
        for (int i = 0; i < CharsetMapping.COLLATION_INDEX_TO_COLLATION_NAME.length; ++i) {
            COLLATION_NAME_TO_COLLATION_INDEX.put(CharsetMapping.COLLATION_INDEX_TO_COLLATION_NAME[i], i);
        }
    }

    /**
     * Convert a MySQL-X {@link ColumnMetaData} message to a C/J {@link Field} object.
     *
     * @param propertySet needed to construct the Field
     * @param col the message from the server
     * @param characterSet the encoding of the strings in the message
     */
    private static Field columnMetaDataToField(PropertySet propertySet, ColumnMetaData col, String characterSet) {
        try {
            LazyString databaseName = new LazyString(col.getSchema().toString(characterSet));
            LazyString tableName = new LazyString(col.getTable().toString(characterSet));
            LazyString originalTableName = new LazyString(col.getOriginalTable().toString(characterSet));
            LazyString columnName = new LazyString(col.getName().toString(characterSet));
            LazyString originalColumnName = new LazyString(col.getOriginalName().toString(characterSet));
            int mysqlType = mysqlxTypeToMysqlType(col.getType(), col.getContentType());
            long length = col.getLength();
            short flags = (short) col.getFlags();
            int decimals = col.getFractionalDigits();
            String encoding = col.getCharset();
            // TODO: support custom character set
            int collationIndex = COLLATION_NAME_TO_COLLATION_INDEX.get(encoding);
            // TODO: anything to do with `content_type'?
            Field f = new Field(propertySet, databaseName, tableName, originalTableName, columnName, originalColumnName, length, mysqlType, flags, decimals, collationIndex, encoding);
            // flags translation
            if (col.getType().equals(FieldType.UINT)) {
                // special case. c.f. "streaming_command_delegate.cc"
                f.setUnsigned();
            } else if (col.getType().equals(FieldType.UINT) && 0 < (col.getFlags() & MYSQLX_COLUMN_FLAGS_UINT_ZEROFILL)) {
                // TODO: propagate flag to Field
            } else if (col.getType().equals(FieldType.DOUBLE) && 0 < (col.getFlags() & MYSQLX_COLUMN_FLAGS_DOUBLE_UNSIGNED)) {
                f.setUnsigned();
            } else if (col.getType().equals(FieldType.FLOAT) && 0 < (col.getFlags() & MYSQLX_COLUMN_FLAGS_FLOAT_UNSIGNED)) {
                f.setUnsigned();
            } else if (col.getType().equals(FieldType.BYTES) && 0 < (col.getFlags() & MYSQLX_COLUMN_FLAGS_BYTES_RIGHTPAD)) {
                // TODO: propagate flag to Field
            }
            return f;
        } catch (UnsupportedEncodingException ex) {
            throw new WrongArgumentException("Unable to decode metadata strings", ex);
        }
    }

    public MessageReader getReader_prototype() {
        return this.reader;
    }

    public ArrayList<Field> readMetadata(PropertySet propertySet, String characterSet) {
        List<ColumnMetaData> fromServer = new LinkedList<>();
        while (this.reader.getNextMessageClass() == ColumnMetaData.class) {
            fromServer.add(this.reader.read(ColumnMetaData.class));
        }
        ArrayList<Field> metadata = new ArrayList<>(fromServer.size());
        fromServer.forEach(col -> metadata.add(columnMetaDataToField(propertySet, col, characterSet)));
        
        return metadata;
    }

    public void sendDocumentFind(String schemaName, String collectionName, String criteria) {
        Find.Builder builder = Find.newBuilder().setCollection(ExprUtil.buildCollection(schemaName, collectionName));
        builder.setCriteria(new ExprParser(criteria).parse());
        this.writer.write(builder.build());
    }

    public void sendDocumentInsert(String schemaName, String collectionName, String json) {
        Insert.Builder builder = Insert.newBuilder().setCollection(ExprUtil.buildCollection(schemaName, collectionName));
        // TODO: Row format is changing THIS WEEK
        //builder.addRow(Row.newBuilder().addField(ExprUtil.buildAny(json)).build());
        this.writer.write(builder.build());
    }
}
