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

import java.io.Closeable;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import static java.util.stream.Collectors.toMap;

import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.sasl.Sasl;
import javax.security.sasl.SaslClient;
import javax.security.sasl.SaslException;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Parser;
import com.mysql.cj.api.MysqlConnection;
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
import com.mysql.cj.core.exceptions.AssertionFailedException;
import com.mysql.cj.core.exceptions.CJCommunicationsException;
import com.mysql.cj.core.exceptions.ConnectionIsClosedException;
import com.mysql.cj.core.exceptions.WrongArgumentException;
import com.mysql.cj.core.io.StatementExecuteOk;
import com.mysql.cj.core.util.LazyString;
import com.mysql.cj.core.util.StringUtils;
import com.mysql.cj.jdbc.Field;
import com.mysql.cj.mysqla.MysqlaConstants;
import com.mysql.cj.mysqla.io.Buffer;
import com.mysql.cj.mysqlx.ExprUtil;
import com.mysql.cj.mysqlx.FilterParams;
import com.mysql.cj.mysqlx.FindParams;
import com.mysql.cj.mysqlx.InsertParams;
import com.mysql.cj.mysqlx.UpdateParams;
import com.mysql.cj.mysqlx.UpdateSpec;
import com.mysql.cj.mysqlx.devapi.WarningImpl;
import com.mysql.cj.mysqlx.protobuf.Mysqlx.Ok;
import com.mysql.cj.mysqlx.protobuf.MysqlxConnection.Capabilities;
import com.mysql.cj.mysqlx.protobuf.MysqlxConnection.CapabilitiesGet;
import com.mysql.cj.mysqlx.protobuf.MysqlxConnection.Capability;
import com.mysql.cj.mysqlx.protobuf.MysqlxCrud.Column;
import com.mysql.cj.mysqlx.protobuf.MysqlxCrud.DataModel;
import com.mysql.cj.mysqlx.protobuf.MysqlxCrud.Delete;
import com.mysql.cj.mysqlx.protobuf.MysqlxCrud.Find;
import com.mysql.cj.mysqlx.protobuf.MysqlxCrud.Insert;
import com.mysql.cj.mysqlx.protobuf.MysqlxCrud.Insert.TypedRow;
import com.mysql.cj.mysqlx.protobuf.MysqlxCrud.Limit;
import com.mysql.cj.mysqlx.protobuf.MysqlxCrud.Order;
import com.mysql.cj.mysqlx.protobuf.MysqlxCrud.Projection;
import com.mysql.cj.mysqlx.protobuf.MysqlxCrud.Update;
import com.mysql.cj.mysqlx.protobuf.MysqlxCrud.UpdateOperation;
import com.mysql.cj.mysqlx.protobuf.MysqlxCrud.UpdateOperation.UpdateType;
import com.mysql.cj.mysqlx.protobuf.MysqlxDatatypes.Any;
import com.mysql.cj.mysqlx.protobuf.MysqlxDatatypes.Scalar;
import com.mysql.cj.mysqlx.protobuf.MysqlxExpr.ColumnIdentifier;
import com.mysql.cj.mysqlx.protobuf.MysqlxExpr.Expr;
import com.mysql.cj.mysqlx.protobuf.MysqlxNotice.Frame;
import com.mysql.cj.mysqlx.protobuf.MysqlxNotice.SessionStateChanged;
import com.mysql.cj.mysqlx.protobuf.MysqlxNotice.Warning;
import com.mysql.cj.mysqlx.protobuf.MysqlxResultset.ColumnMetaData;
import com.mysql.cj.mysqlx.protobuf.MysqlxResultset.ColumnMetaData.FieldType;
import com.mysql.cj.mysqlx.protobuf.MysqlxResultset.FetchDone;
import com.mysql.cj.mysqlx.protobuf.MysqlxResultset.Row;
import com.mysql.cj.mysqlx.protobuf.MysqlxSession.AuthenticateContinue;
import com.mysql.cj.mysqlx.protobuf.MysqlxSession.AuthenticateOk;
import com.mysql.cj.mysqlx.protobuf.MysqlxSession.AuthenticateStart;
import com.mysql.cj.mysqlx.protobuf.MysqlxSession.Close;
import com.mysql.cj.mysqlx.protobuf.MysqlxSql.StmtExecute;
import com.mysql.cj.mysqlx.protobuf.MysqlxSql.StmtExecuteOk;
import com.mysql.cj.mysqlx.result.MysqlxRow;
import com.mysql.cj.mysqlx.result.MysqlxRowInputStream;

/**
 * Low-level interface to communications with a MySQL-X server.
 */
public class MysqlxProtocol implements Protocol {
    // "xplugin" namespace for StmtExecute messages
    private static final String XPLUGIN_NAMESPACE = "xplugin";

    private static enum XpluginStatementCommand {
        XPLUGIN_STMT_CREATE_COLLECTION("create_collection"), XPLUGIN_STMT_CREATE_COLLECTION_INDEX("create_collection_index"), XPLUGIN_STMT_DROP_COLLECTION(
                "drop_collection"), XPLUGIN_STMT_DROP_COLLECTION_INDEX("drop_collection_index"), XPLUGIN_STMT_PING("ping"), XPLUGIN_STMT_LIST_OBJECTS(
                "list_objects"), XPLUGIN_STMT_ENABLE_NOTICES("enable_notices"), XPLUGIN_STMT_DISABLE_NOTICES("disable_notices"), XPLUGIN_STMT_LIST_NOTICES(
                "list_notices");

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
    /** We take responsibility of the socket as the managed resource. We close it when we're done. */
    private Closeable managedResource;
    /** @TODO what is this */
    private PropertySet propertySet;
    private Map<String, Any> capabilities;

    public MysqlxProtocol(MessageReader reader, MessageWriter writer, Closeable network, PropertySet propSet) {
        this.reader = reader;
        this.writer = writer;
        this.managedResource = network;
        this.propertySet = propSet;
        this.capabilities = getCapabilities();
    }

    public void init(MysqlConnection conn, int socketTimeout, SocketConnection socketConnection, PropertySet propSet) {
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
     * Get the capabilities from the server.
     * <p>
     * <b>NOTE:</b> This must be called before authentication.
     * @return capabilities mapped by name
     */
    private Map<String, Any> getCapabilities() {
        this.writer.write(CapabilitiesGet.getDefaultInstance());
        return this.reader.read(Capabilities.class).getCapabilitiesList().stream().collect(toMap(Capability::getName, Capability::getValue));
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
            String[] mechanisms = new String[] { "PLAIN" };
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

    public void readOk() {
        this.reader.read(Ok.class);
    }

    public void readAuthenticateOk() {
        this.reader.read(AuthenticateOk.class);
    }

    public byte[] readAuthenticateContinue() {
        AuthenticateContinue msg = this.reader.read(AuthenticateContinue.class);
        byte[] data = msg.getAuthData().toByteArray();
        if (data.length != 20) {
            throw AssertionFailedException.shouldNotHappen("Salt length should be 20, but is " + data.length);
        }
        return data;
    }

    /**
     * Convenience method to send a {@link StmtExecute} message with namespace "xplugin".
     *
     * @param command
     *            the xplugin command to send
     * @param args
     *            the arguments to the command
     */
    private void sendXpluginCommand(XpluginStatementCommand command, Any... args) {
        StmtExecute.Builder builder = StmtExecute.newBuilder();

        builder.setNamespace(XPLUGIN_NAMESPACE);
        // TODO: encoding (character_set_client)
        builder.setStmt(ByteString.copyFromUtf8(command.commandName));
        Arrays.stream(args).forEach(a -> builder.addArgs(a));

        this.writer.write(builder.build());
    }

    // TODO: the following methods should be expose via a different interface such as CrudProtocol
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
     * List the objects in the given schema. Returns a table as so:
     *
     * <pre>
     * | name                | type       |
     * |---------------------+------------|
     * | CollectionTest      | COLLECTION |
     * | some_view           | VIEW       |
     * | xprotocol_test_test | TABLE      |
     * </pre>
     */
    public void sendListObjects(String schemaName) {
        sendXpluginCommand(XpluginStatementCommand.XPLUGIN_STMT_LIST_OBJECTS, ExprUtil.buildAny(schemaName));
    }

    /**
     * List the notices the server allows subscribing to. Returns a table as so:
     *
     * <pre>
     * | notice (string)     | enabled (int) |
     * |---------------------+---------------|
     * | warnings            | 1             |
     * </pre>
     */
    public void sendListNotices() {
        sendXpluginCommand(XpluginStatementCommand.XPLUGIN_STMT_LIST_NOTICES);
    }

    public void sendEnableNotices(String... notices) {
        Any[] args = Arrays.stream(notices).map(ExprUtil::buildAny).toArray(s -> new Any[notices.length]);
        sendXpluginCommand(XpluginStatementCommand.XPLUGIN_STMT_ENABLE_NOTICES, args);
    }

    public void sendDisableNotices(String... notices) {
        Any[] args = Arrays.stream(notices).map(ExprUtil::buildAny).toArray(s -> new Any[notices.length]);
        sendXpluginCommand(XpluginStatementCommand.XPLUGIN_STMT_DISABLE_NOTICES, args);
    }

    /**
     * @todo see how this feels after continued use
     */
    public StatementExecuteOk readStatementExecuteOk() {
        if (this.reader.getNextMessageClass() == FetchDone.class) {
            // consume this
            // TODO: work out a formal model for how post-row data is handled
            this.reader.read(FetchDone.class);
        }

        long rowsAffected = 0;
        Long lastInsertId = null;
        // TODO: don't use DevApi interfaces here!
        List<com.mysql.cj.api.x.Warning> warnings = new ArrayList<>();
        while (this.reader.getNextMessageClass() == Frame.class) {
            try {
                Frame notice = this.reader.read(Frame.class);
                // TODO: asked Jan for type constants here (these values copied from comments in mysqlx_notice.proto
                final int MysqlxNoticeFrameType_WARNING = 1;
                final int MysqlxNoticeFrameType_SESS_VAR_CHANGED = 2;
                final int MysqlxNoticeFrameType_SESS_STATE_CHANGED = 3;
                if (notice.getType() == MysqlxNoticeFrameType_WARNING) {
                    // TODO: again, shouldn't use DevApi WarningImpl class here
                    Parser<Warning> parser = (Parser<Warning>) MessageConstants.MESSAGE_CLASS_TO_PARSER.get(Warning.class);
                    warnings.add(new WarningImpl(parser.parseFrom(notice.getPayload())));
                    // } else if (notice.getType() == MysqlxNoticeFrameType_SESS_VAR_CHANGED) {
                    //     // TODO: ignored for now
                    //     throw new RuntimeException("Got a session variable changed: " + notice);
                    } else if (notice.getType() == MysqlxNoticeFrameType_SESS_STATE_CHANGED) {
                        // TODO: create a MessageParser or ServerMessageParser if this needs to be done elsewhere
                        Parser<SessionStateChanged> parser = (Parser<SessionStateChanged>) MessageConstants.MESSAGE_CLASS_TO_PARSER.get(SessionStateChanged.class);
                        SessionStateChanged msg = parser.parseFrom(notice.getPayload());
                        switch (msg.getParam()) {
                            case GENERATED_INSERT_ID:
                                // TODO: handle > 2^63-1?
                                lastInsertId = msg.getValue().getVUnsignedInt();
                                break;
                            case ROWS_AFFECTED:
                                // TODO: handle > 2^63-1?
                                rowsAffected = msg.getValue().getVUnsignedInt();
                                break;
                            case PRODUCED_MESSAGE:
                                System.err.println("Ignoring NOTICE message: " + msg.getValue().getVString().getValue().toStringUtf8());
                                break;
                            case CURRENT_SCHEMA:
                            case ACCOUNT_EXPIRED:
                            case ROWS_FOUND:
                            case ROWS_MATCHED:
                            case TRX_COMMITTED:
                            case TRX_ROLLEDBACK:
                                // TODO: propagate state
                            default:
                                // TODO: log warning
                                throw new NullPointerException("unhandled SessionStateChanged notice! " + msg);
                        }
                } else {
                    // TODO: error?
                    throw new RuntimeException("Got an unknown notice: " + notice);
                }
            } catch (InvalidProtocolBufferException ex) {
                throw new CJCommunicationsException(ex);
            }
        }

        this.reader.read(StmtExecuteOk.class);
        return new StatementExecuteOk(rowsAffected, lastInsertId, warnings);
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

    public void drainRows() {
        while (this.reader.getNextMessageClass() == Row.class) {
            this.reader.read(Row.class);
        }
    }

    /**
     * Map a MySQL-X type code from `ColumnMetaData.FieldType' to a MySQL type constant. These are the only types that will be present in {@link MysqlxRow}
     * results.
     *
     * @param type
     *            the type as the ColumnMetaData.FieldType
     * @param contentType
     *            the inner type
     * @return A <b>FIELD_TYPE</b> constant from {@link MysqlaConstants} corresponding to the combination of input parameters.
     */
    private static int mysqlxTypeToMysqlType(FieldType type, int contentType) {
        // TODO: check if the signedness is represented in field flags
        switch (type) {
            case SINT:
                // TODO: figure out ranges in detail and test them
                return MysqlaConstants.FIELD_TYPE_LONGLONG;
            case UINT:
                return MysqlaConstants.FIELD_TYPE_LONGLONG;
            case FLOAT:
                return MysqlaConstants.FIELD_TYPE_FLOAT;
            case DOUBLE:
                return MysqlaConstants.FIELD_TYPE_DOUBLE;
            case DECIMAL:
                return MysqlaConstants.FIELD_TYPE_NEW_DECIMAL;
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
                // may be a timestamp or just a date if time values are missing. metadata doesn't distinguish between the two
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
     * @param propertySet
     *            needed to construct the Field
     * @param col
     *            the message from the server
     * @param characterSet
     *            the encoding of the strings in the message
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
            // TODO: length is returning 0 for all
            // TODO: pass length to mysql type mapping, length = 10 -> DATE, length = 19 -> DATETIME
            // System.err.println("columnName: " + columnName);
            // System.err.println("length (was returning 0 for all types): " + length);
            short flags = (short) col.getFlags();
            int decimals = col.getFractionalDigits();
            int collationIndex = 0;
            if (col.hasCollation()) {
                // TODO: support custom character set
                collationIndex = (int) col.getCollation();
            }
            Field f = new Field(propertySet, databaseName, tableName, originalTableName, columnName, originalColumnName, length, mysqlType, flags, decimals,
                    collationIndex, CharsetMapping.COLLATION_INDEX_TO_COLLATION_NAME[collationIndex]);
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

    public ArrayList<Field> readMetadata(String characterSet) {
        List<ColumnMetaData> fromServer = new LinkedList<>();
        do { // use this construct to read at least one
            fromServer.add(this.reader.read(ColumnMetaData.class));
        } while (this.reader.getNextMessageClass() == ColumnMetaData.class);
        ArrayList<Field> metadata = new ArrayList<>(fromServer.size());
        fromServer.forEach(col -> metadata.add(columnMetaDataToField(this.propertySet, col, characterSet)));

        return metadata;
    }

    public MysqlxRow readRowOrNull(ArrayList<Field> metadata) {
        if (this.reader.getNextMessageClass() == Row.class) {
            Row r = this.reader.read(Row.class);
            return new MysqlxRow(metadata, r);
        } else {
            return null;
        }
    }

    public MysqlxRowInputStream getRowInputStream(ArrayList<Field> metadata) {
        return new MysqlxRowInputStream(metadata, this);
    }

    public void sendFind(String schemaName, String collectionName, FindParams findParams, boolean isRelational) {
        Find.Builder builder = Find.newBuilder().setCollection(ExprUtil.buildCollection(schemaName, collectionName));
        builder.setDataModel(isRelational ? DataModel.TABLE : DataModel.DOCUMENT);
        if (findParams.getFields() != null) {
            builder.addAllProjection((List<Projection>) findParams.getFields());
        }
        if (findParams.getGrouping() != null) {
            builder.addAllGrouping((List<Expr>) findParams.getGrouping());
        }
        if (findParams.getGroupingCriteria() != null) {
            builder.setGroupingCriteria((Expr) findParams.getGroupingCriteria());
        }
        FilterParams filterParams = findParams;
        // TODO: abstract this (already requested Rafal to do it)
        if (filterParams.getOrder() != null) {
            builder.addAllOrder((List<Order>) filterParams.getOrder());
        }
        if (filterParams.getLimit() != null) {
            Limit.Builder lb = Limit.newBuilder().setRowCount(filterParams.getLimit());
            if (filterParams.getOffset() != null) {
                lb.setOffset(filterParams.getOffset());
            }
            builder.setLimit(lb.build());
        }
        if (filterParams.getCriteria() != null) {
            builder.setCriteria((Expr) filterParams.getCriteria());
        }
        if (filterParams.getArgs() != null) {
            builder.addAllArgs((List<Scalar>) filterParams.getArgs());
        }
        this.writer.write(builder.build());
    }

    public void sendDocUpdates(String schemaName, String collectionName, FilterParams filterParams, List<UpdateSpec> updates) {
        Update.Builder builder = Update.newBuilder().setCollection(ExprUtil.buildCollection(schemaName, collectionName));
        updates.forEach(u -> {
            UpdateOperation.Builder opBuilder = UpdateOperation.newBuilder();
            opBuilder.setOperation((UpdateType) u.getUpdateType());
            opBuilder.setSource((ColumnIdentifier) u.getSource());
            if (u.getValue() != null) {
                opBuilder.setValue((Expr) u.getValue());
            }
            builder.addOperation(opBuilder.build());
        });
        // TODO: abstract this (already requested Rafal to do it)
        if (filterParams.getOrder() != null) {
            builder.addAllOrder((List<Order>) filterParams.getOrder());
        }
        if (filterParams.getLimit() != null) {
            Limit.Builder lb = Limit.newBuilder().setRowCount(filterParams.getLimit());
            if (filterParams.getOffset() != null) {
                lb.setOffset(filterParams.getOffset());
            }
            builder.setLimit(lb.build());
        }
        if (filterParams.getCriteria() != null) {
            builder.setCriteria((Expr) filterParams.getCriteria());
        }
        if (filterParams.getArgs() != null) {
            builder.addAllArgs((List<Scalar>) filterParams.getArgs());
        }
        this.writer.write(builder.build());
    }

    // TODO: low-level tests of this method
    public void sendRowUpdates(String schemaName, String tableName, FilterParams filterParams, UpdateParams updateParams) {
        Update.Builder builder = Update.newBuilder().setDataModel(DataModel.TABLE).setCollection(ExprUtil.buildCollection(schemaName, tableName));
        ((Map<ColumnIdentifier, Expr>) updateParams.getUpdates()).entrySet().stream()
                .map(e -> UpdateOperation.newBuilder().setOperation(UpdateType.SET).setSource(e.getKey()).setValue(e.getValue()).build())
                .forEach(builder::addOperation);
        // TODO: abstract this (already requested Rafal to do it)
        if (filterParams.getOrder() != null) {
            builder.addAllOrder((List<Order>) filterParams.getOrder());
        }
        if (filterParams.getLimit() != null) {
            Limit.Builder lb = Limit.newBuilder().setRowCount(filterParams.getLimit());
            if (filterParams.getOffset() != null) {
                lb.setOffset(filterParams.getOffset());
            }
            builder.setLimit(lb.build());
        }
        if (filterParams.getCriteria() != null) {
            builder.setCriteria((Expr) filterParams.getCriteria());
        }
        if (filterParams.getArgs() != null) {
            builder.addAllArgs((List<Scalar>) filterParams.getArgs());
        }
        this.writer.write(builder.build());
    }

    public void sendDocDelete(String schemaName, String collectionName, FilterParams filterParams) {
        Delete.Builder builder = Delete.newBuilder().setCollection(ExprUtil.buildCollection(schemaName, collectionName));
        // TODO: abstract this (already requested Rafal to do it)
        if (filterParams.getOrder() != null) {
            builder.addAllOrder((List<Order>) filterParams.getOrder());
        }
        if (filterParams.getLimit() != null) {
            Limit.Builder lb = Limit.newBuilder().setRowCount(filterParams.getLimit());
            if (filterParams.getOffset() != null) {
                lb.setOffset(filterParams.getOffset());
            }
            builder.setLimit(lb.build());
        }
        if (filterParams.getCriteria() != null) {
            builder.setCriteria((Expr) filterParams.getCriteria());
        }
        if (filterParams.getArgs() != null) {
            builder.addAllArgs((List<Scalar>) filterParams.getArgs());
        }
        this.writer.write(builder.build());
    }

    // TODO: unused
    public void sendDocInsert(String schemaName, String collectionName, String json) {
        Insert.Builder builder = Insert.newBuilder().setCollection(ExprUtil.buildCollection(schemaName, collectionName));
        builder.addRow(TypedRow.newBuilder().addField(ExprUtil.argObjectToExpr(json)).build());
        this.writer.write(builder.build());
    }

    public void sendDocInsert(String schemaName, String collectionName, List<String> json) {
        Insert.Builder builder = Insert.newBuilder().setCollection(ExprUtil.buildCollection(schemaName, collectionName));
        json.stream()
                .map(str -> TypedRow.newBuilder().addField(ExprUtil.argObjectToExpr(str)).build())
                .forEach(builder::addRow);
        this.writer.write(builder.build());
    }

    public void sendRowInsert(String schemaName, String tableName, InsertParams insertParams) {
        Insert.Builder builder = Insert.newBuilder().setDataModel(DataModel.TABLE).setCollection(ExprUtil.buildCollection(schemaName, tableName));
        if (insertParams.getProjection() != null) {
            builder.addAllProjection((List<Column>) insertParams.getProjection());
        }
        builder.addAllRow((List<TypedRow>) insertParams.getRows());
        this.writer.write(builder.build());
    }

    public void sendSessionClose() {
        this.writer.write(Close.getDefaultInstance());
    }

    public String getPluginVersion() {
        return this.capabilities.get("plugin.version").getScalar().getVString().getValue().toStringUtf8();
    }

    public void close() throws IOException {
        if (this.managedResource == null) {
            throw new ConnectionIsClosedException();
        }
        this.managedResource.close();
        this.managedResource = null;
    }

    @Override
    public void connect(String user, String password, String database) {
        // TODO Auto-generated method stub

    }
}
