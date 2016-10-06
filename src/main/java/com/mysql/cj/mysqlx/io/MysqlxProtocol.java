/*
  Copyright (c) 2015, 2016, Oracle and/or its affiliates. All rights reserved.

  The MySQL Connector/J is licensed under the terms of the GPLv2
  <http://www.gnu.org/licenses/old-licenses/gpl-2.0.html>, like most MySQL Connectors.
  There are special exceptions to the terms and conditions of the GPLv2 as it is applied to
  this software, see the FOSS License Exception
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

import static java.util.stream.Collectors.toMap;

import java.io.Closeable;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.channels.CompletionHandler;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.concurrent.CompletableFuture;

import com.google.protobuf.MessageLite;
import com.mysql.cj.api.MysqlConnection;
import com.mysql.cj.api.authentication.AuthenticationProvider;
import com.mysql.cj.api.conf.PropertySet;
import com.mysql.cj.api.exceptions.ExceptionInterceptor;
import com.mysql.cj.api.io.PacketReceivedTimeHolder;
import com.mysql.cj.api.io.PacketSentTimeHolder;
import com.mysql.cj.api.io.Protocol;
import com.mysql.cj.api.io.ServerCapabilities;
import com.mysql.cj.api.io.ServerSession;
import com.mysql.cj.api.io.SocketConnection;
import com.mysql.cj.api.x.SqlResult;
import com.mysql.cj.core.CharsetMapping;
import com.mysql.cj.core.MysqlType;
import com.mysql.cj.core.exceptions.AssertionFailedException;
import com.mysql.cj.core.exceptions.ConnectionIsClosedException;
import com.mysql.cj.core.exceptions.WrongArgumentException;
import com.mysql.cj.core.io.StatementExecuteOk;
import com.mysql.cj.core.result.Field;
import com.mysql.cj.core.util.LazyString;
import com.mysql.cj.mysqla.MysqlaConstants;
import com.mysql.cj.mysqlx.CreateIndexParams;
import com.mysql.cj.mysqlx.ExprUtil;
import com.mysql.cj.mysqlx.FilterParams;
import com.mysql.cj.mysqlx.FindParams;
import com.mysql.cj.mysqlx.InsertParams;
import com.mysql.cj.mysqlx.UpdateParams;
import com.mysql.cj.mysqlx.UpdateSpec;
import com.mysql.cj.mysqlx.io.AsyncMessageReader.MessageListener;
import com.mysql.cj.mysqlx.io.MessageBuilder.XpluginStatementCommand;
import com.mysql.cj.mysqlx.protobuf.Mysqlx.Ok;
import com.mysql.cj.mysqlx.protobuf.MysqlxConnection.Capabilities;
import com.mysql.cj.mysqlx.protobuf.MysqlxConnection.CapabilitiesGet;
import com.mysql.cj.mysqlx.protobuf.MysqlxConnection.Capability;
import com.mysql.cj.mysqlx.protobuf.MysqlxDatatypes.Any;
import com.mysql.cj.mysqlx.protobuf.MysqlxDatatypes.Object.ObjectField;
import com.mysql.cj.mysqlx.protobuf.MysqlxNotice.Frame;
import com.mysql.cj.mysqlx.protobuf.MysqlxNotice.SessionStateChanged;
import com.mysql.cj.mysqlx.protobuf.MysqlxResultset.ColumnMetaData;
import com.mysql.cj.mysqlx.protobuf.MysqlxResultset.ColumnMetaData.FieldType;
import com.mysql.cj.mysqlx.protobuf.MysqlxResultset.FetchDone;
import com.mysql.cj.mysqlx.protobuf.MysqlxResultset.FetchDoneMoreResultsets;
import com.mysql.cj.mysqlx.protobuf.MysqlxResultset.Row;
import com.mysql.cj.mysqlx.protobuf.MysqlxSession.AuthenticateContinue;
import com.mysql.cj.mysqlx.protobuf.MysqlxSession.AuthenticateOk;
import com.mysql.cj.mysqlx.protobuf.MysqlxSession.AuthenticateStart;
import com.mysql.cj.mysqlx.protobuf.MysqlxSession.Close;
import com.mysql.cj.mysqlx.protobuf.MysqlxSql.StmtExecuteOk;
import com.mysql.cj.mysqlx.result.MysqlxRow;
import com.mysql.cj.mysqlx.result.MysqlxRowInputStream;

/**
 * Low-level interface to communications with X Plugin.
 */
public class MysqlxProtocol implements Protocol {
    /**
     * Content-type used in type mapping.
     * c.f. mysqlx_resultset.proto
     */
    private static final int MYSQLX_COLUMN_BYTES_CONTENT_TYPE_GEOMETRY = 0x0001;
    public static final int MYSQLX_COLUMN_BYTES_CONTENT_TYPE_JSON = 0x0002;

    private static final int MYSQLX_COLUMN_FLAGS_UINT_ZEROFILL = 0x0001;
    private static final int MYSQLX_COLUMN_FLAGS_DOUBLE_UNSIGNED = 0x0001;
    private static final int MYSQLX_COLUMN_FLAGS_FLOAT_UNSIGNED = 0x0001;
    private static final int MYSQLX_COLUMN_FLAGS_DECIMAL_UNSIGNED = 0x0001;
    private static final int MYSQLX_COLUMN_FLAGS_BYTES_RIGHTPAD = 0x0001;
    private static final int MYSQLX_COLUMN_FLAGS_DATETIME_TIMESTAMP = 0x0001;
    private static final int MYSQLX_COLUMN_FLAGS_NOT_NULL = 0x0010;
    private static final int MYSQLX_COLUMN_FLAGS_PRIMARY_KEY = 0x0020;
    private static final int MYSQLX_COLUMN_FLAGS_UNIQUE_KEY = 0x0040;
    private static final int MYSQLX_COLUMN_FLAGS_MULTIPLE_KEY = 0x0080;
    private static final int MYSQLX_COLUMN_FLAGS_AUTO_INCREMENT = 0x0100;

    // TODO: need protocol type constants here (these values copied from comments in mysqlx_notice.proto)
    public static final int MysqlxNoticeFrameType_WARNING = 1;
    public static final int MysqlxNoticeFrameType_SESS_VAR_CHANGED = 2;
    public static final int MysqlxNoticeFrameType_SESS_STATE_CHANGED = 3;

    private MessageReader reader;
    private MessageWriter writer;
    /** We take responsibility of the socket as the managed resource. We close it when we're done. */
    private Closeable managedResource;
    /** @TODO what is this */
    private PropertySet propertySet;
    private Map<String, Any> capabilities;
    /** Server-assigned client-id. */
    private long clientId = -1;
    private MessageBuilder msgBuilder = new MessageBuilder();

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
        return this.propertySet;
    }

    public void setPropertySet(PropertySet propertySet) {
        this.propertySet = propertySet;
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

    public PacketSentTimeHolder getPacketSentTimeHolder() {
        throw new NullPointerException("TODO");
    }

    public void setPacketSentTimeHolder(PacketSentTimeHolder packetSentTimeHolder) {
        throw new NullPointerException("TODO");
    }

    @Override
    public PacketReceivedTimeHolder getPacketReceivedTimeHolder() {
        throw new NullPointerException("TODO");
    }

    @Override
    public void setPacketReceivedTimeHolder(PacketReceivedTimeHolder packetReceivedTimeHolder) {
        throw new NullPointerException("TODO");
    }

    /**
     * Get the capabilities from the server.
     * <p>
     * <b>NOTE:</b> This must be called before authentication.
     * 
     * @return capabilities mapped by name
     */
    private Map<String, Any> getCapabilities() {
        this.writer.write(CapabilitiesGet.getDefaultInstance());
        return this.reader.read(Capabilities.class).getCapabilitiesList().stream().collect(toMap(Capability::getName, Capability::getValue));
    }

    /**
     * Set a capability of current session. Must be done before authentication ({@link changeUser(String, String, String)}).
     */
    public void setCapability(String name, Object value) {
        this.writer.write(this.msgBuilder.buildCapabilitiesSet(name, value));
        readOk();
    }

    public void sendSaslMysql41AuthStart() {
        AuthenticateStart.Builder builder = AuthenticateStart.newBuilder().setMechName("MYSQL41");
        this.writer.write(builder.build());
    }

    public void sendSaslMysql41AuthContinue(String user, String password, byte[] salt, String database) {
        this.writer.write(this.msgBuilder.buildMysql41AuthContinue(user, password, salt, database));
    }

    public void sendSaslAuthStart(String user, String password, String database) {
        this.writer.write(this.msgBuilder.buildPlainAuthStart(user, password, database));
    }

    public void negotiateSSLConnection(int packLength) {
        throw new NullPointerException("TODO: SSL is not yet supported in this X Protocol client");
    }

    public void rejectConnection(String message) {
        throw new NullPointerException("TODO");
    }

    public void beforeHandshake() {
        throw new NullPointerException("TODO");
    }

    public void afterHandshake() {
        throw new NullPointerException("TODO");
    }

    public void changeDatabase(String database) {
        throw new NullPointerException("TODO: Figure out how this is relevant for X Protocol client Session");
    }

    public void changeUser(String user, String password, String database) {
        // TODO: implement change user. Do we need to Close the session first?
        throw new NullPointerException("TODO");
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
        while (this.reader.getNextMessageClass() == Frame.class) {
            Frame notice = this.reader.read(Frame.class);
            if (notice.getType() == MysqlxNoticeFrameType_SESS_STATE_CHANGED) {
                SessionStateChanged msg = MessageReader.parseNotice(notice.getPayload(), SessionStateChanged.class);
                switch (msg.getParam()) {
                    case CLIENT_ID_ASSIGNED:
                        this.clientId = msg.getValue().getVUnsignedInt();
                        break;
                    case ACCOUNT_EXPIRED: // TODO
                    default:
                        throw new WrongArgumentException("Unknown SessionStateChanged notice received during authentication: " + msg);
                }
            } else {
                throw new WrongArgumentException("Unknown notice received during authentication: " + notice);
            }
        }
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

    // TODO: the following methods should be expose via a different interface such as CrudProtocol
    public void sendCreateCollection(String schemaName, String collectionName) {
        this.writer.write(this.msgBuilder.buildXpluginCommand(XpluginStatementCommand.XPLUGIN_STMT_CREATE_COLLECTION,
                Any.newBuilder().setType(Any.Type.OBJECT)
                        .setObj(com.mysql.cj.mysqlx.protobuf.MysqlxDatatypes.Object.newBuilder()
                                .addFld(ObjectField.newBuilder().setKey("name").setValue(ExprUtil.buildAny(collectionName)))
                                .addFld(ObjectField.newBuilder().setKey("schema").setValue(ExprUtil.buildAny(schemaName))))
                        .build()));
    }

    /**
     * @todo this works for tables too
     */
    public void sendDropCollection(String schemaName, String collectionName) {
        this.writer.write(this.msgBuilder.buildXpluginCommand(XpluginStatementCommand.XPLUGIN_STMT_DROP_COLLECTION,
                Any.newBuilder().setType(Any.Type.OBJECT)
                        .setObj(com.mysql.cj.mysqlx.protobuf.MysqlxDatatypes.Object.newBuilder()
                                .addFld(ObjectField.newBuilder().setKey("name").setValue(ExprUtil.buildAny(collectionName)))
                                .addFld(ObjectField.newBuilder().setKey("schema").setValue(ExprUtil.buildAny(schemaName))))
                        .build()));
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
    public void sendListObjects(String schemaName, String pattern) {
        this.writer.write(this.msgBuilder.buildXpluginCommand(XpluginStatementCommand.XPLUGIN_STMT_LIST_OBJECTS,
                Any.newBuilder().setType(Any.Type.OBJECT)
                        .setObj(com.mysql.cj.mysqlx.protobuf.MysqlxDatatypes.Object.newBuilder()
                                .addFld(ObjectField.newBuilder().setKey("schema").setValue(ExprUtil.buildAny(schemaName)))
                                .addFld(ObjectField.newBuilder().setKey("pattern").setValue(ExprUtil.buildAny(pattern))))
                        .build()));
    }

    public void sendListObjects(String schemaName) {
        this.writer.write(this.msgBuilder.buildXpluginCommand(XpluginStatementCommand.XPLUGIN_STMT_LIST_OBJECTS,
                Any.newBuilder().setType(Any.Type.OBJECT).setObj(com.mysql.cj.mysqlx.protobuf.MysqlxDatatypes.Object.newBuilder()
                        .addFld(ObjectField.newBuilder().setKey("schema").setValue(ExprUtil.buildAny(schemaName)))).build()));
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
        this.writer.write(this.msgBuilder.buildXpluginCommand(XpluginStatementCommand.XPLUGIN_STMT_LIST_NOTICES));
    }

    public void sendEnableNotices(String... notices) {
        com.mysql.cj.mysqlx.protobuf.MysqlxDatatypes.Array.Builder abuilder = com.mysql.cj.mysqlx.protobuf.MysqlxDatatypes.Array.newBuilder();
        for (String notice : notices) {
            abuilder.addValue(ExprUtil.buildAny(notice));
        }
        this.writer
                .write(this.msgBuilder
                        .buildXpluginCommand(XpluginStatementCommand.XPLUGIN_STMT_ENABLE_NOTICES,
                                Any.newBuilder()
                                        .setType(Any.Type.OBJECT).setObj(com.mysql.cj.mysqlx.protobuf.MysqlxDatatypes.Object.newBuilder().addFld(ObjectField
                                                .newBuilder().setKey("notice").setValue(Any.newBuilder().setType(Any.Type.ARRAY).setArray(abuilder))))
                .build()));
    }

    public void sendDisableNotices(String... notices) {
        com.mysql.cj.mysqlx.protobuf.MysqlxDatatypes.Array.Builder abuilder = com.mysql.cj.mysqlx.protobuf.MysqlxDatatypes.Array.newBuilder();
        for (String notice : notices) {
            abuilder.addValue(ExprUtil.buildAny(notice));
        }
        this.writer
                .write(this.msgBuilder
                        .buildXpluginCommand(XpluginStatementCommand.XPLUGIN_STMT_DISABLE_NOTICES,
                                Any.newBuilder()
                                        .setType(Any.Type.OBJECT).setObj(com.mysql.cj.mysqlx.protobuf.MysqlxDatatypes.Object.newBuilder().addFld(ObjectField
                                                .newBuilder().setKey("notice").setValue(Any.newBuilder().setType(Any.Type.ARRAY).setArray(abuilder))))
                .build()));
    }

    public boolean hasMoreResults() {
        if (this.reader.getNextMessageClass() == FetchDoneMoreResultsets.class) {
            this.reader.read(FetchDoneMoreResultsets.class);
            if (this.reader.getNextMessageClass() == FetchDone.class) {
                // possibly bug in xplugin sending FetchDone immediately following FetchDoneMoreResultsets
                return false;
            }
            return true;
        }
        return false;
    }

    public StatementExecuteOk readStatementExecuteOk() {
        StatementExecuteOkBuilder builder = new StatementExecuteOkBuilder();
        if (this.reader.getNextMessageClass() == FetchDone.class) {
            this.reader.read(FetchDone.class);
        }
        while (this.reader.getNextMessageClass() == Frame.class) {
            builder.addNotice(this.reader.read(Frame.class));
        }
        this.reader.read(StmtExecuteOk.class);
        return builder.build();
    }

    public void sendSqlStatement(String statement) {
        sendSqlStatement(statement, null);
    }

    /**
     * @todo option for brief metadata (types only)
     */
    @SuppressWarnings("unchecked")
    public void sendSqlStatement(String statement, Object args) {
        this.writer.write(this.msgBuilder.buildSqlStatement(statement, (List<Any>) args));
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
     * Map a X Protocol type code from `ColumnMetaData.FieldType' to a MySQL type constant. These are the only types that will be present in {@link MysqlxRow}
     * results.
     *
     * @param type
     *            the type as the ColumnMetaData.FieldType
     * @param contentType
     *            the inner type
     * @return A <b>FIELD_TYPE</b> constant from {@link MysqlaConstants} corresponding to the combination of input parameters.
     */
    private static int mysqlxTypeToMysqlType(FieldType type, int contentType) {
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
                return MysqlaConstants.FIELD_TYPE_NEWDECIMAL;
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

    public static MysqlType findMysqlType(FieldType type, int contentType, int flags, int collationIndex) {
        switch (type) {
            case SINT:
                return MysqlType.BIGINT;
            case UINT:
                return MysqlType.BIGINT_UNSIGNED;
            case FLOAT:
                return 0 < (flags & MYSQLX_COLUMN_FLAGS_FLOAT_UNSIGNED) ? MysqlType.FLOAT_UNSIGNED : MysqlType.FLOAT;
            case DOUBLE:
                return 0 < (flags & MYSQLX_COLUMN_FLAGS_DOUBLE_UNSIGNED) ? MysqlType.DOUBLE_UNSIGNED : MysqlType.DOUBLE;
            case DECIMAL:
                return 0 < (flags & MYSQLX_COLUMN_FLAGS_DECIMAL_UNSIGNED) ? MysqlType.DECIMAL_UNSIGNED : MysqlType.DECIMAL;
            case BYTES:
                switch (contentType) {
                    case MYSQLX_COLUMN_BYTES_CONTENT_TYPE_GEOMETRY:
                        return MysqlType.GEOMETRY;
                    case MYSQLX_COLUMN_BYTES_CONTENT_TYPE_JSON:
                        return MysqlType.JSON;
                    default:
                        if (collationIndex == 33) {
                            return MysqlType.VARBINARY;
                        }
                        return MysqlType.VARCHAR;
                }
            case TIME:
                return MysqlType.TIME;
            case DATETIME:
                return MysqlType.DATETIME;
            case SET:
                return MysqlType.SET;
            case ENUM:
                return MysqlType.ENUM;
            case BIT:
                return MysqlType.BIT;
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
     * Convert a X Protocol {@link ColumnMetaData} message to a C/J {@link Field} object.
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

            long length = Integer.toUnsignedLong(col.getLength());
            int decimals = col.getFractionalDigits();
            int collationIndex = 0;
            if (col.hasCollation()) {
                // TODO: support custom character set
                collationIndex = (int) col.getCollation();
            }

            String encoding = CharsetMapping.COLLATION_INDEX_TO_COLLATION_NAME[collationIndex];

            MysqlType mysqlType = findMysqlType(col.getType(), col.getContentType(), col.getFlags(), collationIndex);
            int mysqlTypeId = mysqlxTypeToMysqlType(col.getType(), col.getContentType());

            // flags translation; unsigned is handled in Field by checking the MysqlType, so here we check others
            short flags = (short) 0;
            if (col.getType().equals(FieldType.UINT) && 0 < (col.getFlags() & MYSQLX_COLUMN_FLAGS_UINT_ZEROFILL)) {
                flags |= MysqlType.FIELD_FLAG_ZEROFILL;
            } else if (col.getType().equals(FieldType.BYTES) && 0 < (col.getFlags() & MYSQLX_COLUMN_FLAGS_BYTES_RIGHTPAD)) {
                mysqlType = MysqlType.CHAR;
            } else if (col.getType().equals(FieldType.DATETIME) && 0 < (col.getFlags() & MYSQLX_COLUMN_FLAGS_DATETIME_TIMESTAMP)) {
                mysqlType = MysqlType.TIMESTAMP;
            }
            if ((col.getFlags() & MYSQLX_COLUMN_FLAGS_NOT_NULL) > 0) {
                flags |= MysqlType.FIELD_FLAG_NOT_NULL;
            }
            if ((col.getFlags() & MYSQLX_COLUMN_FLAGS_PRIMARY_KEY) > 0) {
                flags |= MysqlType.FIELD_FLAG_PRIMARY_KEY;
            }
            if ((col.getFlags() & MYSQLX_COLUMN_FLAGS_UNIQUE_KEY) > 0) {
                flags |= MysqlType.FIELD_FLAG_UNIQUE_KEY;
            }
            if ((col.getFlags() & MYSQLX_COLUMN_FLAGS_MULTIPLE_KEY) > 0) {
                flags |= MysqlType.FIELD_FLAG_MULTIPLE_KEY;
            }
            if ((col.getFlags() & MYSQLX_COLUMN_FLAGS_AUTO_INCREMENT) > 0) {
                flags |= MysqlType.FIELD_FLAG_AUTO_INCREMENT;
            }

            Field f = new Field(databaseName, tableName, originalTableName, columnName, originalColumnName, length, mysqlTypeId, flags, decimals,
                    collationIndex, encoding, mysqlType);
            return f;
        } catch (UnsupportedEncodingException ex) {
            throw new WrongArgumentException("Unable to decode metadata strings", ex);
        }
    }

    public ArrayList<Field> readMetadata(String characterSet) {
        while (this.reader.getNextMessageClass() == Frame.class) {
            // TODO put notices somewhere like it's done eg. in readStatementExecuteOk(): builder.addNotice(this.reader.read(Frame.class));
            this.reader.read(Frame.class);
        }
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
        }
        return null;
    }

    public MysqlxRowInputStream getRowInputStream(ArrayList<Field> metadata) {
        return new MysqlxRowInputStream(metadata, this);
    }

    @SuppressWarnings("unchecked")
    public CompletableFuture<SqlResult> asyncExecuteSql(String sql, Object args, String metadataCharacterSet, TimeZone defaultTimeZone) {
        CompletableFuture<SqlResult> f = new CompletableFuture<>();
        MessageListener l = new SqlResultMessageListener(f, (col) -> columnMetaDataToField(this.propertySet, col, metadataCharacterSet), defaultTimeZone);
        CompletionHandler<Long, Void> resultHandler = new ErrorToFutureCompletionHandler<Long>(f,
                () -> ((AsyncMessageReader) this.reader).pushMessageListener(l));
        ((AsyncMessageWriter) this.writer).writeAsync(this.msgBuilder.buildSqlStatement(sql, (List<Any>) args), resultHandler);
        return f;
    }

    /**
     *
     * @param findParams
     * @param metadataCharacterSet
     * @param callbacks
     * @param errorFuture
     *            the future to complete exceptionally if the request fails
     */
    public void asyncFind(FindParams findParams, String metadataCharacterSet, ResultListener callbacks, CompletableFuture<?> errorFuture) {
        MessageListener l = new ResultMessageListener((col) -> columnMetaDataToField(this.propertySet, col, metadataCharacterSet), callbacks);
        CompletionHandler<Long, Void> resultHandler = new ErrorToFutureCompletionHandler<Long>(errorFuture,
                () -> ((AsyncMessageReader) this.reader).pushMessageListener(l));
        ((AsyncMessageWriter) this.writer).writeAsync(this.msgBuilder.buildFind(findParams), resultHandler);
    }

    private CompletableFuture<StatementExecuteOk> asyncUpdate(MessageLite commandMessage) {
        CompletableFuture<StatementExecuteOk> f = new CompletableFuture<>();
        final StatementExecuteOkMessageListener l = new StatementExecuteOkMessageListener(f);
        CompletionHandler<Long, Void> resultHandler = new ErrorToFutureCompletionHandler<Long>(f,
                () -> ((AsyncMessageReader) this.reader).pushMessageListener(l));
        ((AsyncMessageWriter) this.writer).writeAsync(commandMessage, resultHandler);
        return f;
    }

    public CompletableFuture<StatementExecuteOk> asyncAddDocs(String schemaName, String collectionName, List<String> jsonStrings) {
        return asyncUpdate(this.msgBuilder.buildDocInsert(schemaName, collectionName, jsonStrings));
    }

    public CompletableFuture<StatementExecuteOk> asyncInsertRows(String schemaName, String tableName, InsertParams insertParams) {
        return asyncUpdate(this.msgBuilder.buildRowInsert(schemaName, tableName, insertParams));
    }

    public CompletableFuture<StatementExecuteOk> asyncUpdateDocs(FilterParams filterParams, List<UpdateSpec> updates) {
        return asyncUpdate(this.msgBuilder.buildDocUpdate(filterParams, updates));
    }

    public CompletableFuture<StatementExecuteOk> asyncUpdateRows(FilterParams filterParams, UpdateParams updateParams) {
        return asyncUpdate(this.msgBuilder.buildRowUpdate(filterParams, updateParams));
    }

    public CompletableFuture<StatementExecuteOk> asyncDeleteDocs(FilterParams filterParams) {
        return asyncUpdate(this.msgBuilder.buildDelete(filterParams));
    }

    public CompletableFuture<StatementExecuteOk> asyncCreateCollectionIndex(String schemaName, String collectionName, CreateIndexParams params) {
        return asyncUpdate(this.msgBuilder.buildCreateCollectionIndex(schemaName, collectionName, params));
    }

    public CompletableFuture<StatementExecuteOk> asyncDropCollectionIndex(String schemaName, String collectionName, String indexName) {
        return asyncUpdate(this.msgBuilder.buildDropCollectionIndex(schemaName, collectionName, indexName));
    }

    public void sendFind(FindParams findParams) {
        this.writer.write(this.msgBuilder.buildFind(findParams));
    }

    public void sendDocUpdates(FilterParams filterParams, List<UpdateSpec> updates) {
        this.writer.write(this.msgBuilder.buildDocUpdate(filterParams, updates));
    }

    public void sendRowUpdates(FilterParams filterParams, UpdateParams updateParams) {
        this.writer.write(this.msgBuilder.buildRowUpdate(filterParams, updateParams));
    }

    public void sendDocDelete(FilterParams filterParams) {
        this.writer.write(this.msgBuilder.buildDelete(filterParams));
    }

    public void sendDocInsert(String schemaName, String collectionName, List<String> jsonStrings) {
        this.writer.write(this.msgBuilder.buildDocInsert(schemaName, collectionName, jsonStrings));
    }

    public void sendRowInsert(String schemaName, String tableName, InsertParams insertParams) {
        this.writer.write(this.msgBuilder.buildRowInsert(schemaName, tableName, insertParams));
    }

    public void sendSessionClose() {
        this.writer.write(Close.getDefaultInstance());
    }

    public String getPluginVersion() {
        return this.capabilities.get("plugin.version").getScalar().getVString().getValue().toStringUtf8();
    }

    public void sendCreateCollectionIndex(String schemaName, String collectionName, CreateIndexParams params) {
        this.writer.write(this.msgBuilder.buildCreateCollectionIndex(schemaName, collectionName, params));
    }

    public void sendDropCollectionIndex(String schemaName, String collectionName, String indexName) {
        this.writer.write(this.msgBuilder.buildDropCollectionIndex(schemaName, collectionName, indexName));
    }

    public boolean isOpen() {
        return this.managedResource != null;
    }

    public void close() throws IOException {
        if (this.managedResource == null) {
            throw new ConnectionIsClosedException();
        }
        this.managedResource.close();
        this.managedResource = null;
    }

    public boolean isSqlResultPending() {
        Class<?> nextMessageClass = this.reader.getNextMessageClass();
        if (nextMessageClass == ColumnMetaData.class) {
            return true;
        } else if (nextMessageClass == FetchDoneMoreResultsets.class) {
            this.reader.read(FetchDoneMoreResultsets.class);
        }
        return false;
    }

    /**
     * Get the server-assigned client ID. Not initialized until the <code>AuthenticateOk</code> is read.
     */
    public long getClientId() {
        return this.clientId;
    }

    @Override
    public void connect(String user, String password, String database) {
        // TODO Auto-generated method stub

    }

    public void setMaxAllowedPacket(int maxAllowedPacket) {
        this.writer.setMaxAllowedPacket(maxAllowedPacket);
    }
}
