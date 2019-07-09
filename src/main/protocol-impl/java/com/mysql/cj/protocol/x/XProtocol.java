/*
 * Copyright (c) 2015, 2019, Oracle and/or its affiliates. All rights reserved.
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

import static java.util.stream.Collectors.toMap;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.lang.ref.Reference;
import java.lang.ref.ReferenceQueue;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

import com.google.protobuf.GeneratedMessageV3;
import com.mysql.cj.CharsetMapping;
import com.mysql.cj.Constants;
import com.mysql.cj.Messages;
import com.mysql.cj.QueryResult;
import com.mysql.cj.Session;
import com.mysql.cj.TransactionEventHandler;
import com.mysql.cj.conf.HostInfo;
import com.mysql.cj.conf.PropertyDefinitions;
import com.mysql.cj.conf.PropertyDefinitions.SslMode;
import com.mysql.cj.conf.PropertyDefinitions.XdevapiSslMode;
import com.mysql.cj.conf.PropertyKey;
import com.mysql.cj.conf.PropertySet;
import com.mysql.cj.conf.RuntimeProperty;
import com.mysql.cj.exceptions.AssertionFailedException;
import com.mysql.cj.exceptions.CJCommunicationsException;
import com.mysql.cj.exceptions.CJConnectionFeatureNotAvailableException;
import com.mysql.cj.exceptions.CJOperationNotSupportedException;
import com.mysql.cj.exceptions.ConnectionIsClosedException;
import com.mysql.cj.exceptions.ExceptionFactory;
import com.mysql.cj.exceptions.ExceptionInterceptor;
import com.mysql.cj.exceptions.FeatureNotAvailableException;
import com.mysql.cj.exceptions.MysqlErrorNumbers;
import com.mysql.cj.exceptions.SSLParamsException;
import com.mysql.cj.exceptions.WrongArgumentException;
import com.mysql.cj.protocol.AbstractProtocol;
import com.mysql.cj.protocol.ColumnDefinition;
import com.mysql.cj.protocol.ExportControlled;
import com.mysql.cj.protocol.Message;
import com.mysql.cj.protocol.MessageListener;
import com.mysql.cj.protocol.MessageReader;
import com.mysql.cj.protocol.MessageSender;
import com.mysql.cj.protocol.Protocol;
import com.mysql.cj.protocol.ProtocolEntity;
import com.mysql.cj.protocol.ProtocolEntityFactory;
import com.mysql.cj.protocol.ResultBuilder;
import com.mysql.cj.protocol.ResultStreamer;
import com.mysql.cj.protocol.Resultset;
import com.mysql.cj.protocol.ServerCapabilities;
import com.mysql.cj.protocol.ServerSession;
import com.mysql.cj.protocol.SocketConnection;
import com.mysql.cj.protocol.a.NativeSocketConnection;
import com.mysql.cj.protocol.x.Notice.XSessionStateChanged;
import com.mysql.cj.result.DefaultColumnDefinition;
import com.mysql.cj.result.Field;
import com.mysql.cj.result.LongValueFactory;
import com.mysql.cj.util.SequentialIdLease;
import com.mysql.cj.util.StringUtils;
import com.mysql.cj.x.protobuf.Mysqlx.Error;
import com.mysql.cj.x.protobuf.Mysqlx.ServerMessages;
import com.mysql.cj.x.protobuf.MysqlxConnection.Capabilities;
import com.mysql.cj.x.protobuf.MysqlxConnection.Capability;
import com.mysql.cj.x.protobuf.MysqlxNotice.Frame;
import com.mysql.cj.x.protobuf.MysqlxResultset.ColumnMetaData;
import com.mysql.cj.x.protobuf.MysqlxResultset.FetchDone;
import com.mysql.cj.x.protobuf.MysqlxResultset.FetchDoneMoreResultsets;
import com.mysql.cj.x.protobuf.MysqlxResultset.Row;
import com.mysql.cj.x.protobuf.MysqlxSession.AuthenticateContinue;
import com.mysql.cj.x.protobuf.MysqlxSql.StmtExecuteOk;
import com.mysql.cj.xdevapi.PreparableStatement;
import com.mysql.cj.xdevapi.PreparableStatement.PreparableStatementFinalizer;

/**
 * Low-level interface to communications with X Plugin.
 */
public class XProtocol extends AbstractProtocol<XMessage> implements Protocol<XMessage> {
    private static int RETRY_PREPARE_STATEMENT_COUNTDOWN = 100;

    private MessageReader<XMessageHeader, XMessage> reader;
    private MessageSender<XMessage> sender;
    /** We take responsibility of the socket as the managed resource. We close it when we're done. */
    private Closeable managedResource;

    private ResultStreamer currentResultStreamer;

    XServerSession serverSession = null;
    Boolean useSessionResetKeepOpen = null;

    public String defaultSchemaName;

    private Map<String, Object> clientCapabilities = new HashMap<>();

    /** Keeps track of whether this X Server session supports prepared statements. True by default until first failure of a statement prepare. */
    private boolean supportsPreparedStatements = true;
    private int retryPrepareStatementCountdown = 0;
    private SequentialIdLease preparedStatementIds = new SequentialIdLease();
    private ReferenceQueue<PreparableStatement<?>> preparableStatementRefQueue = new ReferenceQueue<>();
    private Map<Integer, PreparableStatementFinalizer> preparableStatementFinalizerReferences = new TreeMap<>();

    private Map<Class<? extends GeneratedMessageV3>, ProtocolEntityFactory<? extends ProtocolEntity, XMessage>> messageToProtocolEntityFactory = new HashMap<>();

    public XProtocol(String host, int port, String defaultSchema, PropertySet propertySet) {

        this.defaultSchemaName = defaultSchema;

        // Override common connectTimeout with xdevapi.connect-timeout to provide unified logic in StandardSocketFactory
        RuntimeProperty<Integer> connectTimeout = propertySet.getIntegerProperty(PropertyKey.connectTimeout);
        RuntimeProperty<Integer> xdevapiConnectTimeout = propertySet.getIntegerProperty(PropertyKey.xdevapiConnectTimeout);
        if (xdevapiConnectTimeout.isExplicitlySet() || !connectTimeout.isExplicitlySet()) {
            connectTimeout.setValue(xdevapiConnectTimeout.getValue());
        }

        SocketConnection socketConn = propertySet.getBooleanProperty(PropertyKey.xdevapiUseAsyncProtocol).getValue() ? new XAsyncSocketConnection()
                : new NativeSocketConnection();
        socketConn.connect(host, port, propertySet, null, null, 0);
        init(null, socketConn, propertySet, null);
    }

    public XProtocol(HostInfo hostInfo, PropertySet propertySet) {
        String host = hostInfo.getHost();
        if (host == null || StringUtils.isEmptyOrWhitespaceOnly(host)) {
            host = "localhost";
        }
        int port = hostInfo.getPort();
        if (port < 0) {
            port = 33060;
        }
        this.defaultSchemaName = hostInfo.getDatabase();

        // Override common connectTimeout with xdevapi.connect-timeout to provide unified logic in StandardSocketFactory
        RuntimeProperty<Integer> connectTimeout = propertySet.getIntegerProperty(PropertyKey.connectTimeout);
        RuntimeProperty<Integer> xdevapiConnectTimeout = propertySet.getIntegerProperty(PropertyKey.xdevapiConnectTimeout);
        if (xdevapiConnectTimeout.isExplicitlySet() || !connectTimeout.isExplicitlySet()) {
            connectTimeout.setValue(xdevapiConnectTimeout.getValue());
        }

        SocketConnection socketConn = propertySet.getBooleanProperty(PropertyKey.xdevapiUseAsyncProtocol).getValue() ? new XAsyncSocketConnection()
                : new NativeSocketConnection();
        socketConn.connect(host, port, propertySet, null, null, 0);
        init(null, socketConn, propertySet, null);
    }

    @Override
    public void init(Session sess, SocketConnection socketConn, PropertySet propSet, TransactionEventHandler trManager) {
        super.init(sess, socketConn, propSet, trManager);

        this.messageBuilder = new XMessageBuilder();

        this.authProvider = new XAuthenticationProvider();
        this.authProvider.init(this, propSet, null);

        this.useSessionResetKeepOpen = null;

        this.messageToProtocolEntityFactory.put(ColumnMetaData.class, new FieldFactory("latin1")); // TODO configure metadata character set from server session
        this.messageToProtocolEntityFactory.put(Frame.class, new NoticeFactory());
        this.messageToProtocolEntityFactory.put(Row.class, new XProtocolRowFactory());
        this.messageToProtocolEntityFactory.put(FetchDoneMoreResultsets.class, new FetchDoneMoreResultsFactory());
        this.messageToProtocolEntityFactory.put(FetchDone.class, new FetchDoneEntityFactory());
        this.messageToProtocolEntityFactory.put(StmtExecuteOk.class, new StatementExecuteOkFactory());
        this.messageToProtocolEntityFactory.put(com.mysql.cj.x.protobuf.Mysqlx.Ok.class, new OkFactory());
    }

    public ServerSession getServerSession() {
        return this.serverSession;
    }

    /**
     * Set client capabilities of current session. Must be done before authentication ({@link #changeUser(String, String, String)}).
     * 
     * @param keyValuePair
     *            capabilities name/value map
     */
    public void sendCapabilities(Map<String, Object> keyValuePair) {
        keyValuePair.forEach((k, v) -> ((XServerCapabilities) getServerSession().getCapabilities()).setCapability(k, v));
        this.sender.send(((XMessageBuilder) this.messageBuilder).buildCapabilitiesSet(keyValuePair));
        readQueryResult(new OkBuilder());
    }

    public void negotiateSSLConnection(int packLength) {

        if (!ExportControlled.enabled()) {
            throw new CJConnectionFeatureNotAvailableException();
        }

        if (!((XServerCapabilities) this.serverSession.getCapabilities()).hasCapability(XServerCapabilities.KEY_TLS)) {
            throw new CJCommunicationsException("A secure connection is required but the server is not configured with SSL.");
        }

        // the message reader is async and is always "reading". we need to stop it to use the socket for the TLS handshake
        this.reader.stopAfterNextMessage();

        Map<String, Object> tlsCapabilities = new HashMap<>();
        tlsCapabilities.put(XServerCapabilities.KEY_TLS, true);
        sendCapabilities(tlsCapabilities);

        try {
            this.socketConnection.performTlsHandshake(null); //(this.serverSession);
        } catch (SSLParamsException | FeatureNotAvailableException | IOException e) {
            throw new CJCommunicationsException(e);
        }

        try {
            if (this.socketConnection.isSynchronous()) {
                // i/o streams were replaced, build new packet sender/reader
                this.sender = new SyncMessageSender(this.socketConnection.getMysqlOutput());
                this.reader = new SyncMessageReader(this.socketConnection.getMysqlInput());
            } else {
                // resume message processing
                ((AsyncMessageSender) this.sender).setChannel(this.socketConnection.getAsynchronousSocketChannel());
                this.reader.start();
            }
        } catch (IOException e) {
            throw new XProtocolError(e.getMessage(), e);
        }

    }

    public void beforeHandshake() {
        this.serverSession = new XServerSession();

        try {
            if (this.socketConnection.isSynchronous()) {
                this.sender = new SyncMessageSender(this.socketConnection.getMysqlOutput());
                this.reader = new SyncMessageReader(this.socketConnection.getMysqlInput());
                this.managedResource = this.socketConnection.getMysqlSocket();
            } else {
                this.sender = new AsyncMessageSender(this.socketConnection.getAsynchronousSocketChannel());
                this.reader = new AsyncMessageReader(this.propertySet, this.socketConnection);
                this.reader.start();
                this.managedResource = this.socketConnection.getAsynchronousSocketChannel();
            }
        } catch (IOException e) {
            throw new XProtocolError(e.getMessage(), e);
        }

        this.serverSession.setCapabilities(readServerCapabilities());

        // connection attributes
        String attributes = this.propertySet.getStringProperty(PropertyKey.xdevapiConnectionAttributes).getValue();
        if (attributes == null || !attributes.equalsIgnoreCase("false")) {
            Map<String, String> attMap = getConnectionAttributesMap("true".equalsIgnoreCase(attributes) ? "" : attributes);
            this.clientCapabilities.put(XServerCapabilities.KEY_SESSION_CONNECT_ATTRS, attMap);
        }

        // Override common SSL properties with xdevapi ones to provide unified logic in ExportControlled via common SSL properties
        RuntimeProperty<XdevapiSslMode> xdevapiSslMode = this.propertySet.<XdevapiSslMode>getEnumProperty(PropertyKey.xdevapiSSLMode);
        if (xdevapiSslMode.isExplicitlySet()) {
            this.propertySet.<SslMode>getEnumProperty(PropertyKey.sslMode).setValue(SslMode.valueOf(xdevapiSslMode.getValue().toString()));
        }
        RuntimeProperty<String> sslTrustStoreUrl = this.propertySet.getStringProperty(PropertyKey.xdevapiSSLTrustStoreUrl);
        if (sslTrustStoreUrl.isExplicitlySet()) {
            this.propertySet.getStringProperty(PropertyKey.trustCertificateKeyStoreUrl).setValue(sslTrustStoreUrl.getValue());
        }
        RuntimeProperty<String> sslTrustStoreType = this.propertySet.getStringProperty(PropertyKey.xdevapiSSLTrustStoreType);
        if (sslTrustStoreType.isExplicitlySet()) {
            this.propertySet.getStringProperty(PropertyKey.trustCertificateKeyStoreType).setValue(sslTrustStoreType.getValue());
        }
        RuntimeProperty<String> sslTrustStorePassword = this.propertySet.getStringProperty(PropertyKey.xdevapiSSLTrustStorePassword);
        if (sslTrustStorePassword.isExplicitlySet()) {
            this.propertySet.getStringProperty(PropertyKey.trustCertificateKeyStorePassword).setValue(sslTrustStorePassword.getValue());
        }

        // TODO WL#9925 will redefine other SSL connection properties for X Protocol

        RuntimeProperty<SslMode> sslMode = this.propertySet.<SslMode>getEnumProperty(PropertyKey.sslMode);

        if (sslMode.getValue() == SslMode.PREFERRED) { // PREFERRED mode is not applicable to X Protocol
            sslMode.setValue(SslMode.REQUIRED);
        }

        boolean verifyServerCert = sslMode.getValue() == SslMode.VERIFY_CA || sslMode.getValue() == SslMode.VERIFY_IDENTITY;
        String trustStoreUrl = this.propertySet.getStringProperty(PropertyKey.trustCertificateKeyStoreUrl).getValue();

        if (!verifyServerCert && !StringUtils.isNullOrEmpty(trustStoreUrl)) {
            StringBuilder msg = new StringBuilder("Incompatible security settings. The property '");
            msg.append(PropertyKey.xdevapiSSLTrustStoreUrl.getKeyName()).append("' requires '");
            msg.append(PropertyKey.xdevapiSSLMode.getKeyName()).append("' as '");
            msg.append(PropertyDefinitions.SslMode.VERIFY_CA).append("' or '");
            msg.append(PropertyDefinitions.SslMode.VERIFY_IDENTITY).append("'.");
            throw new CJCommunicationsException(msg.toString());
        }

        if (this.clientCapabilities.size() > 0) {
            try {
                sendCapabilities(this.clientCapabilities);
            } catch (XProtocolError e) {
                // XProtocolError: ERROR 5002 (HY000) Capability 'session_connect_attrs' doesn't exist
                // happens when connecting to xplugin which doesn't support this feature. Just ignore this error.
                if (e.getErrorCode() != 5002 && !e.getMessage().contains(XServerCapabilities.KEY_SESSION_CONNECT_ATTRS)) {
                    throw e;
                }
                this.clientCapabilities.remove(XServerCapabilities.KEY_SESSION_CONNECT_ATTRS);
            }
        }

        if (xdevapiSslMode.getValue() != XdevapiSslMode.DISABLED) {
            negotiateSSLConnection(0);
        }
    }

    private Map<String, String> getConnectionAttributesMap(String attStr) {
        Map<String, String> attMap = new HashMap<>();

        if (attStr != null) {
            if (attStr.startsWith("[") && attStr.endsWith("]")) {
                attStr = attStr.substring(1, attStr.length() - 1);
            }
            if (!StringUtils.isNullOrEmpty(attStr)) {
                String[] pairs = attStr.split(",");
                for (String pair : pairs) {
                    String[] kv = pair.split("=");
                    String key = kv[0].trim();
                    String value = kv.length > 1 ? kv[1].trim() : "";
                    if (key.startsWith("_")) {
                        throw ExceptionFactory.createException(WrongArgumentException.class, Messages.getString("Protocol.WrongAttributeName"));
                    } else if (attMap.put(key, value) != null) {
                        throw ExceptionFactory.createException(WrongArgumentException.class,
                                Messages.getString("Protocol.DuplicateAttribute", new Object[] { key }));
                    }
                }
            }
        }

        attMap.put("_platform", Constants.OS_ARCH);
        attMap.put("_os", Constants.OS_NAME + "-" + Constants.OS_VERSION);
        attMap.put("_client_name", Constants.CJ_NAME);
        attMap.put("_client_version", Constants.CJ_VERSION);
        attMap.put("_client_license", Constants.CJ_LICENSE);
        attMap.put("_runtime_version", Constants.JVM_VERSION);
        attMap.put("_runtime_vendor", Constants.JVM_VENDOR);
        return attMap;
    }

    private String currUser = null, currPassword = null, currDatabase = null; // TODO remove these variables after implementing mysql_reset_connection() in reset() method

    @Override
    public void connect(String user, String password, String database) {
        this.currUser = user;
        this.currPassword = password;
        this.currDatabase = database;

        beforeHandshake();
        this.authProvider.connect(null, user, password, database);
    }

    public void changeUser(String user, String password, String database) {
        this.currUser = user;
        this.currPassword = password;
        this.currDatabase = database;

        this.authProvider.changeUser(null, user, password, database);
    }

    public void afterHandshake() {
        // TODO setup all required server session states
        initServerSession();
    }

    @Override
    public void configureTimezone() {
        // no-op
    }

    @Override
    public void initServerSession() {
        configureTimezone();

        send(this.messageBuilder.buildSqlStatement("select @@mysqlx_max_allowed_packet"), 0);
        // TODO: can use a simple default for this as we don't need metadata. need to prevent against exceptions though
        ColumnDefinition metadata = readMetadata();
        long count = new XProtocolRowInputStream(metadata, this, null).next().getValue(0, new LongValueFactory(this.propertySet));
        readQueryResult(new StatementExecuteOkBuilder());
        setMaxAllowedPacket((int) count);
    }

    public void readAuthenticateOk() {
        try {
            XMessage mess = this.reader.readMessage(null, ServerMessages.Type.SESS_AUTHENTICATE_OK_VALUE);
            if (mess != null && mess.getNotices() != null) {
                for (Notice notice : mess.getNotices()) {
                    if (notice instanceof XSessionStateChanged) {
                        switch (((XSessionStateChanged) notice).getParamType()) {
                            case Notice.SessionStateChanged_CLIENT_ID_ASSIGNED:
                                this.getServerSession().setThreadId(((XSessionStateChanged) notice).getValue().getVUnsignedInt());
                                break;
                            case Notice.SessionStateChanged_ACCOUNT_EXPIRED:
                                // TODO
                                break;
                            default:
                                break;
                        }
                    }
                }
            }
        } catch (IOException e) {
            throw new XProtocolError(e.getMessage(), e);
        }
    }

    public byte[] readAuthenticateContinue() {
        try {
            AuthenticateContinue msg = (AuthenticateContinue) this.reader.readMessage(null, ServerMessages.Type.SESS_AUTHENTICATE_CONTINUE_VALUE).getMessage();
            byte[] data = msg.getAuthData().toByteArray();
            if (data.length != 20) {
                throw AssertionFailedException.shouldNotHappen("Salt length should be 20, but is " + data.length);
            }
            return data;
        } catch (IOException e) {
            throw new XProtocolError(e.getMessage(), e);
        }
    }

    public boolean hasMoreResults() {
        try {
            XMessageHeader header;
            if ((header = this.reader.readHeader()).getMessageType() == ServerMessages.Type.RESULTSET_FETCH_DONE_MORE_RESULTSETS_VALUE) {
                this.reader.readMessage(null, header);
                if (this.reader.readHeader().getMessageType() == ServerMessages.Type.RESULTSET_FETCH_DONE_VALUE) {
                    // possibly bug in xplugin sending FetchDone immediately following FetchDoneMoreResultsets
                    return false;
                }
                return true;
            }
            return false;
        } catch (IOException e) {
            throw new XProtocolError(e.getMessage(), e);
        }
    }

    public <T extends QueryResult> T readQueryResult(ResultBuilder<T> resultBuilder) {
        try {
            List<Notice> notices;
            boolean done = false;

            while (!done) {
                XMessageHeader header = this.reader.readHeader();
                XMessage mess = this.reader.readMessage(null, header);
                @SuppressWarnings("unchecked")
                Class<? extends GeneratedMessageV3> msgClass = (Class<? extends GeneratedMessageV3>) mess.getMessage().getClass();

                if (Error.class.equals(msgClass)) {
                    throw new XProtocolError(Error.class.cast(mess.getMessage()));

                } else if (!this.messageToProtocolEntityFactory.containsKey(msgClass)) {
                    throw new WrongArgumentException("Unhandled msg class (" + msgClass + ") + msg=" + mess.getMessage());

                }

                if ((notices = mess.getNotices()) != null) {
                    notices.stream().forEach(resultBuilder::addProtocolEntity);
                }
                done = resultBuilder.addProtocolEntity(this.messageToProtocolEntityFactory.get(msgClass).createFromMessage(mess));

            }
            return resultBuilder.build();
        } catch (IOException e) {
            throw new XProtocolError(e.getMessage(), e);
        }
    }

    /**
     * Used only in tests
     * 
     * @return true if there are result rows
     */
    public boolean hasResults() {
        try {
            return this.reader.readHeader().getMessageType() == ServerMessages.Type.RESULTSET_COLUMN_META_DATA_VALUE;
        } catch (IOException e) {
            throw new XProtocolError(e.getMessage(), e);
        }
    }

    /**
     * Used only in tests
     */
    public void drainRows() {
        try {
            XMessageHeader header;
            while ((header = this.reader.readHeader()).getMessageType() == ServerMessages.Type.RESULTSET_ROW_VALUE) {
                this.reader.readMessage(null, header);
            }
        } catch (XProtocolError e) {
            this.currentResultStreamer = null;
            throw e;
        } catch (IOException e) {
            this.currentResultStreamer = null;
            throw new XProtocolError(e.getMessage(), e);
        }
    }

    // TODO: put this in CharsetMapping..
    public static Map<String, Integer> COLLATION_NAME_TO_COLLATION_INDEX = new java.util.HashMap<>();

    static {
        for (int i = 0; i < CharsetMapping.COLLATION_INDEX_TO_COLLATION_NAME.length; ++i) {
            COLLATION_NAME_TO_COLLATION_INDEX.put(CharsetMapping.COLLATION_INDEX_TO_COLLATION_NAME[i], i);
        }
    }

    public ColumnDefinition readMetadata() {
        try {
            List<ColumnMetaData> fromServer = new LinkedList<>();
            do { // use this construct to read at least one
                fromServer.add((ColumnMetaData) this.reader.readMessage(null, ServerMessages.Type.RESULTSET_COLUMN_META_DATA_VALUE).getMessage());
                // TODO put notices somewhere like it's done eg. in readStatementExecuteOk(): builder.addNotice(this.reader.read(Frame.class));
            } while (this.reader.readHeader().getMessageType() == ServerMessages.Type.RESULTSET_COLUMN_META_DATA_VALUE);
            ArrayList<Field> metadata = new ArrayList<>(fromServer.size());
            @SuppressWarnings("unchecked")
            ProtocolEntityFactory<Field, XMessage> fieldFactory = (ProtocolEntityFactory<Field, XMessage>) this.messageToProtocolEntityFactory
                    .get(ColumnMetaData.class);
            fromServer.forEach(col -> metadata.add(fieldFactory.createFromMessage(new XMessage(col))));

            return new DefaultColumnDefinition(metadata.toArray(new Field[] {}));
        } catch (IOException e) {
            throw new XProtocolError(e.getMessage(), e);
        }
    }

    public ColumnDefinition readMetadata(Field f, Consumer<Notice> noticeConsumer) {
        try {
            List<Notice> notices;
            List<ColumnMetaData> fromServer = new LinkedList<>();
            while (this.reader.readHeader().getMessageType() == ServerMessages.Type.RESULTSET_COLUMN_META_DATA_VALUE) { // use this construct to read at least one
                XMessage mess = this.reader.readMessage(null, ServerMessages.Type.RESULTSET_COLUMN_META_DATA_VALUE);
                if (noticeConsumer != null && (notices = mess.getNotices()) != null) {
                    notices.stream().forEach(noticeConsumer::accept);
                }
                fromServer.add((ColumnMetaData) mess.getMessage());
            }
            ArrayList<Field> metadata = new ArrayList<>(fromServer.size());
            metadata.add(f);
            @SuppressWarnings("unchecked")
            ProtocolEntityFactory<Field, XMessage> fieldFactory = (ProtocolEntityFactory<Field, XMessage>) this.messageToProtocolEntityFactory
                    .get(ColumnMetaData.class);
            fromServer.forEach(col -> metadata.add(fieldFactory.createFromMessage(new XMessage(col))));

            return new DefaultColumnDefinition(metadata.toArray(new Field[] {}));
        } catch (IOException e) {
            throw new XProtocolError(e.getMessage(), e);
        }
    }

    public XProtocolRow readRowOrNull(ColumnDefinition metadata, Consumer<Notice> noticeConsumer) {
        try {
            List<Notice> notices;
            XMessageHeader header;
            if ((header = this.reader.readHeader()).getMessageType() == ServerMessages.Type.RESULTSET_ROW_VALUE) {
                XMessage mess = this.reader.readMessage(null, header);
                if (noticeConsumer != null && (notices = mess.getNotices()) != null) {
                    notices.stream().forEach(noticeConsumer::accept);
                }
                XProtocolRow res = new XProtocolRow((Row) mess.getMessage());
                res.setMetadata(metadata);
                return res;
            }
            return null;
        } catch (XProtocolError e) {
            this.currentResultStreamer = null;
            throw e;
        } catch (IOException e) {
            this.currentResultStreamer = null;
            throw new XProtocolError(e.getMessage(), e);
        }
    }

    /**
     * Checks if the MySQL server currently connected supports prepared statements.
     * 
     * @return
     *         {@code true} if the MySQL server currently connected supports prepared statements.
     */
    public boolean supportsPreparedStatements() {
        return this.supportsPreparedStatements;
    }

    /**
     * Checks if enough statements have been executed in this MySQL server so that another prepare statement attempt should be done.
     * 
     * @return
     *         {@code true} if enough executions have been done since last time a prepared statement failed to prepare
     */
    public boolean readyForPreparingStatements() {
        if (this.retryPrepareStatementCountdown == 0) {
            return true;
        }
        this.retryPrepareStatementCountdown--;
        return false;
    }

    /**
     * Returns an id to be used as a client-managed prepared statement id. The method {@link #freePreparedStatementId(int)} must be called when the prepared
     * statement is deallocated so that the same id can be re-used.
     * 
     * @param preparableStatement
     *            {@link PreparableStatement}
     * 
     * @return a new identifier to be used as prepared statement id
     */
    public int getNewPreparedStatementId(PreparableStatement<?> preparableStatement) {
        if (!this.supportsPreparedStatements) {
            throw new XProtocolError("The connected MySQL server does not support prepared statements.");
        }
        int preparedStatementId = this.preparedStatementIds.allocateSequentialId();
        this.preparableStatementFinalizerReferences.put(preparedStatementId,
                new PreparableStatementFinalizer(preparableStatement, this.preparableStatementRefQueue, preparedStatementId));
        return preparedStatementId;
    }

    /**
     * Frees a prepared statement id so that it can be reused. Note that freeing an id from an active prepared statement will result in a statement prepare
     * conflict next time one gets prepared with the same released id.
     * 
     * @param preparedStatementId
     *            the prepared statement id to release
     */
    public void freePreparedStatementId(int preparedStatementId) {
        if (!this.supportsPreparedStatements) {
            throw new XProtocolError("The connected MySQL server does not support prepared statements.");
        }
        this.preparedStatementIds.releaseSequentialId(preparedStatementId);
        this.preparableStatementFinalizerReferences.remove(preparedStatementId);
    }

    /**
     * Informs this protocol instance that preparing a statement on the connected server failed.
     * 
     * @param preparedStatementId
     *            the id of the prepared statement that failed to prepare
     * @param e
     *            {@link XProtocolError}
     * @return
     *         {@code true} if the exception was properly handled
     */
    public boolean failedPreparingStatement(int preparedStatementId, XProtocolError e) {
        freePreparedStatementId(preparedStatementId);

        if (e.getErrorCode() == MysqlErrorNumbers.ER_MAX_PREPARED_STMT_COUNT_REACHED) {
            this.retryPrepareStatementCountdown = RETRY_PREPARE_STATEMENT_COUNTDOWN;
            return true;
        }

        if (e.getErrorCode() == MysqlErrorNumbers.ER_UNKNOWN_COM_ERROR && this.preparableStatementFinalizerReferences.isEmpty()) {
            // The server doesn't recognize the protocol message, so it doesn't support prepared statements.
            this.supportsPreparedStatements = false;
            this.retryPrepareStatementCountdown = 0;
            this.preparedStatementIds = null;
            this.preparableStatementRefQueue = null;
            this.preparableStatementFinalizerReferences = null;
            return true;
        }

        return false;
    }

    /**
     * Signal the intent to start processing a new command. A session supports processing a single command at a time. Results are reading lazily from the
     * wire. It is necessary to flush any pending result before starting a new command. This method performs the flush if necessary.
     */
    protected void newCommand() {
        if (this.currentResultStreamer != null) {
            try {
                this.currentResultStreamer.finishStreaming();
            } finally {
                // so we don't call finishStreaming() again if there's an exception
                this.currentResultStreamer = null;
            }
        }

        // Before continuing clean up any abandoned prepared statements that were not properly deallocated.
        if (this.supportsPreparedStatements) {
            Reference<? extends PreparableStatement<?>> ref;
            while ((ref = this.preparableStatementRefQueue.poll()) != null) {
                PreparableStatementFinalizer psf = (PreparableStatementFinalizer) ref;
                psf.clear();
                try {
                    this.sender.send(((XMessageBuilder) this.messageBuilder).buildPrepareDeallocate(psf.getPreparedStatementId()));
                    readQueryResult(new OkBuilder());
                } catch (XProtocolError e) {
                    if (e.getErrorCode() != MysqlErrorNumbers.ER_X_BAD_STATEMENT_ID) {
                        throw e;
                    } // Else ignore exception, the Statement may have been deallocated elsewhere.
                } finally {
                    freePreparedStatementId(psf.getPreparedStatementId());
                }
            }
        }
    }

    public <M extends Message, R extends QueryResult> R query(M message, ResultBuilder<R> resultBuilder) {
        send(message, 0);
        R res = readQueryResult(resultBuilder);
        if (ResultStreamer.class.isAssignableFrom(res.getClass())) {
            this.currentResultStreamer = (ResultStreamer) res;
        }
        return res;
    }

    public <M extends Message, R extends QueryResult> CompletableFuture<R> queryAsync(M message, ResultBuilder<R> resultBuilder) {
        newCommand();
        CompletableFuture<R> f = new CompletableFuture<>();
        MessageListener<XMessage> l = new ResultMessageListener<>(this.messageToProtocolEntityFactory, resultBuilder, f);
        this.sender.send((XMessage) message, f, () -> this.reader.pushMessageListener(l));
        return f;
    }

    public boolean isOpen() {
        return this.managedResource != null;
    }

    public void close() throws IOException {
        try {
            send(this.messageBuilder.buildClose(), 0);
            readQueryResult(new OkBuilder());
        } catch (Exception e) {
            // ignore exceptions
        } finally {
            try {
                if (this.managedResource == null) {
                    throw new ConnectionIsClosedException();
                }
                this.managedResource.close();
                this.managedResource = null;
            } catch (IOException ex) {
                throw new CJCommunicationsException(ex);
            }
        }
    }

    public boolean isSqlResultPending() {
        try {
            XMessageHeader header;
            switch ((header = this.reader.readHeader()).getMessageType()) {
                case ServerMessages.Type.RESULTSET_COLUMN_META_DATA_VALUE:
                    return true;
                case ServerMessages.Type.RESULTSET_FETCH_DONE_MORE_RESULTSETS_VALUE:
                    this.reader.readMessage(null, header);
                    break;
                default:
                    break;
            }
            return false;
        } catch (IOException e) {
            throw new XProtocolError(e.getMessage(), e);
        }
    }

    public void setMaxAllowedPacket(int maxAllowedPacket) {
        this.sender.setMaxAllowedPacket(maxAllowedPacket);
    }

    @Override
    public void send(Message message, int packetLen) {
        newCommand();
        this.sender.send((XMessage) message);
    }

    /**
     * Get the capabilities from the server.
     * <p>
     * <b>NOTE:</b> This must be called before authentication.
     * 
     * @return capabilities mapped by name
     */
    public ServerCapabilities readServerCapabilities() {
        try {
            this.sender.send(((XMessageBuilder) this.messageBuilder).buildCapabilitiesGet());
            return new XServerCapabilities(((Capabilities) this.reader.readMessage(null, ServerMessages.Type.CONN_CAPABILITIES_VALUE).getMessage())
                    .getCapabilitiesList().stream().collect(toMap(Capability::getName, Capability::getValue)));
        } catch (IOException | AssertionFailedException e) {
            throw new XProtocolError(e.getMessage(), e);
        }
    }

    @Override
    public void reset() {
        newCommand();
        this.propertySet.reset();

        if (this.useSessionResetKeepOpen == null) {
            try {
                send(((XMessageBuilder) this.messageBuilder).buildExpectOpen(), 0);
                readQueryResult(new OkBuilder());
                this.useSessionResetKeepOpen = true;
            } catch (XProtocolError e) {
                if (e.getErrorCode() != 5168 && /* for MySQL 5.7 */ e.getErrorCode() != 5160) {
                    throw e;
                }
                this.useSessionResetKeepOpen = false;
            }
        }
        if (this.useSessionResetKeepOpen) {
            send(((XMessageBuilder) this.messageBuilder).buildSessionResetKeepOpen(), 0);
            readQueryResult(new OkBuilder());
        } else {
            send(((XMessageBuilder) this.messageBuilder).buildSessionResetAndClose(), 0);
            readQueryResult(new OkBuilder());

            if (this.clientCapabilities.containsKey(XServerCapabilities.KEY_SESSION_CONNECT_ATTRS)) {
                // this code may never work because xplugin connection attributes were introduced later than new session reset
                Map<String, Object> reducedClientCapabilities = new HashMap<>();
                reducedClientCapabilities.put(XServerCapabilities.KEY_SESSION_CONNECT_ATTRS,
                        this.clientCapabilities.get(XServerCapabilities.KEY_SESSION_CONNECT_ATTRS));
                if (reducedClientCapabilities.size() > 0) {
                    sendCapabilities(reducedClientCapabilities);
                }
            }

            this.authProvider.changeUser(null, this.currUser, this.currPassword, this.currDatabase);
        }

        // No prepared statements survived to Mysqlx.Session.Reset. Reset all related control structures.
        if (this.supportsPreparedStatements) {
            this.retryPrepareStatementCountdown = 0;
            this.preparedStatementIds = new SequentialIdLease();
            this.preparableStatementRefQueue = new ReferenceQueue<>();
            this.preparableStatementFinalizerReferences = new TreeMap<>();
        }
    }

    @Override
    public ExceptionInterceptor getExceptionInterceptor() {
        throw ExceptionFactory.createException(CJOperationNotSupportedException.class, "Not supported");
    }

    public void changeDatabase(String database) {
        throw ExceptionFactory.createException(CJOperationNotSupportedException.class, "Not supported");
        // TODO: Figure out how this is relevant for X Protocol client Session
    }

    public String getPasswordCharacterEncoding() {
        throw ExceptionFactory.createException(CJOperationNotSupportedException.class, "Not supported");
    }

    public boolean versionMeetsMinimum(int major, int minor, int subminor) {
        //TODO: expose this via ServerVersion so calls look like x.getServerVersion().meetsMinimum(major, minor, subminor)
        throw ExceptionFactory.createException(CJOperationNotSupportedException.class, "Not supported");
    }

    @Override
    public XMessage readMessage(XMessage reuse) {
        throw ExceptionFactory.createException(CJOperationNotSupportedException.class, "Not supported");
    }

    @Override
    public XMessage checkErrorMessage() {
        throw ExceptionFactory.createException(CJOperationNotSupportedException.class, "Not supported");
    }

    @Override
    public XMessage sendCommand(Message queryPacket, boolean skipCheck, int timeoutMillis) {
        throw ExceptionFactory.createException(CJOperationNotSupportedException.class, "Not supported");
    }

    @Override
    public <T extends ProtocolEntity> T read(Class<T> requiredClass, ProtocolEntityFactory<T, XMessage> protocolEntityFactory) throws IOException {
        throw ExceptionFactory.createException(CJOperationNotSupportedException.class, "Not supported");
    }

    @Override
    public <T extends ProtocolEntity> T read(Class<Resultset> requiredClass, int maxRows, boolean streamResults, XMessage resultPacket, boolean isBinaryEncoded,
            ColumnDefinition metadata, ProtocolEntityFactory<T, XMessage> protocolEntityFactory) throws IOException {
        throw ExceptionFactory.createException(CJOperationNotSupportedException.class, "Not supported");
    }

    @Override
    public void setLocalInfileInputStream(InputStream stream) {
        throw ExceptionFactory.createException(CJOperationNotSupportedException.class, "Not supported");
    }

    @Override
    public InputStream getLocalInfileInputStream() {
        throw ExceptionFactory.createException(CJOperationNotSupportedException.class, "Not supported");
    }

    @Override
    public String getQueryComment() {
        throw ExceptionFactory.createException(CJOperationNotSupportedException.class, "Not supported");
    }

    @Override
    public void setQueryComment(String comment) {
        throw ExceptionFactory.createException(CJOperationNotSupportedException.class, "Not supported");
    }
}
