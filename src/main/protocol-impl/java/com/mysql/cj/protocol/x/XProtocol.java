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

import static java.util.stream.Collectors.toMap;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.CompletionHandler;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import com.mysql.cj.CharsetMapping;
import com.mysql.cj.QueryResult;
import com.mysql.cj.Session;
import com.mysql.cj.TransactionEventHandler;
import com.mysql.cj.conf.PropertyDefinitions;
import com.mysql.cj.conf.PropertyDefinitions.SslMode;
import com.mysql.cj.conf.PropertySet;
import com.mysql.cj.exceptions.AssertionFailedException;
import com.mysql.cj.exceptions.CJCommunicationsException;
import com.mysql.cj.exceptions.CJOperationNotSupportedException;
import com.mysql.cj.exceptions.ConnectionIsClosedException;
import com.mysql.cj.exceptions.ExceptionFactory;
import com.mysql.cj.exceptions.ExceptionInterceptor;
import com.mysql.cj.exceptions.FeatureNotAvailableException;
import com.mysql.cj.exceptions.SSLParamsException;
import com.mysql.cj.exceptions.WrongArgumentException;
import com.mysql.cj.protocol.AbstractProtocol;
import com.mysql.cj.protocol.ColumnDefinition;
import com.mysql.cj.protocol.Message;
import com.mysql.cj.protocol.MessageListener;
import com.mysql.cj.protocol.MessageReader;
import com.mysql.cj.protocol.MessageSender;
import com.mysql.cj.protocol.PacketReceivedTimeHolder;
import com.mysql.cj.protocol.PacketSentTimeHolder;
import com.mysql.cj.protocol.Protocol;
import com.mysql.cj.protocol.ProtocolEntity;
import com.mysql.cj.protocol.ProtocolEntityFactory;
import com.mysql.cj.protocol.ResultListener;
import com.mysql.cj.protocol.ResultStreamer;
import com.mysql.cj.protocol.Resultset;
import com.mysql.cj.protocol.ServerCapabilities;
import com.mysql.cj.protocol.ServerSession;
import com.mysql.cj.protocol.SocketConnection;
import com.mysql.cj.protocol.a.NativeSocketConnection;
import com.mysql.cj.result.DefaultColumnDefinition;
import com.mysql.cj.result.Field;
import com.mysql.cj.result.LongValueFactory;
import com.mysql.cj.util.StringUtils;
import com.mysql.cj.x.protobuf.Mysqlx.ServerMessages;
import com.mysql.cj.x.protobuf.MysqlxConnection.Capabilities;
import com.mysql.cj.x.protobuf.MysqlxConnection.Capability;
import com.mysql.cj.x.protobuf.MysqlxResultset.ColumnMetaData;
import com.mysql.cj.x.protobuf.MysqlxResultset.Row;
import com.mysql.cj.x.protobuf.MysqlxSession.AuthenticateContinue;
import com.mysql.cj.xdevapi.FindParams;
import com.mysql.cj.xdevapi.SqlResult;

/**
 * Low-level interface to communications with X Plugin.
 */
public class XProtocol extends AbstractProtocol<XMessage> implements Protocol<XMessage> {

    private MessageReader<XMessageHeader, XMessage> reader;
    private MessageSender<XMessage> writer;
    /** We take responsibility of the socket as the managed resource. We close it when we're done. */
    private Closeable managedResource;
    private ProtocolEntityFactory<Field, XMessage> fieldFactory;
    private ProtocolEntityFactory<Notice, XMessage> noticeFactory;
    private String metadataCharacterSet;

    private ResultStreamer currentResultStreamer;

    XServerSession serverSession = null;

    public static XProtocol getInstance(String host, int port, PropertySet propertySet) {

        SocketConnection socketConnection = propertySet.getBooleanReadableProperty(PropertyDefinitions.PNAME_useAsyncProtocol).getValue()
                ? new XAsyncSocketConnection() :
                // TODO: we should share SocketConnection unless there comes a time where they need to diverge
                new NativeSocketConnection();

        socketConnection.connect(host, port, propertySet, null, null, 0);

        XProtocol protocol = new XProtocol();
        protocol.init(null, socketConnection, propertySet, null);
        return protocol;
    }

    public void init(Session sess, SocketConnection socketConn, PropertySet propSet, TransactionEventHandler transactionManager) {
        this.socketConnection = socketConn;
        this.propertySet = propSet;
        this.messageBuilder = new XMessageBuilder();

        this.authProvider = new XAuthenticationProvider();
        this.authProvider.init(this, propSet, null);

        this.metadataCharacterSet = "latin1"; // TODO configure from server session
        this.fieldFactory = new FieldFactory(this.metadataCharacterSet);
        this.noticeFactory = new NoticeFactory();
    }

    public ServerSession getServerSession() {
        return this.serverSession;
    }

    /**
     * Get the capabilities from the server.
     * <p>
     * <b>NOTE:</b> This must be called before authentication.
     * 
     * @return capabilities mapped by name
     */
    private XServerCapabilities getCapabilities() {
        try {
            this.writer.send(((XMessageBuilder) this.messageBuilder).buildCapabilitiesGet());
            return new XServerCapabilities(((Capabilities) this.reader.readMessage(null, ServerMessages.Type.CONN_CAPABILITIES_VALUE).getMessage())
                    .getCapabilitiesList().stream().collect(toMap(Capability::getName, Capability::getValue)));
        } catch (IOException e) {
            throw new XProtocolError(e.getMessage(), e);
        }
    }

    /**
     * Set a capability of current session. Must be done before authentication ({@link #changeUser(String, String, String)}).
     * 
     * @param name
     *            capability name
     * @param value
     *            capability value
     */
    public void setCapability(String name, Object value) {
        ((XServerCapabilities) getServerSession().getCapabilities()).setCapability(name, value);
        this.writer.send(((XMessageBuilder) this.messageBuilder).buildCapabilitiesSet(name, value));
        readOk();
    }

    public void negotiateSSLConnection(int packLength) {

        if (!((XServerCapabilities) this.serverSession.getCapabilities()).hasCapability("tls")) {
            throw new CJCommunicationsException("A secure connection is required but the server is not configured with SSL.");
        }

        // the message reader is async and is always "reading". we need to stop it to use the socket for the TLS handshake
        ((AsyncMessageReader) this.reader).stopAfterNextMessage();
        setCapability("tls", true);

        try {
            this.socketConnection.performTlsHandshake(null);
        } catch (SSLParamsException | FeatureNotAvailableException | IOException e) {
            throw new CJCommunicationsException(e);
        }

        // resume message processing
        ((AsyncMessageSender) this.writer).setChannel(this.socketConnection.getAsynchronousSocketChannel());
        ((AsyncMessageReader) this.reader).start();
    }

    public void beforeHandshake() {
        this.serverSession = new XServerSession();

        if (this.socketConnection.isSynchronous()) {
            this.reader = new SyncMessageReader(this.socketConnection.getMysqlInput());
            this.writer = new SyncMessageSender(this.socketConnection.getMysqlOutput());
            this.managedResource = this.socketConnection.getMysqlSocket();

            this.serverSession.setCapabilities(getCapabilities());
            return;
        }

        XAsyncSocketConnection sc = (XAsyncSocketConnection) this.socketConnection;
        this.reader = new AsyncMessageReader(this.propertySet, sc);
        ((AsyncMessageReader) this.reader).start();
        this.writer = new AsyncMessageSender(sc.getAsynchronousSocketChannel());

        this.managedResource = sc.getAsynchronousSocketChannel();

        this.serverSession.setCapabilities(getCapabilities());

        SslMode sslMode = this.propertySet.<SslMode> getEnumReadableProperty(PropertyDefinitions.PNAME_sslMode).getValue();
        boolean verifyServerCert = sslMode == SslMode.VERIFY_CA || sslMode == SslMode.VERIFY_IDENTITY;
        String trustStoreUrl = this.propertySet.getStringReadableProperty(PropertyDefinitions.PNAME_sslTrustStoreUrl).getValue();

        if (!verifyServerCert && !StringUtils.isNullOrEmpty(trustStoreUrl)) {
            StringBuilder msg = new StringBuilder("Incompatible security settings. The property '");
            msg.append(PropertyDefinitions.PNAME_sslTrustStoreUrl).append("' requires '");
            msg.append(PropertyDefinitions.PNAME_sslMode).append("' as '");
            msg.append(PropertyDefinitions.SslMode.VERIFY_CA).append("' or '");
            msg.append(PropertyDefinitions.SslMode.VERIFY_IDENTITY).append("'.");
            throw new CJCommunicationsException(msg.toString());
        }

        if (sslMode != SslMode.DISABLED) {
            negotiateSSLConnection(0);
        }
    }

    @Override
    public void connect(String user, String password, String database) {
        beforeHandshake();
        this.authProvider.connect(null, user, password, database);
    }

    public void changeUser(String user, String password, String database) {
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
        long count = getRowInputStream(metadata).next().getValue(0, new LongValueFactory());
        readQueryResult();
        setMaxAllowedPacket((int) count);
    }

    public void readOk() {
        try {
            XMessageHeader header;
            while ((header = this.reader.readHeader()).getMessageType() == ServerMessages.Type.NOTICE_VALUE) {
                this.reader.readMessage(null, header);
                // TODO OkBuilder.addNotice(this.reader.read(Frame.class));
            }
            this.reader.readMessage(null, ServerMessages.Type.OK_VALUE);
        } catch (IOException e) {
            throw new XProtocolError(e.getMessage(), e);
        }
    }

    public void readAuthenticateOk() {
        try {
            XMessageHeader header;
            while ((header = this.reader.readHeader()).getMessageType() == ServerMessages.Type.NOTICE_VALUE) {
                Notice notice = this.noticeFactory.createFromMessage(this.reader.readMessage(null, header));
                if (notice.getType() == Notice.XProtocolNoticeFrameType_SESS_STATE_CHANGED) {
                    switch (notice.getParamType()) {
                        case Notice.SessionStateChanged_CLIENT_ID_ASSIGNED:
                            this.getServerSession().setThreadId(notice.getValue().getVUnsignedInt());
                            break;
                        case Notice.SessionStateChanged_ACCOUNT_EXPIRED:
                            // TODO
                        default:
                            throw new WrongArgumentException("Unknown SessionStateChanged notice received during authentication: " + notice.getParamType());
                    }
                } else {
                    throw new WrongArgumentException("Unknown notice received during authentication: " + notice);
                }
            }
            this.reader.readMessage(null, ServerMessages.Type.SESS_AUTHENTICATE_OK_VALUE);
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

    @SuppressWarnings("unchecked")
    @Override
    public <QR extends QueryResult> QR readQueryResult() {
        try {
            StatementExecuteOkBuilder builder = new StatementExecuteOkBuilder();
            XMessageHeader header;
            if ((header = this.reader.readHeader()).getMessageType() == ServerMessages.Type.RESULTSET_FETCH_DONE_VALUE) {
                this.reader.readMessage(null, header);
            }
            while ((header = this.reader.readHeader()).getMessageType() == ServerMessages.Type.NOTICE_VALUE) {
                builder.addNotice(this.noticeFactory.createFromMessage(this.reader.readMessage(null, header)));
            }
            this.reader.readMessage(null, ServerMessages.Type.SQL_STMT_EXECUTE_OK_VALUE);
            return (QR) builder.build();
        } catch (IOException e) {
            throw new XProtocolError(e.getMessage(), e);
        }
    }

    public boolean hasResults() {
        try {
            return this.reader.readHeader().getMessageType() == ServerMessages.Type.RESULTSET_COLUMN_META_DATA_VALUE;
        } catch (IOException e) {
            throw new XProtocolError(e.getMessage(), e);
        }
    }

    public void drainRows() {
        try {
            XMessageHeader header;
            while ((header = this.reader.readHeader()).getMessageType() == ServerMessages.Type.RESULTSET_ROW_VALUE) {
                this.reader.readMessage(null, header);
            }
        } catch (IOException e) {
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
            XMessageHeader header;
            while ((header = this.reader.readHeader()).getMessageType() == ServerMessages.Type.NOTICE_VALUE) {
                // TODO put notices somewhere like it's done eg. in readStatementExecuteOk(): builder.addNotice(this.reader.read(Frame.class));
                this.reader.readMessage(null, header);
            }
            List<ColumnMetaData> fromServer = new LinkedList<>();
            do { // use this construct to read at least one
                fromServer.add((ColumnMetaData) this.reader.readMessage(null, ServerMessages.Type.RESULTSET_COLUMN_META_DATA_VALUE).getMessage());

            } while (this.reader.readHeader().getMessageType() == ServerMessages.Type.RESULTSET_COLUMN_META_DATA_VALUE);
            ArrayList<Field> metadata = new ArrayList<>(fromServer.size());
            fromServer.forEach(col -> metadata.add(this.fieldFactory.createFromMessage(new XMessage(col))));

            return new DefaultColumnDefinition(metadata.toArray(new Field[] {}));
        } catch (IOException e) {
            throw new XProtocolError(e.getMessage(), e);
        }
    }

    public XProtocolRow readRowOrNull(ColumnDefinition metadata) {
        try {
            XMessageHeader header;
            if ((header = this.reader.readHeader()).getMessageType() == ServerMessages.Type.RESULTSET_ROW_VALUE) {
                Row r = (Row) this.reader.readMessage(null, header).getMessage();
                return new XProtocolRow(metadata, r);
            }
            return null;
        } catch (IOException e) {
            throw new XProtocolError(e.getMessage(), e);
        }
    }

    public XProtocolRowInputStream getRowInputStream(ColumnDefinition metadata) {
        return new XProtocolRowInputStream(metadata, this);
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
    }

    public void setCurrentResultStreamer(ResultStreamer currentResultStreamer) {
        this.currentResultStreamer = currentResultStreamer;
    }

    public CompletableFuture<SqlResult> asyncExecuteSql(String sql, List<Object> args) {
        newCommand();
        CompletableFuture<SqlResult> f = new CompletableFuture<>();
        com.mysql.cj.protocol.MessageListener<XMessage> l = new SqlResultMessageListener(f, this.fieldFactory, this.noticeFactory,
                this.serverSession.getDefaultTimeZone());
        CompletionHandler<Long, Void> resultHandler = new ErrorToFutureCompletionHandler<>(f, () -> ((AsyncMessageReader) this.reader).pushMessageListener(l));
        ((AsyncMessageSender) this.writer).writeAsync(this.messageBuilder.buildSqlStatement(sql, args), resultHandler);
        return f;
    }

    /**
     *
     * @param findParams
     *            {@link FindParams}
     * @param callbacks
     *            {@link ResultListener}
     * @param errorFuture
     *            the {@link CompletableFuture} to complete exceptionally if the request fails
     */
    public void asyncFind(FindParams findParams, ResultListener<StatementExecuteOk> callbacks, CompletableFuture<?> errorFuture) {
        newCommand();
        MessageListener<XMessage> l = new ResultMessageListener(this.fieldFactory, this.noticeFactory, callbacks);
        CompletionHandler<Long, Void> resultHandler = new ErrorToFutureCompletionHandler<>(errorFuture,
                () -> ((AsyncMessageReader) this.reader).pushMessageListener(l));
        ((AsyncMessageSender) this.writer).writeAsync(((XMessageBuilder) this.messageBuilder).buildFind(findParams), resultHandler);
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
        this.writer.setMaxAllowedPacket(maxAllowedPacket);
    }

    @Override
    public void send(Message message, int packetLen) {
        newCommand();
        this.writer.send((XMessage) message);
    }

    @SuppressWarnings("unchecked")
    @Override
    public <RES extends QueryResult> CompletableFuture<RES> sendAsync(Message message) {
        newCommand();
        CompletableFuture<StatementExecuteOk> f = new CompletableFuture<>();
        final StatementExecuteOkMessageListener l = new StatementExecuteOkMessageListener(f, this.noticeFactory);
        CompletionHandler<Long, Void> resultHandler = new ErrorToFutureCompletionHandler<>(f, () -> ((AsyncMessageReader) this.reader).pushMessageListener(l));
        ((AsyncMessageSender) this.writer).writeAsync((XMessage) message, resultHandler);
        return (CompletableFuture<RES>) f;
    }

    public ServerCapabilities readServerCapabilities() {
        throw ExceptionFactory.createException(CJOperationNotSupportedException.class, "Not supported");
    }

    @Override
    public ExceptionInterceptor getExceptionInterceptor() {
        throw ExceptionFactory.createException(CJOperationNotSupportedException.class, "Not supported");
    }

    @Override
    public PacketSentTimeHolder getPacketSentTimeHolder() {
        throw ExceptionFactory.createException(CJOperationNotSupportedException.class, "Not supported");
    }

    @Override
    public void setPacketSentTimeHolder(PacketSentTimeHolder packetSentTimeHolder) {
        throw ExceptionFactory.createException(CJOperationNotSupportedException.class, "Not supported");
    }

    @Override
    public PacketReceivedTimeHolder getPacketReceivedTimeHolder() {
        throw ExceptionFactory.createException(CJOperationNotSupportedException.class, "Not supported");
    }

    @Override
    public void setPacketReceivedTimeHolder(PacketReceivedTimeHolder packetReceivedTimeHolder) {
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
