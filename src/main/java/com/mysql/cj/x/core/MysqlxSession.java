/*
 * Copyright (c) 2015, 2017, Oracle and/or its affiliates. All rights reserved.
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

package com.mysql.cj.x.core;

import java.io.IOException;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.Spliterators;
import java.util.TimeZone;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import com.mysql.cj.api.ProfilerEventHandler;
import com.mysql.cj.api.Session;
import com.mysql.cj.api.conf.PropertySet;
import com.mysql.cj.api.exceptions.ExceptionInterceptor;
import com.mysql.cj.api.io.Protocol;
import com.mysql.cj.api.io.ServerSession;
import com.mysql.cj.api.io.ValueFactory;
import com.mysql.cj.api.log.Log;
import com.mysql.cj.api.result.Row;
import com.mysql.cj.api.x.core.ResultCtor;
import com.mysql.cj.api.x.io.MetadataToRowToElement;
import com.mysql.cj.api.x.io.ResultListener;
import com.mysql.cj.api.x.io.ResultStreamer;
import com.mysql.cj.api.xdevapi.DataStatement.Reducer;
import com.mysql.cj.api.xdevapi.DatabaseObject;
import com.mysql.cj.api.xdevapi.DocResult;
import com.mysql.cj.api.xdevapi.RowResult;
import com.mysql.cj.api.xdevapi.SqlResult;
import com.mysql.cj.core.ServerVersion;
import com.mysql.cj.core.conf.DefaultPropertySet;
import com.mysql.cj.core.conf.PropertyDefinitions;
import com.mysql.cj.core.exceptions.CJCommunicationsException;
import com.mysql.cj.core.exceptions.WrongArgumentException;
import com.mysql.cj.core.io.LongValueFactory;
import com.mysql.cj.core.io.StringValueFactory;
import com.mysql.cj.core.result.Field;
import com.mysql.cj.core.util.StringUtils;
import com.mysql.cj.x.io.DevapiRowFactory;
import com.mysql.cj.x.io.ResultCreatingResultListener;
import com.mysql.cj.x.io.RowWiseReducingResultListener;
import com.mysql.cj.x.io.StatementExecuteOkBuilder;
import com.mysql.cj.x.io.XProtocol;
import com.mysql.cj.x.io.XProtocolFactory;
import com.mysql.cj.xdevapi.CreateIndexParams;
import com.mysql.cj.xdevapi.DbDoc;
import com.mysql.cj.xdevapi.DbDocValueFactory;
import com.mysql.cj.xdevapi.DocResultImpl;
import com.mysql.cj.xdevapi.ExprUnparser;
import com.mysql.cj.xdevapi.FilterParams;
import com.mysql.cj.xdevapi.FindParams;
import com.mysql.cj.xdevapi.InsertParams;
import com.mysql.cj.xdevapi.RowResultImpl;
import com.mysql.cj.xdevapi.SqlDataResult;
import com.mysql.cj.xdevapi.SqlResultImpl;
import com.mysql.cj.xdevapi.SqlUpdateResult;
import com.mysql.cj.xdevapi.UpdateParams;
import com.mysql.cj.xdevapi.UpdateSpec;

public class MysqlxSession implements Session {

    private XProtocol protocol;
    private ResultStreamer currentResult;
    private String host;
    private int port;
    // TODO Need to expand options here. Force user to specify, given possible heterogenous configuration in sharded env?
    private TimeZone defaultTimeZone = TimeZone.getDefault();
    ValueFactory<String> svf = new StringValueFactory();

    protected String authMech = "MYSQL41"; // used in test case to check what type of the authentications was actually used

    public MysqlxSession(Properties properties) {

        PropertySet pset = new DefaultPropertySet();
        pset.initializeProperties(properties);

        // create protocol instance
        this.host = properties.getProperty(PropertyDefinitions.HOST_PROPERTY_KEY);
        if (this.host == null || StringUtils.isEmptyOrWhitespaceOnly(this.host)) {
            this.host = "localhost";
        }
        this.port = Integer.parseInt(properties.getProperty(PropertyDefinitions.PORT_PROPERTY_KEY, "33060"));

        this.protocol = XProtocolFactory.getInstance(this.host, this.port, pset);
    }

    public String getHost() {
        return this.host;
    }

    public int getPort() {
        return this.port;
    }

    public PropertySet getPropertySet() {
        return this.protocol.getPropertySet();
    }

    public Protocol getProtocol() {
        throw new NullPointerException("TODO: You are not allowed to have my protocol");
    }

    /**
     * Change user as given by parameters. This implementation only supports calling this during the initial handshake.
     */
    public void changeUser(String user, String password, String database) {
        this.authMech = this.protocol.getPropertySet().getStringReadableProperty(PropertyDefinitions.PNAME_auth).getValue();
        boolean overTLS = this.protocol.getTls();

        // default choice
        if (this.authMech == null) {
            this.authMech = overTLS ? "PLAIN" : "MYSQL41";
            // TODO see WL#10992 this.authMech = overTLS ? "PLAIN" : (this.protocol.getAuthenticationMechanisms().contains("SHA256_MEMORY") ? "SHA256_MEMORY" : "MYSQL41");
        } else {
            this.authMech = this.authMech.toUpperCase();
        }

        switch (this.authMech) {
            case "MYSQL41":
                this.protocol.sendSaslMysql41AuthStart();
                byte[] salt = this.protocol.readAuthenticateContinue();
                this.protocol.sendSaslMysql41AuthContinue(user, password, salt, database);
                break;
            case "PLAIN":
                if (overTLS) {
                    this.protocol.sendSaslPlainAuthStart(user, password, database);
                } else {
                    throw new XDevAPIError("PLAIN authentication is not allowed via unencrypted connection.");
                }
                break;
            case "EXTERNAL":
                this.protocol.sendSaslExternalAuthStart(database);
                break;
            // TODO see WL#10992
            //            case "SHA256_MEMORY":
            //                this.protocol.sendSaslSha256MemoryAuthStart();
            //                salt = this.protocol.readAuthenticateContinue();
            //                this.protocol.sendSaslSha256MemoryAuthContinue(user, password, salt, database);
            //                break;

            default:
                throw new WrongArgumentException("Unknown authentication mechanism '" + this.authMech + "'.");
        }

        this.protocol.readAuthenticateOk();
        setupInternalState();
    }

    /**
     * Setup internal state of the session.
     */
    private void setupInternalState() {
        this.protocol.setMaxAllowedPacket((int) queryForLong("select @@mysqlx_max_allowed_packet"));
    }

    public ExceptionInterceptor getExceptionInterceptor() {
        throw new NullPointerException("TODO: You are not allowed to have this");
    }

    public void setExceptionInterceptor(ExceptionInterceptor exceptionInterceptor) {
        throw new NullPointerException("TODO: I don't need your stinkin exception interceptor");
    }

    public boolean inTransactionOnServer() {
        throw new NullPointerException("TODO: who wants to know? Also, check NEW tx state in OK packet extensions");
    }

    public String getServerVariable(String name) {
        throw new NullPointerException("TODO: ");
    }

    public Map<String, String> getServerVariables() {
        throw new NullPointerException("TODO: ");
    }

    public void abortInternal() {
        throw new NullPointerException("TODO: REPLACE ME WITH close() unless there's different semantics here");
    }

    public void quit() {
        throw new NullPointerException("TODO: REPLACE ME WITH close() unless there's different semantics here");
    }

    public void forceClose() {
        throw new NullPointerException("TODO: REPLACE ME WITH close() unless there's different semantics here");
    }

    public ServerVersion getServerVersion() {
        throw new NullPointerException("TODO: isn't this in server session?");
    }

    public boolean versionMeetsMinimum(int major, int minor, int subminor) {
        throw new NullPointerException("TODO: ");
    }

    public long getThreadId() {
        return this.protocol.getClientId();
    }

    public boolean isSetNeededForAutoCommitMode(boolean autoCommitFlag) {
        throw new NullPointerException("TODO: ");
    }

    /**
     * Signal the intent to start processing a new command. A session supports processing a single command at a time. Results are reading lazily from the
     * wire. It is necessary to flush any pending result before starting a new command. This method performs the flush if necessary.
     */
    private void newCommand() {
        if (this.currentResult != null) {
            try {
                this.currentResult.finishStreaming();
            } finally {
                // so we don't call finishStreaming() again if there's an exception
                this.currentResult = null;
            }
        }
    }

    public StatementExecuteOk addDocs(String schemaName, String collectionName, List<String> jsonStrings, boolean upsert) {
        newCommand();
        this.protocol.sendDocInsert(schemaName, collectionName, jsonStrings, upsert);
        return this.protocol.readStatementExecuteOk();
    }

    public StatementExecuteOk insertRows(String schemaName, String tableName, InsertParams insertParams) {
        newCommand();
        this.protocol.sendRowInsert(schemaName, tableName, insertParams);
        return this.protocol.readStatementExecuteOk();
    }

    public StatementExecuteOk updateDocs(FilterParams filterParams, List<UpdateSpec> updates) {
        newCommand();
        this.protocol.sendDocUpdates(filterParams, updates);
        return this.protocol.readStatementExecuteOk();
    }

    public StatementExecuteOk updateRows(FilterParams filterParams, UpdateParams updateParams) {
        newCommand();
        this.protocol.sendRowUpdates(filterParams, updateParams);
        return this.protocol.readStatementExecuteOk();
    }

    public StatementExecuteOk deleteDocs(FilterParams filterParams) {
        newCommand();
        this.protocol.sendDocDelete(filterParams);
        return this.protocol.readStatementExecuteOk();
    }

    public StatementExecuteOk deleteRows(FilterParams filterParams) {
        newCommand();
        // TODO: this works because xplugin doesn't check dataModel on delete. it doesn't need to... protocol change?
        this.protocol.sendDocDelete(filterParams);
        return this.protocol.readStatementExecuteOk();
    }

    private <T extends ResultStreamer> T findInternal(FindParams findParams, ResultCtor<T> resultCtor) {
        newCommand();
        this.protocol.sendFind(findParams);
        // TODO: put characterSetMetadata somewhere useful
        ArrayList<Field> metadata = this.protocol.readMetadata("latin1");
        T res = resultCtor.apply(metadata).apply(this.protocol.getRowInputStream(metadata), this.protocol::readStatementExecuteOk);
        this.currentResult = res;
        return res;
    }

    public DocResultImpl findDocs(FindParams findParams) {
        return findInternal(findParams, metadata -> (rows, task) -> new DocResultImpl(rows, task));
    }

    public RowResultImpl selectRows(FindParams findParams) {
        return findInternal(findParams, metadata -> (rows, task) -> new RowResultImpl(metadata, this.defaultTimeZone, rows, task));
    }

    public void createCollection(String schemaName, String collectionName) {
        newCommand();
        this.protocol.sendCreateCollection(schemaName, collectionName);
        this.protocol.readStatementExecuteOk();
    }

    public void dropCollection(String schemaName, String collectionName) {
        newCommand();
        this.protocol.sendDropCollection(schemaName, collectionName);
        this.protocol.readStatementExecuteOk();
    }

    public void dropCollectionIfExists(String schemaName, String collectionName) {
        if (tableExists(schemaName, collectionName)) {
            dropCollection(schemaName, collectionName);
        }
    }

    public StatementExecuteOk createCollectionIndex(String schemaName, String collectionName, CreateIndexParams params) {
        newCommand();
        this.protocol.sendCreateCollectionIndex(schemaName, collectionName, params);
        return this.protocol.readStatementExecuteOk();
    }

    public StatementExecuteOk dropCollectionIndex(String schemaName, String collectionName, String indexName) {
        newCommand();
        this.protocol.sendDropCollectionIndex(schemaName, collectionName, indexName);
        return this.protocol.readStatementExecuteOk();
    }

    private long queryForLong(String sql) {
        newCommand();
        this.protocol.sendSqlStatement(sql);
        // TODO: can use a simple default for this as we don't need metadata. need to prevent against exceptions though
        ArrayList<Field> metadata = this.protocol.readMetadata("latin1");
        long count = this.protocol.getRowInputStream(metadata).next().getValue(0, new LongValueFactory());
        this.protocol.readStatementExecuteOk();
        return count;
    }

    public long tableCount(String schemaName, String tableName) {
        StringBuilder stmt = new StringBuilder("select count(*) from ");
        stmt.append(ExprUnparser.quoteIdentifier(schemaName));
        stmt.append(".");
        stmt.append(ExprUnparser.quoteIdentifier(tableName));
        return queryForLong(stmt.toString());
    }

    public boolean schemaExists(String schemaName) {
        StringBuilder stmt = new StringBuilder("select count(*) from information_schema.schemata where schema_name = '");
        // TODO: verify quoting rules
        stmt.append(schemaName.replaceAll("'", "\\'"));
        stmt.append("'");
        return 1 == queryForLong(stmt.toString());
    }

    public boolean tableExists(String schemaName, String tableName) {
        StringBuilder stmt = new StringBuilder("select count(*) from information_schema.tables where table_schema = '");
        // TODO: verify quoting rules
        stmt.append(schemaName.replaceAll("'", "\\'"));
        stmt.append("' and table_name = '");
        stmt.append(tableName.replaceAll("'", "\\'"));
        stmt.append("'");
        return 1 == queryForLong(stmt.toString());
    }

    /**
     * Retrieve the list of objects in the given schema of the specified type. The type may be one of {COLLECTION, TABLE, VIEW}.
     *
     * @param schemaName
     *            schema to return object names from
     * @param type
     *            type of objects to return
     * @return object names
     */
    public List<String> getObjectNamesOfType(String schemaName, DatabaseObject.DbObjectType... type) {
        return getObjectNamesOfType(schemaName, null, type);
    }

    /**
     * Retrieve the list of objects in the given schema of the specified type. The type may be one of {COLLECTION, TABLE, VIEW}.
     *
     * @param schemaName
     *            schema to return object names from
     * @param type
     *            type of objects to return
     * @param pattern
     *            object name pattern
     * @return object names
     */
    public List<String> getObjectNamesOfType(String schemaName, String pattern, DatabaseObject.DbObjectType... type) {
        newCommand();
        if (pattern == null) {
            this.protocol.sendListObjects(schemaName);
        } else {
            this.protocol.sendListObjects(schemaName, pattern);
        }
        // TODO: charactersetMetadata
        ArrayList<Field> metadata = this.protocol.readMetadata("latin1");
        Iterator<Row> ris = this.protocol.getRowInputStream(metadata);

        Set<String> strTypes = Arrays.stream(type).map(DatabaseObject.DbObjectType::toString).collect(Collectors.toSet());
        List<String> objectNames = StreamSupport.stream(Spliterators.spliteratorUnknownSize(ris, 0), false)
                .filter(r -> (strTypes).contains(r.getValue(1, this.svf))).map(r -> r.getValue(0, this.svf)).collect(Collectors.toList());
        this.protocol.readStatementExecuteOk();
        return objectNames;
    }

    public List<DatabaseObjectDescription> listObjects(String schemaName, String pattern) {
        newCommand();
        if (pattern == null) {
            this.protocol.sendListObjects(schemaName);
        } else {
            this.protocol.sendListObjects(schemaName, pattern);
        }
        // TODO: charactersetMetadata
        ArrayList<Field> metadata = this.protocol.readMetadata("latin1");
        Iterator<Row> ris = this.protocol.getRowInputStream(metadata);
        List<DatabaseObjectDescription> objects = StreamSupport.stream(Spliterators.spliteratorUnknownSize(ris, 0), false)
                .map(r -> new DatabaseObjectDescription(r.getValue(0, this.svf), r.getValue(1, this.svf))).collect(Collectors.toList());
        this.protocol.readStatementExecuteOk();
        return objects;
    }

    public <RES_T, R> RES_T query(String sql, Function<Row, R> eachRow, Collector<R, ?, RES_T> collector) {
        newCommand();
        this.protocol.sendSqlStatement(sql);
        // TODO: characterSetMetadata
        ArrayList<Field> metadata = this.protocol.readMetadata("latin1");
        Iterator<Row> ris = this.protocol.getRowInputStream(metadata);
        RES_T result = StreamSupport.stream(Spliterators.spliteratorUnknownSize(ris, 0), false).map(eachRow).collect(collector);
        this.protocol.readStatementExecuteOk();
        return result;
    }

    public SqlResult executeSql(String sql, Object args) {
        newCommand();
        this.protocol.sendSqlStatement(sql, args);
        boolean readLastResult[] = new boolean[1];
        Supplier<StatementExecuteOk> okReader = () -> {
            if (readLastResult[0]) {
                throw new CJCommunicationsException("Invalid state attempting to read ok packet");
            }
            if (this.protocol.hasMoreResults()) {
                // empty/fabricated OK packet
                return new StatementExecuteOkBuilder().build();
            }
            readLastResult[0] = true;
            return this.protocol.readStatementExecuteOk();
        };
        Supplier<SqlResult> resultStream = () -> {
            if (readLastResult[0]) {
                return null;
            } else if (this.protocol.isSqlResultPending()) {
                // TODO: put characterSetMetadata somewhere useful
                ArrayList<Field> metadata = this.protocol.readMetadata("latin1");
                return new SqlDataResult(metadata, this.defaultTimeZone, this.protocol.getRowInputStream(metadata), okReader);
            } else {
                readLastResult[0] = true;
                return new SqlUpdateResult(this.protocol.readStatementExecuteOk());
            }
        };
        SqlResultImpl res = new SqlResultImpl(resultStream);
        this.currentResult = res;
        return res;
    }

    public CompletableFuture<SqlResult> asyncExecuteSql(String sql, Object args) {
        newCommand();
        // TODO: put characterSetMetadata somewhere useful
        return this.protocol.asyncExecuteSql(sql, args, "latin1", this.defaultTimeZone);
    }

    public StatementExecuteOk update(String sql) {
        newCommand();
        this.protocol.sendSqlStatement(sql);
        return this.protocol.readStatementExecuteOk();
    }

    public boolean isOpen() {
        return this.protocol.isOpen();
    }

    public void close() {
        try {
            newCommand();
            this.protocol.sendSessionClose();
            this.protocol.readOk();
        } finally {
            try {
                this.protocol.close();
            } catch (IOException ex) {
                throw new CJCommunicationsException(ex);
            }
        }
    }

    private <RES_T> CompletableFuture<RES_T> asyncFindInternal(FindParams findParams, ResultCtor<? extends RES_T> resultCtor) {
        CompletableFuture<RES_T> f = new CompletableFuture<>();
        ResultListener l = new ResultCreatingResultListener<>(resultCtor, f);
        newCommand();
        // TODO: put characterSetMetadata somewhere useful
        this.protocol.asyncFind(findParams, "latin1", l, f);
        return f;
    }

    public CompletableFuture<DocResult> asyncFindDocs(FindParams findParams) {
        return asyncFindInternal(findParams, metadata -> (rows, task) -> new DocResultImpl(rows, task));
    }

    public CompletableFuture<RowResult> asyncSelectRows(FindParams findParams) {
        return asyncFindInternal(findParams, metadata -> (rows, task) -> new RowResultImpl(metadata, this.defaultTimeZone, rows, task));
    }

    public <R> CompletableFuture<R> asyncFindDocsReduce(FindParams findParams, R id, Reducer<DbDoc, R> reducer) {
        CompletableFuture<R> f = new CompletableFuture<>();
        ResultListener l = new RowWiseReducingResultListener<>(id, reducer, f,
                (ArrayList<Field> _ignored_metadata) -> r -> r.getValue(0, new DbDocValueFactory()));
        newCommand();
        // TODO: put characterSetMetadata somewhere useful
        this.protocol.asyncFind(findParams, "latin1", l, f);
        return f;
    }

    public <R> CompletableFuture<R> asyncSelectRowsReduce(FindParams findParams, R id, Reducer<com.mysql.cj.api.xdevapi.Row, R> reducer) {
        CompletableFuture<R> f = new CompletableFuture<>();
        MetadataToRowToElement<com.mysql.cj.api.xdevapi.Row> rowFactory = metadata -> new DevapiRowFactory(metadata, this.defaultTimeZone);
        ResultListener l = new RowWiseReducingResultListener<>(id, reducer, f, rowFactory);
        newCommand();
        // TODO: put characterSetMetadata somewhere useful
        this.protocol.asyncFind(findParams, "latin1", l, f);
        return f;
    }

    public CompletableFuture<StatementExecuteOk> asyncAddDocs(String schemaName, String collectionName, List<String> jsonStrings, boolean upsert) {
        newCommand();
        return this.protocol.asyncAddDocs(schemaName, collectionName, jsonStrings, upsert);
    }

    public CompletableFuture<StatementExecuteOk> asyncInsertRows(String schemaName, String tableName, InsertParams insertParams) {
        newCommand();
        return this.protocol.asyncInsertRows(schemaName, tableName, insertParams);
    }

    public CompletableFuture<StatementExecuteOk> asyncUpdateDocs(FilterParams filterParams, List<UpdateSpec> updates) {
        newCommand();
        return this.protocol.asyncUpdateDocs(filterParams, updates);
    }

    public CompletableFuture<StatementExecuteOk> asyncUpdateRows(FilterParams filterParams, UpdateParams updateParams) {
        newCommand();
        return this.protocol.asyncUpdateRows(filterParams, updateParams);
    }

    public CompletableFuture<StatementExecuteOk> asyncDeleteDocs(FilterParams filterParams) {
        newCommand();
        return this.protocol.asyncDeleteDocs(filterParams);
    }

    public CompletableFuture<StatementExecuteOk> asyncDeleteRows(FilterParams filterParams) {
        newCommand();
        return this.protocol.asyncDeleteDocs(filterParams);
    }

    public CompletableFuture<StatementExecuteOk> asyncCreateCollectionIndex(String schemaName, String collectionName, CreateIndexParams params) {
        newCommand();
        return this.protocol.asyncCreateCollectionIndex(schemaName, collectionName, params);
    }

    public CompletableFuture<StatementExecuteOk> asyncDropCollectionIndex(String schemaName, String collectionName, String indexName) {
        newCommand();
        return this.protocol.asyncDropCollectionIndex(schemaName, collectionName, indexName);
    }

    @Override
    public int getServerVariable(String variableName, int fallbackValue) {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public Log getLog() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void configureTimezone() {
        // TODO Auto-generated method stub

    }

    @Override
    public TimeZone getDefaultTimeZone() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public String getErrorMessageEncoding() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public int getMaxBytesPerChar(String javaCharsetName) {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public int getMaxBytesPerChar(Integer charsetIndex, String javaCharsetName) {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public String getEncodingForIndex(int collationIndex) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public ProfilerEventHandler getProfilerEventHandler() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void setProfilerEventHandler(ProfilerEventHandler h) {
        // TODO Auto-generated method stub

    }

    public ServerSession getServerSession() {
        // TODO;
        return null;
    }

    @Override
    public boolean isSSLEstablished() {
        // TODO;
        return false;
    }

    @Override
    public SocketAddress getRemoteSocketAddress() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public boolean serverSupportsFracSecs() {
        return true;
    }

    @Override
    public String getProcessHost() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void addListener(SessionEventListener l) {
        // TODO Auto-generated method stub

    }

    @Override
    public void removeListener(SessionEventListener l) {
        // TODO Auto-generated method stub

    }
}
