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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Spliterators;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import com.mysql.cj.api.Session;
import com.mysql.cj.api.conf.PropertySet;
import com.mysql.cj.api.exceptions.ExceptionInterceptor;
import com.mysql.cj.api.io.Protocol;
import com.mysql.cj.api.result.RowInputStream;
import com.mysql.cj.api.result.RowList;
import com.mysql.cj.core.ServerVersion;
import com.mysql.cj.core.io.LongValueFactory;
import com.mysql.cj.core.io.StatementExecuteOk;
import com.mysql.cj.core.io.StringValueFactory;
import com.mysql.cj.core.exceptions.CJCommunicationsException;
import com.mysql.cj.core.result.BufferedRowList;
import com.mysql.cj.jdbc.Field;
import com.mysql.cj.mysqlx.FilterParams;
import com.mysql.cj.mysqlx.io.MysqlxProtocol;
import com.mysql.cj.mysqlx.devapi.DbDocsImpl;

public class MysqlxSession implements Session {
    MysqlxProtocol protocol;

    public MysqlxSession(MysqlxProtocol protocol) {
        this.protocol = protocol;
    }

    public PropertySet getPropertySet() {
        throw new NullPointerException("TODO: justify use of this method");
    }

    public Protocol getProtocol() {
        throw new NullPointerException("TODO: You are not allowed to have my protocol");
    }

    public void changeUser(String user, String password, String database) {
        // TODO: use MYSQL41 auth by default?
        this.protocol.sendSaslMysql41AuthStart();
        byte[] salt = protocol.readAuthenticateContinue();
        this.protocol.sendSaslMysql41AuthContinue(user, password, salt, database);

        protocol.readAuthenticateOk();

        // TODO: remove this when server bug is fixed
        protocol.sendSqlStatement("use " + database);
        protocol.readStatementExecuteOk();
    }

    public ExceptionInterceptor getExceptionInterceptor() {
        throw new NullPointerException("TODO: You are not allowed to have this");
    }

    public void setExceptionInterceptor(ExceptionInterceptor exceptionInterceptor) {
        throw new NullPointerException("TODO: I don't need your stinkin exception interceptor");
    }

    public boolean characterSetNamesMatches(String mysqlEncodingName) {
        throw new NullPointerException("TODO: don't implement this method here");
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

    public void setServerVariables(Map<String, String> serverVariables) {
        throw new NullPointerException("TODO: ");
    }

    public int getServerCharsetIndex() {
        throw new NullPointerException("TODO: ");
    }

    public void setServerCharsetIndex(int serverCharsetIndex) {
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
        throw new NullPointerException("TODO: ");
    }

    public boolean isSetNeededForAutoCommitMode(boolean autoCommitFlag) {
        throw new NullPointerException("TODO: ");
    }

    public StatementExecuteOk addDocs(String schemaName, String collectionName, List<String> jsonStrings) {
        this.protocol.sendDocInsert(schemaName, collectionName, jsonStrings);
        return this.protocol.readStatementExecuteOk();
    }

    public DbDocsImpl findDocs(String schemaName, String collectionName, FilterParams filterParams) {
        this.protocol.sendDocFind(schemaName, collectionName, filterParams);
        // TODO: put characterSetMetadata somewhere useful
        ArrayList<Field> metadata = this.protocol.readMetadata("latin1");
        RowInputStream rowInputStream = this.protocol.getRowInputStream(metadata);
        // TODO: allow to choose this buffering vs streaming, etc, need a FURTHER extension on these
        // TODO: also need a "smart buffering" mode to handle this nicely
        RowList rows = new BufferedRowList(rowInputStream);
        return new DbDocsImpl(this, rows);
    }

    public void createCollection(String schemaName, String collectionName) {
        this.protocol.sendCreateCollection(schemaName, collectionName);
        this.protocol.readStatementExecuteOk();
    }

    public void dropCollection(String schemaName, String collectionName) {
        this.protocol.sendDropCollection(schemaName, collectionName);
        this.protocol.readStatementExecuteOk();
    }

    private long queryForLong(String sql) {
        this.protocol.sendSqlStatement(sql);
        // TODO: can use a simple default for this as we don't need metadata. need to prevent against exceptions though
        ArrayList<Field> metadata = this.protocol.readMetadata("latin1");
        long count = this.protocol.getRowInputStream(metadata).readRow().getValue(0, new LongValueFactory());
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
     * @param schemaName schema to return object names from
     * @param type type of objects to return
     * @return object names
     */
    public List<String> getObjectNamesOfType(String schemaName, String type) {
        this.protocol.sendListObjects(schemaName);
        // TODO: charactersetMetadata
        ArrayList<Field> metadata = this.protocol.readMetadata("latin1");
        RowInputStream ris = this.protocol.getRowInputStream(metadata);
        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(ris, 0), false)
                .filter(r -> r.getValue(1, new StringValueFactory()).equals(type))
                .map(r -> r.getValue(0, new StringValueFactory()))
                .collect(Collectors.toList());
    }

    public void close() {
        try {
            this.protocol.close();
        } catch (IOException ex) {
            throw new CJCommunicationsException(ex);
        }
    }
}
