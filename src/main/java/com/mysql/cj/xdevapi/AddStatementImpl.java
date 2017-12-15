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

package com.mysql.cj.xdevapi;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import com.mysql.cj.api.xdevapi.AddResult;
import com.mysql.cj.api.xdevapi.AddStatement;
import com.mysql.cj.api.xdevapi.JsonValue;
import com.mysql.cj.core.exceptions.AssertionFailedException;
import com.mysql.cj.x.core.MysqlxSession;
import com.mysql.cj.x.core.StatementExecuteOk;

public class AddStatementImpl implements AddStatement {
    private MysqlxSession mysqlxSession;
    private String schemaName;
    private String collectionName;
    private List<DbDoc> newDocs;
    private boolean upsert = false;

    /* package private */ AddStatementImpl(MysqlxSession mysqlxSession, String schema, String collection, DbDoc newDoc) {
        this.mysqlxSession = mysqlxSession;
        this.schemaName = schema;
        this.collectionName = collection;
        this.newDocs = new ArrayList<>();
        this.newDocs.add(newDoc);
    }

    /* package private */ AddStatementImpl(MysqlxSession mysqlxSession, String schema, String collection, DbDoc[] newDocs) {
        this.mysqlxSession = mysqlxSession;
        this.schemaName = schema;
        this.collectionName = collection;
        this.newDocs = new ArrayList<>();
        this.newDocs.addAll(Arrays.asList(newDocs));
    }

    public AddStatement add(String jsonString) {
        try {
            DbDoc doc = JsonParser.parseDoc(new StringReader(jsonString));
            return add(doc);
        } catch (IOException ex) {
            throw AssertionFailedException.shouldNotHappen(ex);
        }
    }

    public AddStatement add(DbDoc... docs) {
        this.newDocs.addAll(Arrays.asList(docs));
        return this;
    }

    private List<String> assignIds() {
        return this.newDocs.stream().map(d -> {
            JsonValue id = d.get("_id");
            if (id != null) {
                return id instanceof JsonString ? ((JsonString) id).getString() : id.toString();
            }
            String newId = DocumentID.generate();
            d.put("_id", new JsonString().setValue(newId));
            return newId;
        }).collect(Collectors.toList());
    }

    private List<String> serializeDocs() {
        return this.newDocs.stream().map(DbDoc::toString).collect(Collectors.toList());
    }

    public AddResult execute() {
        if (this.newDocs.size() == 0) { // according to X DevAPI specification, this is a no-op. we create an empty Result
            StatementExecuteOk ok = new StatementExecuteOk(0, null, new ArrayList<>());
            return new AddResultImpl(ok, new ArrayList<>());
        }
        List<String> newIds = assignIds();
        StatementExecuteOk ok = this.mysqlxSession.addDocs(this.schemaName, this.collectionName, serializeDocs(), this.upsert);
        return new AddResultImpl(ok, newIds);
    }

    public CompletableFuture<AddResult> executeAsync() {
        final List<String> newIds = assignIds();
        CompletableFuture<StatementExecuteOk> okF = this.mysqlxSession.asyncAddDocs(this.schemaName, this.collectionName, serializeDocs(), this.upsert);
        return okF.thenApply(ok -> new AddResultImpl(ok, newIds));
    }

    public boolean isUpsert() {
        return this.upsert;
    }

    public AddStatement setUpsert(boolean upsert) {
        this.upsert = upsert;
        return this;
    }
}
