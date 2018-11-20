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

package com.mysql.cj.xdevapi;

import java.io.IOException;
import java.io.StringReader;
import java.util.Map;

import com.mysql.cj.Messages;
import com.mysql.cj.MysqlxSession;
import com.mysql.cj.exceptions.AssertionFailedException;
import com.mysql.cj.exceptions.FeatureNotAvailableException;
import com.mysql.cj.exceptions.MysqlErrorNumbers;
import com.mysql.cj.protocol.x.StatementExecuteOk;
import com.mysql.cj.protocol.x.XMessage;
import com.mysql.cj.protocol.x.XMessageBuilder;
import com.mysql.cj.protocol.x.XProtocolError;

public class CollectionImpl implements Collection {
    private MysqlxSession mysqlxSession;
    private XMessageBuilder xbuilder;
    private SchemaImpl schema;
    private String name;

    /* package private */ CollectionImpl(MysqlxSession mysqlxSession, SchemaImpl schema, String name) {
        this.mysqlxSession = mysqlxSession;
        this.schema = schema;
        this.name = name;
        this.xbuilder = (XMessageBuilder) this.mysqlxSession.<XMessage> getMessageBuilder();
    }

    public Session getSession() {
        return this.schema.getSession();
    }

    public Schema getSchema() {
        return this.schema;
    }

    public String getName() {
        return this.name;
    }

    public DbObjectStatus existsInDatabase() {
        if (this.mysqlxSession.getDataStoreMetadata().tableExists(this.schema.getName(), this.name)) {
            // TODO should also check that the table has a DbObjectType.COLLECTION type  
            return DbObjectStatus.EXISTS;
        }
        return DbObjectStatus.NOT_EXISTS;
    }

    public AddStatement add(Map<String, ?> doc) {
        throw new FeatureNotAvailableException("TODO: ");
    }

    @Override
    public AddStatement add(String... jsonString) {
        try {
            DbDoc[] docs = new DbDoc[jsonString.length];
            for (int i = 0; i < jsonString.length; i++) {
                docs[i] = JsonParser.parseDoc(new StringReader(jsonString[i]));
            }
            return add(docs);
        } catch (IOException ex) {
            throw AssertionFailedException.shouldNotHappen(ex);
        }
    }

    @Override
    public AddStatement add(DbDoc doc) {
        return new AddStatementImpl(this.mysqlxSession, this.schema.getName(), this.name, doc);
    }

    public AddStatement add(DbDoc... docs) {
        return new AddStatementImpl(this.mysqlxSession, this.schema.getName(), this.name, docs);
    }

    public FindStatement find() {
        return find(null);
    }

    public FindStatement find(String searchCondition) {
        return new FindStatementImpl(this.mysqlxSession, this.schema.getName(), this.name, searchCondition);
    }

    public ModifyStatement modify(String searchCondition) {
        return new ModifyStatementImpl(this.mysqlxSession, this.schema.getName(), this.name, searchCondition);
    }

    public RemoveStatement remove(String searchCondition) {
        return new RemoveStatementImpl(this.mysqlxSession, this.schema.getName(), this.name, searchCondition);
    }

    @Override
    public Result createIndex(String indexName, DbDoc indexDefinition) {
        StatementExecuteOk ok = this.mysqlxSession
                .sendMessage(this.xbuilder.buildCreateCollectionIndex(this.schema.getName(), this.name, new CreateIndexParams(indexName, indexDefinition)));
        return new UpdateResult(ok);
    }

    @Override
    public Result createIndex(String indexName, String jsonIndexDefinition) {
        StatementExecuteOk ok = this.mysqlxSession
                .sendMessage(this.xbuilder.buildCreateCollectionIndex(this.schema.getName(), this.name, new CreateIndexParams(indexName, jsonIndexDefinition)));
        return new UpdateResult(ok);
    }

    public void dropIndex(String indexName) {
        try {
            this.mysqlxSession.sendMessage(this.xbuilder.buildDropCollectionIndex(this.schema.getName(), this.name, indexName));
        } catch (XProtocolError e) {
            // If specified object does not exist, dropX() methods succeed (no error is reported)
            // TODO check MySQL > 8.0.1 for built in solution, like passing ifExists to dropView
            if (e.getErrorCode() != MysqlErrorNumbers.ER_CANT_DROP_FIELD_OR_KEY) {
                throw e;
            }
        }
    }

    public long count() {
        try {
            return this.mysqlxSession.getDataStoreMetadata().getTableRowCount(this.schema.getName(), this.name);
        } catch (XProtocolError e) {
            if (e.getErrorCode() == 1146) {
                throw new XProtocolError("Collection '" + this.name + "' does not exist in schema '" + this.schema.getName() + "'", e);
            }
            throw e;
        }
    }

    public DbDoc newDoc() {
        return new DbDocImpl();
    }

    @Override
    public boolean equals(Object other) {
        return other != null && other.getClass() == CollectionImpl.class && ((CollectionImpl) other).schema.equals(this.schema)
                && ((CollectionImpl) other).mysqlxSession == this.mysqlxSession && this.name.equals(((CollectionImpl) other).name);
    }

    @Override
    public int hashCode() {
        assert false : "hashCode not designed";
        return 0;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("Collection(");
        sb.append(ExprUnparser.quoteIdentifier(this.schema.getName()));
        sb.append(".");
        sb.append(ExprUnparser.quoteIdentifier(this.name));
        sb.append(")");
        return sb.toString();
    }

    @Override
    public Result replaceOne(String id, DbDoc doc) {
        if (id == null) {
            throw new XDevAPIError(Messages.getString("CreateTableStatement.0", new String[] { "id" }));
        }
        if (doc == null) {
            throw new XDevAPIError(Messages.getString("CreateTableStatement.0", new String[] { "doc" }));
        }
        return modify("_id = :id").set("$", doc).bind("id", id).execute();
    }

    @Override
    public Result replaceOne(String id, String jsonString) {
        if (id == null) {
            throw new XDevAPIError(Messages.getString("CreateTableStatement.0", new String[] { "id" }));
        }
        if (jsonString == null) {
            throw new XDevAPIError(Messages.getString("CreateTableStatement.0", new String[] { "jsonString" }));
        }
        try {
            return replaceOne(id, JsonParser.parseDoc(new StringReader(jsonString)));
        } catch (IOException e) {
            throw AssertionFailedException.shouldNotHappen(e);
        }
    }

    @Override
    public Result addOrReplaceOne(String id, DbDoc doc) {
        if (id == null) {
            throw new XDevAPIError(Messages.getString("CreateTableStatement.0", new String[] { "id" }));
        }
        if (doc == null) {
            throw new XDevAPIError(Messages.getString("CreateTableStatement.0", new String[] { "doc" }));
        }
        if (doc.get("_id") == null) {
            doc.add("_id", new JsonString().setValue(id));
        } else if (!id.equals(((JsonString) doc.get("_id")).getString())) {
            throw new XDevAPIError("Document already has an _id that doesn't match to id parameter");
        }
        return add(doc).setUpsert(true).execute();
    }

    @Override
    public Result addOrReplaceOne(String id, String jsonString) {
        if (id == null) {
            throw new XDevAPIError(Messages.getString("CreateTableStatement.0", new String[] { "id" }));
        }
        if (jsonString == null) {
            throw new XDevAPIError(Messages.getString("CreateTableStatement.0", new String[] { "jsonString" }));
        }
        try {
            return addOrReplaceOne(id, JsonParser.parseDoc(new StringReader(jsonString)));
        } catch (IOException e) {
            throw AssertionFailedException.shouldNotHappen(e);
        }
    }

    @Override
    public DbDoc getOne(String id) {
        return find("_id = :id").bind("id", id).execute().fetchOne();
    }

    @Override
    public Result removeOne(String id) {
        return remove("_id = :id").bind("id", id).execute();
    }
}
