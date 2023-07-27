/*
 * Copyright (c) 2015, 2023, Oracle and/or its affiliates.
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
        this.xbuilder = (XMessageBuilder) this.mysqlxSession.<XMessage>getMessageBuilder();
    }

    @Override
    public Session getSession() {
        return this.schema.getSession();
    }

    @Override
    public Schema getSchema() {
        return this.schema;
    }

    @Override
    public String getName() {
        return this.name;
    }

    @Override
    public DbObjectStatus existsInDatabase() {
        if (this.mysqlxSession.getDataStoreMetadata().tableExists(this.schema.getName(), this.name)) {
            // TODO should also check that the table has a DbObjectType.COLLECTION type
            return DbObjectStatus.EXISTS;
        }
        return DbObjectStatus.NOT_EXISTS;
    }

    @Override
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

    @Override
    public AddStatement add(DbDoc... docs) {
        return new AddStatementImpl(this.mysqlxSession, this.schema.getName(), this.name, docs);
    }

    @Override
    public FindStatement find() {
        return find(null);
    }

    @Override
    public FindStatement find(String searchCondition) {
        return new FindStatementImpl(this.mysqlxSession, this.schema.getName(), this.name, searchCondition);
    }

    @Override
    public ModifyStatement modify(String searchCondition) {
        return new ModifyStatementImpl(this.mysqlxSession, this.schema.getName(), this.name, searchCondition);
    }

    @Override
    public RemoveStatement remove(String searchCondition) {
        return new RemoveStatementImpl(this.mysqlxSession, this.schema.getName(), this.name, searchCondition);
    }

    @Override
    public Result createIndex(String indexName, DbDoc indexDefinition) {
        return this.mysqlxSession.query(
                this.xbuilder.buildCreateCollectionIndex(this.schema.getName(), this.name, new CreateIndexParams(indexName, indexDefinition)),
                new UpdateResultBuilder<>());
    }

    @Override
    public Result createIndex(String indexName, String jsonIndexDefinition) {
        return this.mysqlxSession.query(
                this.xbuilder.buildCreateCollectionIndex(this.schema.getName(), this.name, new CreateIndexParams(indexName, jsonIndexDefinition)),
                new UpdateResultBuilder<>());
    }

    @Override
    public void dropIndex(String indexName) {
        try {
            this.mysqlxSession.query(this.xbuilder.buildDropCollectionIndex(this.schema.getName(), this.name, indexName), new UpdateResultBuilder<>());
        } catch (XProtocolError e) {
            // If specified object does not exist, dropX() methods succeed (no error is reported)
            // TODO check MySQL > 8.0.1 for built in solution, like passing ifExists to dropView
            if (e.getErrorCode() != MysqlErrorNumbers.ER_CANT_DROP_FIELD_OR_KEY) {
                throw e;
            }
        }
    }

    @Override
    public long count() {
        try {
            return this.mysqlxSession.getDataStoreMetadata().getTableRowCount(this.schema.getName(), this.name);
        } catch (XProtocolError e) {
            if (e.getErrorCode() == MysqlErrorNumbers.ER_NO_SUCH_TABLE) {
                throw new XProtocolError("Collection '" + this.name + "' does not exist in schema '" + this.schema.getName() + "'", e);
            }
            throw e;
        }
    }

    @Override
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
        JsonValue docId = doc.get("_id");
        if (docId != null && (!JsonString.class.isInstance(docId) || !id.equals(((JsonString) docId).getString()))) {
            throw new XDevAPIError(Messages.getString("Collection.DocIdMismatch"));
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
        JsonValue docId = doc.get("_id");
        if (docId == null) {
            doc.add("_id", new JsonString().setValue(id));
        } else if (!JsonString.class.isInstance(docId) || !id.equals(((JsonString) docId).getString())) {
            throw new XDevAPIError(Messages.getString("Collection.DocIdMismatch"));
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
