/*
  Copyright (c) 2015, 2017, Oracle and/or its affiliates. All rights reserved.

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

package com.mysql.cj.xdevapi;

import java.io.IOException;
import java.io.StringReader;
import java.util.Map;

import com.mysql.cj.api.xdevapi.AddStatement;
import com.mysql.cj.api.xdevapi.Collection;
import com.mysql.cj.api.xdevapi.CreateCollectionIndexStatement;
import com.mysql.cj.api.xdevapi.DropCollectionIndexStatement;
import com.mysql.cj.api.xdevapi.FindStatement;
import com.mysql.cj.api.xdevapi.ModifyStatement;
import com.mysql.cj.api.xdevapi.RemoveStatement;
import com.mysql.cj.api.xdevapi.Schema;
import com.mysql.cj.api.xdevapi.Session;
import com.mysql.cj.core.exceptions.AssertionFailedException;
import com.mysql.cj.core.exceptions.FeatureNotAvailableException;
import com.mysql.cj.x.core.MysqlxSession;

public class CollectionImpl implements Collection {
    private MysqlxSession mysqlxSession;
    private SchemaImpl schema;
    private String name;

    /* package private */ CollectionImpl(MysqlxSession mysqlxSession, SchemaImpl schema, String name) {
        this.mysqlxSession = mysqlxSession;
        this.schema = schema;
        this.name = name;
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
        if (this.mysqlxSession.tableExists(this.schema.getName(), this.name)) {
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

    public CreateCollectionIndexStatement createIndex(String indexName, boolean isUnique) {
        return new CreateCollectionIndexStatementImpl(this.mysqlxSession, this.schema.getName(), this.name, indexName, isUnique);
    }

    public DropCollectionIndexStatement dropIndex(String indexName) {
        return new DropCollectionIndexStatementImpl(this.mysqlxSession, this.schema.getName(), this.name, indexName);
    }

    public long count() {
        return this.mysqlxSession.tableCount(this.schema.getName(), this.name);
    }

    public DbDoc newDoc() {
        return new DbDoc();
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
}
