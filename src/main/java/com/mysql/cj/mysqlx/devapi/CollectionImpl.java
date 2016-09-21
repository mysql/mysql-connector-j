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

package com.mysql.cj.mysqlx.devapi;

import java.io.IOException;
import java.io.StringReader;
import java.util.Map;

import com.mysql.cj.api.x.AddStatement;
import com.mysql.cj.api.x.BaseSession;
import com.mysql.cj.api.x.Collection;
import com.mysql.cj.api.x.CreateCollectionIndexStatement;
import com.mysql.cj.api.x.DropCollectionIndexStatement;
import com.mysql.cj.api.x.FindStatement;
import com.mysql.cj.api.x.ModifyStatement;
import com.mysql.cj.api.x.RemoveStatement;
import com.mysql.cj.api.x.Schema;
import com.mysql.cj.core.exceptions.AssertionFailedException;
import com.mysql.cj.core.exceptions.FeatureNotAvailableException;
import com.mysql.cj.mysqlx.ExprUnparser;
import com.mysql.cj.x.json.DbDoc;
import com.mysql.cj.x.json.JsonParser;

public class CollectionImpl implements Collection {
    private SchemaImpl schema;
    private String name;

    /* package private */ CollectionImpl(SchemaImpl schema, String name) {
        this.schema = schema;
        this.name = name;
    }

    public BaseSession getSession() {
        return this.schema.getSession();
    }

    public Schema getSchema() {
        return this.schema;
    }

    public String getName() {
        return this.name;
    }

    public DbObjectStatus existsInDatabase() {
        if (this.schema.getSession().getMysqlxSession().tableExists(this.schema.getName(), this.name)) {
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
        return new AddStatementImpl(this, doc);
    }

    public AddStatement add(DbDoc... docs) {
        return new AddStatementImpl(this, docs);
    }

    public FindStatement find() {
        return find(null);
    }

    public FindStatement find(String searchCondition) {
        return new FindStatementImpl(this, searchCondition);
    }

    public ModifyStatement modify() {
        return modify(null);
    }

    public ModifyStatement modify(String searchCondition) {
        return new ModifyStatementImpl(this, searchCondition);
    }

    public RemoveStatement remove() {
        return remove(null);
    }

    public RemoveStatement remove(String searchCondition) {
        return new RemoveStatementImpl(this, searchCondition);
    }

    public CreateCollectionIndexStatement createIndex(String indexName, boolean isUnique) {
        return new CreateCollectionIndexStatementImpl(this, indexName, isUnique);
    }

    public DropCollectionIndexStatement dropIndex(String indexName) {
        return new DropCollectionIndexStatementImpl(this, indexName);
    }

    public long count() {
        return this.schema.getSession().getMysqlxSession().tableCount(this.schema.getName(), this.name);
    }

    public DbDoc newDoc() {
        return new DbDoc();
    }

    @Override
    public boolean equals(Object other) {
        if (other != null && other.getClass() == CollectionImpl.class) {
            if (((CollectionImpl) other).schema.equals(this.schema)) {
                if (((CollectionImpl) other).schema.getSession() == this.schema.getSession()) {
                    return this.name.equals(((CollectionImpl) other).name);
                }
            }
        }
        return false;
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
