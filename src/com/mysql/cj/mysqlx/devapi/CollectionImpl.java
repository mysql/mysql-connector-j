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

package com.mysql.cj.mysqlx.devapi;

import java.io.IOException;
import java.io.StringReader;
import java.util.Map;

import com.mysql.cj.api.x.Collection;
import com.mysql.cj.api.x.DbDoc;
import com.mysql.cj.api.x.Schema;
import com.mysql.cj.api.x.Session;
import com.mysql.cj.api.x.CollectionStatement.AddStatement;
import com.mysql.cj.api.x.CollectionStatement.FindStatement;
import com.mysql.cj.api.x.CollectionStatement.ModifyStatement;
import com.mysql.cj.api.x.CollectionStatement.RemoveStatement;
import com.mysql.cj.core.exceptions.AssertionFailedException;
import com.mysql.cj.x.json.JsonDoc;
import com.mysql.cj.x.json.JsonParser;

public class CollectionImpl implements Collection {
    private SessionImpl session;
    private SchemaImpl schema;
    private String name;

    /* package private */ CollectionImpl(SessionImpl session, SchemaImpl schema, String name) {
        this.session = session;
        this.schema = schema;
        this.name = name;
    }

    public Session getSession() {
        return this.session;
    }

    public Schema getSchema() {
        return this.schema;
    }

    public String getName() {
        return this.name;
    }

    public DbObjectStatus existsInDatabase() {
        throw new NullPointerException("TODO:");
    }

    public AddStatement add(Map<String, ?> doc) {
        throw new NullPointerException("TODO: ");
    }

    public AddStatement add(String jsonString) {
        try {
            JsonDoc doc = JsonParser.parseDoc(new StringReader(jsonString));
            return add((DbDoc) doc);
        } catch (IOException ex) {
            throw AssertionFailedException.shouldNotHappen(ex);
        }
    }

    public AddStatement add(DbDoc document) {
        JsonDoc doc = (JsonDoc) document;
        return new AddStatementImpl(this.session, this, doc);
    }

    public FindStatement find(String searchCondition) {
        return new FindStatementImpl(this.session, this, searchCondition);
    }

    public ModifyStatement modify(String searchCondition) {
        throw new NullPointerException("TODO:");
    }

    public RemoveStatement remove(String searchCondition) {
        throw new NullPointerException("TODO:");
    }

    public void drop() {
        this.session.getMysqlxSession().dropCollection(this.schema.getName(), this.name);
    }

    public Collection as(String alias) {
        throw new NullPointerException("TODO: this should be moved to Dev API v2. it doesn't have any meaning in v1");
    }

    public long count() {
        return this.session.getMysqlxSession().tableCount(this.schema.getName(), this.name);
    }

    public DbDoc newDoc() {
        return new JsonDoc();
    }
}
