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

import java.util.Iterator;
import java.util.UUID;
import java.util.concurrent.Future;

import com.mysql.cj.api.x.CollectionStatement;
import com.mysql.cj.api.x.DbDoc;
import com.mysql.cj.api.x.Result;
import com.mysql.cj.api.x.Statement;
import com.mysql.cj.x.json.JsonDoc;
import com.mysql.cj.x.json.JsonValueString;

public class AddStatementImpl implements CollectionStatement.AddStatement {
    private SessionImpl session;
    private CollectionImpl collection;
    private JsonDoc newDoc;

    /* package private */ AddStatementImpl(SessionImpl session, CollectionImpl collection, JsonDoc newDoc) {
        this.session = session;
        this.collection = collection;
        this.newDoc = newDoc;
    }

    public Result execute() {
        String newId = null;
        // TODO: string constants (c.f. CollectionImpl.add()'s validation)
        if (this.newDoc.get("_id") == null) {
            newId = UUID.randomUUID().toString().replaceAll("-", "");
            newDoc.put("_id", new JsonValueString().setValue(newId));
        }
        return this.session.getMysqlxSession().addDoc(this.collection.getSchema().getName(), this.collection.getName(), newDoc.toString(), newId);
    }

    public Future<Result> executeAsync() {
        throw new NullPointerException("TODO:");
    }

    // TODO: put all these as default implementations of Statement interface?
    public Statement bind(DbDoc document) {
        throw new UnsupportedOperationException("This statement doesn't support bound parameters");
    }

    public Statement bind(String key, String value, String... others) {
        throw new NullPointerException("TODO:");
    }

    public <T> Statement bind(Iterator<T> iterator) {
        throw new NullPointerException("TODO:");
    }

    public Statement bind(String val) {
        throw new NullPointerException("TODO:");
    }

    public Statement bind(int val) {
        throw new NullPointerException("TODO:");
    }
}
