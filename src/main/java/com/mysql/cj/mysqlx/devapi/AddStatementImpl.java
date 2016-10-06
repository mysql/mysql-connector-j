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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import com.mysql.cj.api.x.AddStatement;
import com.mysql.cj.api.x.Result;
import com.mysql.cj.core.exceptions.AssertionFailedException;
import com.mysql.cj.core.io.StatementExecuteOk;
import com.mysql.cj.mysqlx.DocumentID;
import com.mysql.cj.x.json.DbDoc;
import com.mysql.cj.x.json.JsonParser;
import com.mysql.cj.x.json.JsonString;

/**
 * @todo
 */
public class AddStatementImpl implements AddStatement {
    private CollectionImpl collection;
    private List<DbDoc> newDocs;

    /* package private */ AddStatementImpl(CollectionImpl collection, DbDoc newDoc) {
        this.collection = collection;
        this.newDocs = new ArrayList<>();
        this.newDocs.add(newDoc);
    }

    /* package private */ AddStatementImpl(CollectionImpl collection, DbDoc[] newDocs) {
        this.collection = collection;
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
        return this.newDocs.stream().filter(d -> d.get("_id") == null).map(d -> {
            String newId = DocumentID.generate();
            d.put("_id", new JsonString().setValue(newId));
            return newId;
        }).collect(Collectors.toList());
    }

    private List<String> serializeDocs() {
        return this.newDocs.stream().map(DbDoc::toPackedString).collect(Collectors.toList());
    }

    public Result execute() {
        if (this.newDocs.size() == 0) { // according to dev api sec, this is a no-op. we create an empty Result
            StatementExecuteOk ok = new StatementExecuteOk(0, null, new ArrayList<>());
            return new UpdateResult(ok, new ArrayList<>());
        }
        List<String> newIds = assignIds();
        StatementExecuteOk ok = this.collection.getSession().getMysqlxSession().addDocs(this.collection.getSchema().getName(), this.collection.getName(),
                serializeDocs());
        return new UpdateResult(ok, newIds);
    }

    public CompletableFuture<Result> executeAsync() {
        final List<String> newIds = assignIds();
        CompletableFuture<StatementExecuteOk> okF = this.collection.getSession().getMysqlxSession().asyncAddDocs(this.collection.getSchema().getName(),
                this.collection.getName(), serializeDocs());
        return okF.thenApply(ok -> new UpdateResult(ok, newIds));
    }
}
