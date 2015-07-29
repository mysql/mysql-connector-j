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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import com.mysql.cj.api.x.CollectionStatement;
import com.mysql.cj.api.x.Result;
import com.mysql.cj.core.io.StatementExecuteOk;
import com.mysql.cj.x.json.JsonDoc;
import com.mysql.cj.x.json.JsonString;

/**
 * @todo
 */
public class AddStatementImpl implements CollectionStatement.AddStatement {
    private CollectionImpl collection;
    private List<JsonDoc> newDocs;

    /* package private */AddStatementImpl(CollectionImpl collection, JsonDoc newDoc) {
        this.collection = collection;
        this.newDocs = new ArrayList<>();
        this.newDocs.add(newDoc);
    }

    /* package private */AddStatementImpl(CollectionImpl collection, JsonDoc[] newDocs) {
        this.collection = collection;
        this.newDocs = Arrays.asList(newDocs);
    }

    public Result execute() {
        List<String> newIds = this.newDocs.stream().filter(d -> d.get("_id") == null).map(d -> {
            String newId = UUID.randomUUID().toString().replaceAll("-", "");
            d.put("_id", new JsonString().setValue(newId));
            return newId;
        }).collect(Collectors.toList());

        List<String> jsonStrings = this.newDocs.stream().map(Object::toString).collect(Collectors.toList());

        StatementExecuteOk ok = this.collection.getSession().getMysqlxSession()
                .addDocs(this.collection.getSchema().getName(), this.collection.getName(), jsonStrings);
        String newId = newIds.size() > 0 ? newIds.get(0) : null;
        return new UpdateResult(ok, newId);
    }
}
