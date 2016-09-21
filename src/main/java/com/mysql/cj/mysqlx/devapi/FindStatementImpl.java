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

import java.util.concurrent.CompletableFuture;

import com.mysql.cj.api.x.DocResult;
import com.mysql.cj.api.x.Expression;
import com.mysql.cj.api.x.FindStatement;
import com.mysql.cj.mysqlx.DocFindParams;
import com.mysql.cj.x.json.DbDoc;

public class FindStatementImpl extends FilterableStatement<FindStatement, DocResult> implements FindStatement {
    private CollectionImpl collection;
    private DocFindParams findParams;

    /* package private */ FindStatementImpl(CollectionImpl collection, String criteria) {
        super(new DocFindParams(collection.getSchema().getName(), collection.getName()));
        this.findParams = (DocFindParams) this.filterParams;
        this.collection = collection;
        if (criteria != null && criteria.length() > 0) {
            this.findParams.setCriteria(criteria);
        }
    }

    public DocResultImpl execute() {
        return this.collection.getSession().getMysqlxSession().findDocs(this.findParams);
    }

    public CompletableFuture<DocResult> executeAsync() {
        return this.collection.getSession().getMysqlxSession().asyncFindDocs(this.findParams);
    }

    public <R> CompletableFuture<R> executeAsync(R id, Reducer<DbDoc, R> reducer) {
        return this.collection.getSession().getMysqlxSession().asyncFindDocsReduce(this.findParams, id, reducer);
    }

    @Override
    public FindStatement fields(String... projection) {
        this.findParams.setFields(projection);
        return this;
    }

    public FindStatement fields(Expression docProjection) {
        this.findParams.setFields(docProjection);
        return this;
    }

    @Override
    public FindStatement groupBy(String... groupBy) {
        this.findParams.setGrouping(groupBy);
        return this;
    }

    public FindStatement having(String having) {
        this.findParams.setGroupingCriteria(having);
        return this;
    }
}
