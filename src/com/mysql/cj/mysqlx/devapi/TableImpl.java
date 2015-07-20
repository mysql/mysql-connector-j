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

import com.mysql.cj.api.x.Schema;
import com.mysql.cj.api.x.Session;
import com.mysql.cj.api.x.Table;
import com.mysql.cj.api.x.TableStatement.DeleteStatement;
import com.mysql.cj.api.x.TableStatement.InsertStatement;
import com.mysql.cj.api.x.TableStatement.SelectStatement;
import com.mysql.cj.api.x.TableStatement.UpdateStatement;
import com.mysql.cj.mysqlx.ExprUnparser;

public class TableImpl implements Table {
    private SessionImpl session;
    private SchemaImpl schema;
    private String name;

    /* package private */ TableImpl(SessionImpl session, SchemaImpl schema, String name) {
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
        if (this.session.getMysqlxSession().tableExists(this.schema.getName(), this.name)) {
            return DbObjectStatus.EXISTS;
        } else {
            return DbObjectStatus.NOT_EXISTS;
        }
    }

    public InsertStatement insert(String projection) {
        return new InsertStatementImpl(this.session, this, projection);
    }

    public SelectStatement select(String searchFields) {
        return new SelectStatementImpl(this.session, this, searchFields);
    }

    public UpdateStatement update() {
        return new UpdateStatementImpl(this.session, this);
    }

    public DeleteStatement delete() {
        throw new NullPointerException("TODO: ");
    }

    public Table as(String alias) {
        throw new NullPointerException("TODO: ");
    }

    public long count() {
        return this.session.getMysqlxSession().tableCount(this.schema.getName(), this.name);
    }

    @Override
    public boolean equals(Object other) {
        if (other != null && other.getClass() == TableImpl.class) {
            if (((TableImpl) other).schema.equals(this.schema)) {
                if (((TableImpl) other).session == this.session) {
                    return this.name.equals(((TableImpl) other).name);
                }
            }
        }
        return false;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("Table(");
        sb.append(ExprUnparser.quoteIdentifier(this.schema.getName()));
        sb.append(".");
        sb.append(ExprUnparser.quoteIdentifier(this.name));
        sb.append(")");
        return sb.toString();
    }
}
