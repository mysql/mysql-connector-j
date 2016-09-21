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

import java.util.List;
import java.util.Map;

import com.mysql.cj.api.x.BaseSession;
import com.mysql.cj.api.x.DeleteStatement;
import com.mysql.cj.api.x.InsertStatement;
import com.mysql.cj.api.x.Schema;
import com.mysql.cj.api.x.SelectStatement;
import com.mysql.cj.api.x.Table;
import com.mysql.cj.api.x.UpdateStatement;
import com.mysql.cj.mysqlx.ExprUnparser;

public class TableImpl implements Table {

    private SchemaImpl schema;
    private String name;
    private Boolean isView = null;

    /* package private */ TableImpl(SchemaImpl schema, String name) {
        this.schema = schema;
        this.name = name;
    }

    /* package private */ TableImpl(SchemaImpl schema, DatabaseObjectDescription descr) {
        this.schema = schema;
        this.name = descr.getObjectName();
        this.isView = descr.getObjectType() == DbObjectType.VIEW;
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

    public InsertStatement insert() {
        return new InsertStatementImpl(this, new String[] {});
    }

    public InsertStatement insert(String... fields) {
        return new InsertStatementImpl(this, fields);
    }

    public InsertStatement insert(Map<String, Object> fieldsAndValues) {
        return new InsertStatementImpl(this, fieldsAndValues);
    }

    @Override
    public SelectStatement select(String... projection) {
        return new SelectStatementImpl(this, projection);
    }

    public UpdateStatement update() {
        return new UpdateStatementImpl(this);
    }

    public DeleteStatement delete() {
        return new DeleteStatementImpl(this);
    }

    public long count() {
        return this.schema.getSession().getMysqlxSession().tableCount(this.schema.getName(), this.name);
    }

    @Override
    public boolean equals(Object other) {
        if (other != null && other.getClass() == TableImpl.class) {
            if (((TableImpl) other).schema.equals(this.schema)) {
                if (((TableImpl) other).schema.getSession() == this.schema.getSession()) {
                    return this.name.equals(((TableImpl) other).name);
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
        StringBuilder sb = new StringBuilder("Table(");
        sb.append(ExprUnparser.quoteIdentifier(this.schema.getName()));
        sb.append(".");
        sb.append(ExprUnparser.quoteIdentifier(this.name));
        sb.append(")");
        return sb.toString();
    }

    @Override
    public boolean isView() {
        // if this.isView isn't set (was unknown on the table construction time) then query database
        if (this.isView == null) {
            List<DatabaseObjectDescription> objects = getSession().getMysqlxSession().listObjects(this.schema.getName(), this.name);
            if (objects.isEmpty()) {
                // object not found, means it doesn't exist in database
                return false;
            }
            // objects should contain exactly one element with matching this.name
            this.isView = objects.get(0).getObjectType() == DbObjectType.VIEW;
        }
        return this.isView;
    }

    public void setView(boolean isView) {
        this.isView = isView;
    }
}
