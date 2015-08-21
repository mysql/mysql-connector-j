/*
  Copyright (c) 2015, Oracle and/or its affiliates. All rights reserved.

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
import java.util.stream.Collectors;

import com.mysql.cj.api.x.Collection;
import com.mysql.cj.api.x.Schema;
import com.mysql.cj.api.x.Session;
import com.mysql.cj.api.x.Table;
import com.mysql.cj.api.x.View;
import com.mysql.cj.core.exceptions.MysqlErrorNumbers;
import com.mysql.cj.core.exceptions.WrongArgumentException;
import com.mysql.cj.mysqlx.ExprUnparser;
import com.mysql.cj.mysqlx.MysqlxError;

public class SchemaImpl implements Schema {
    private Session session;
    private String name;

    /* package private */SchemaImpl(Session session, String name) {
        this.session = session;
        this.name = name;
    }

    public Session getSession() {
        return this.session;
    }

    public Schema getSchema() {
        return this;
    }

    public String getName() {
        return this.name;
    }

    public DbObjectStatus existsInDatabase() {
        if (this.session.getMysqlxSession().schemaExists(this.name)) {
            return DbObjectStatus.EXISTS;
        }
        return DbObjectStatus.NOT_EXISTS;
    }

    public List<Collection> getCollections() {
        return this.session.getMysqlxSession().getObjectNamesOfType(this.name, "COLLECTION").stream().map(this::getCollection).collect(Collectors.toList());
    }

    public List<Table> getTables() {
        return this.session.getMysqlxSession().getObjectNamesOfType(this.name, "TABLE").stream().map(this::getTable).collect(Collectors.toList());
    }

    public List<View> getViews() {
        throw new NullPointerException("TODO:");
    }

    public Collection getCollection(String collectionName) {
        return new CollectionImpl(this, collectionName);
    }

    public Collection getCollection(String collectionName, boolean requireExists) {
        CollectionImpl coll = new CollectionImpl(this, collectionName);
        if (requireExists && coll.existsInDatabase() != DbObjectStatus.EXISTS) {
            // TODO: We should have a better exception design for the API
            throw new WrongArgumentException(coll.toString() + " doesn't exist");
        }
        return coll;
    }

    public Table getCollectionAsTable(String collectionName) {
        return getTable(collectionName);
    }

    public Table getTable(String tableName) {
        return new TableImpl(this, tableName);
    }

    public Table getTable(String tableName, boolean requireExists) {
        throw new NullPointerException("TODO:");
    }

    public View getView(String viewName) {
        throw new NullPointerException("TODO:");
    }

    public void drop() {
        throw new NullPointerException("TODO:");
    }

    public Collection createCollection(String collectionName) {
        this.session.getMysqlxSession().createCollection(this.name, collectionName);
        return new CollectionImpl(this, collectionName);
    }

    public Collection createCollection(String collectionName, boolean reuseExistingObject) {
        try {
            return createCollection(collectionName);
        } catch (MysqlxError ex) {
            if (ex.getErrorCode() == MysqlErrorNumbers.ER_TABLE_EXISTS_ERROR) {
                return getCollection(collectionName);
            }
            throw ex;
        }
    }

    public View createView(String viewName) {
        throw new NullPointerException("TODO:");
    }

    @Override
    public boolean equals(Object other) {
        if (other != null && other.getClass() == SchemaImpl.class) {
            if (((SchemaImpl) other).session == this.session) {
                return this.name.equals(((SchemaImpl) other).name);
            }
        }
        return false;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("Schema(");
        sb.append(ExprUnparser.quoteIdentifier(this.name));
        sb.append(")");
        return sb.toString();
    }
}
