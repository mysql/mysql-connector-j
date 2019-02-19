/*
 * Copyright (c) 2015, 2019, Oracle and/or its affiliates. All rights reserved.
 *
 * This program is free software; you can redistribute it and/or modify it under
 * the terms of the GNU General Public License, version 2.0, as published by the
 * Free Software Foundation.
 *
 * This program is also distributed with certain software (including but not
 * limited to OpenSSL) that is licensed under separate terms, as designated in a
 * particular file or component or in included license documentation. The
 * authors of MySQL hereby grant you an additional permission to link the
 * program and your derivative works with the separately licensed software that
 * they have included with MySQL.
 *
 * Without limiting anything contained in the foregoing, this file, which is
 * part of MySQL Connector/J, is also subject to the Universal FOSS Exception,
 * version 1.0, a copy of which can be found at
 * http://oss.oracle.com/licenses/universal-foss-exception.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 * FOR A PARTICULAR PURPOSE. See the GNU General Public License, version 2.0,
 * for more details.
 *
 * You should have received a copy of the GNU General Public License along with
 * this program; if not, write to the Free Software Foundation, Inc.,
 * 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA
 */

package com.mysql.cj.xdevapi;

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.mysql.cj.Messages;
import com.mysql.cj.MysqlxSession;
import com.mysql.cj.protocol.x.XMessage;
import com.mysql.cj.protocol.x.XMessageBuilder;
import com.mysql.cj.protocol.x.XProtocolError;
import com.mysql.cj.result.Row;
import com.mysql.cj.result.StringValueFactory;
import com.mysql.cj.result.ValueFactory;

/**
 * {@link Table} implementation
 */
public class TableImpl implements Table {

    private MysqlxSession mysqlxSession;
    private SchemaImpl schema;
    private String name;
    private Boolean isView = null;
    private XMessageBuilder xbuilder;

    /* package private */ TableImpl(MysqlxSession mysqlxSession, SchemaImpl schema, String name) {
        if (mysqlxSession == null) {
            throw new XDevAPIError(Messages.getString("CreateTableStatement.0", new String[] { "mysqlxSession" }));
        }
        if (schema == null) {
            throw new XDevAPIError(Messages.getString("CreateTableStatement.0", new String[] { "schema" }));
        }
        if (name == null) {
            throw new XDevAPIError(Messages.getString("CreateTableStatement.0", new String[] { "name" }));
        }
        this.mysqlxSession = mysqlxSession;
        this.xbuilder = (XMessageBuilder) this.mysqlxSession.<XMessage> getMessageBuilder();
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
        if (this.mysqlxSession.getDataStoreMetadata().tableExists(this.schema.getName(), this.name)) {
            return DbObjectStatus.EXISTS;
        }
        return DbObjectStatus.NOT_EXISTS;
    }

    public InsertStatement insert() {
        return new InsertStatementImpl(this.mysqlxSession, this.schema.getName(), this.name, new String[] {});
    }

    public InsertStatement insert(String... fields) {
        return new InsertStatementImpl(this.mysqlxSession, this.schema.getName(), this.name, fields);
    }

    public InsertStatement insert(Map<String, Object> fieldsAndValues) {
        return new InsertStatementImpl(this.mysqlxSession, this.schema.getName(), this.name, fieldsAndValues);
    }

    @Override
    public SelectStatement select(String... projection) {
        return new SelectStatementImpl(this.mysqlxSession, this.schema.getName(), this.name, projection);
    }

    public UpdateStatement update() {
        return new UpdateStatementImpl(this.mysqlxSession, this.schema.getName(), this.name);
    }

    public DeleteStatement delete() {
        return new DeleteStatementImpl(this.mysqlxSession, this.schema.getName(), this.name);
    }

    public long count() {
        try {
            return this.mysqlxSession.getDataStoreMetadata().getTableRowCount(this.schema.getName(), this.name);
        } catch (XProtocolError e) {
            if (e.getErrorCode() == 1146) {
                throw new XProtocolError("Table '" + this.name + "' does not exist in schema '" + this.schema.getName() + "'", e);
            }
            throw e;
        }
    }

    @Override
    public boolean equals(Object other) {
        return other != null && other.getClass() == TableImpl.class && ((TableImpl) other).schema.equals(this.schema)
                && ((TableImpl) other).mysqlxSession == this.mysqlxSession && this.name.equals(((TableImpl) other).name);
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
            ValueFactory<String> svf = new StringValueFactory(this.mysqlxSession.getPropertySet());
            Function<Row, DatabaseObjectDescription> rowToDatabaseObjectDescription = r -> new DatabaseObjectDescription(r.getValue(0, svf),
                    r.getValue(1, svf));
            List<DatabaseObjectDescription> objects = this.mysqlxSession.query(this.xbuilder.buildListObjects(this.schema.getName(), this.name), null,
                    rowToDatabaseObjectDescription, Collectors.toList());
            if (objects.isEmpty()) {
                // object not found, means it doesn't exist in database
                return false;
            }
            // objects should contain exactly one element with matching this.name
            this.isView = objects.get(0).getObjectType() == DbObjectType.VIEW || objects.get(0).getObjectType() == DbObjectType.COLLECTION_VIEW;
        }
        return this.isView;
    }

    /**
     * Set flag indicating if the underlying object is a view.
     * 
     * @param isView
     *            true if it is a View
     */
    public void setView(boolean isView) {
        this.isView = isView;
    }
}
