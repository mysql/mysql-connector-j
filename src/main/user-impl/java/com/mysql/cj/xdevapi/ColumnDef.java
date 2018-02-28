/*
 * Copyright (c) 2016, 2018, Oracle and/or its affiliates. All rights reserved.
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

import java.util.HashMap;
import java.util.Map;

import com.mysql.cj.Messages;
import com.mysql.cj.util.StringUtils;
import com.mysql.cj.xdevapi.ColumnDefinition.StaticColumnDefinition;

public final class ColumnDef extends AbstractColumnDef<StaticColumnDefinition> implements StaticColumnDefinition {
    protected String defaultExpr = null;
    protected boolean defaultExprWasSet = false;
    protected boolean autoIncrement = false;
    protected Map<String, String[]> foreignKey = new HashMap<>();

    public ColumnDef(String columnName, Type columnType) {
        if (columnName == null) {
            throw new XDevAPIError(Messages.getString("CreateTableStatement.0", new String[] { "columnName" }));
        }
        if (columnType == null) {
            throw new XDevAPIError(Messages.getString("CreateTableStatement.0", new String[] { "columnType" }));
        }
        this.name = columnName;
        this.type = columnType;
    }

    public ColumnDef(String columnName, Type columnType, int length) {
        if (columnName == null) {
            throw new XDevAPIError(Messages.getString("CreateTableStatement.0", new String[] { "columnName" }));
        }
        if (columnType == null) {
            throw new XDevAPIError(Messages.getString("CreateTableStatement.0", new String[] { "columnType" }));
        }
        this.name = columnName;
        this.type = columnType;
        this.length = length;
    }

    @Override
    StaticColumnDefinition self() {
        return this;
    }

    @Override
    public StaticColumnDefinition setDefault(String expr) {
        this.defaultExpr = expr;
        this.defaultExprWasSet = true;
        return self();
    }

    @Override
    public StaticColumnDefinition autoIncrement() {
        this.autoIncrement = true;
        return self();
    }

    @Override
    public StaticColumnDefinition foreignKey(String tableName, String... foreignColumnName) {
        if (tableName == null) {
            throw new XDevAPIError(Messages.getString("CreateTableStatement.0", new String[] { "tableName" }));
        }
        if (foreignColumnName == null) {
            throw new XDevAPIError(Messages.getString("CreateTableStatement.0", new String[] { "foreignColumnName" }));
        }

        for (String c : foreignColumnName) {
            if (c == null) {
                throw new XDevAPIError(Messages.getString("CreateTableStatement.1", new String[] { "foreignColumnName" }));
            }
        }

        this.foreignKey.put(tableName, foreignColumnName);
        return self();
    }

    /**
     * column_definition:
     * data_type [NOT NULL | NULL] [DEFAULT default_value]
     * [AUTO_INCREMENT] [UNIQUE [KEY] | [PRIMARY] KEY]
     * [COMMENT 'string']
     * [COLUMN_FORMAT {FIXED|DYNAMIC|DEFAULT}]
     * [STORAGE {DISK|MEMORY|DEFAULT}]
     * [reference_definition]
     */
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder(this.name);
        sb.append(" ").append(getMysqlType());

        if (this.notNull != null) {
            sb.append(this.notNull ? " NOT NULL" : " NULL");
        }
        if (this.defaultExprWasSet) {
            sb.append(" DEFAULT ").append(this.defaultExpr);
        }
        if (this.autoIncrement) {
            sb.append(" AUTO_INCREMENT");
        }
        if (this.primaryKey) {
            sb.append(" PRIMARY KEY");
        } else if (this.uniqueIndex) {
            sb.append(" UNIQUE KEY");
        }
        if (this.comment != null && !this.comment.isEmpty()) {
            sb.append(" COMMENT ").append(StringUtils.quoteIdentifier(this.comment, "'", true));
        }

        return sb.toString();
    }

}
