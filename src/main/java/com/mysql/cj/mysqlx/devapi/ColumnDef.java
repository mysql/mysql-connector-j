/*
  Copyright (c) 2016, Oracle and/or its affiliates. All rights reserved.

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

import java.util.HashMap;
import java.util.Map;

import com.mysql.cj.api.x.ColumnDefinition.StaticColumnDefinition;
import com.mysql.cj.api.x.Type;
import com.mysql.cj.core.util.StringUtils;

public final class ColumnDef extends AbstractColumnDef<StaticColumnDefinition> implements StaticColumnDefinition {
    protected String defaultExpr = null;
    protected boolean defaultExprWasSet = false;
    protected boolean autoIncrement = false;
    protected Map<String, String[]> foreignKey = new HashMap<>();

    public ColumnDef(String columnName, Type columnType) {
        this.name = columnName;
        this.type = columnType;
    }

    public ColumnDef(String columnName, Type columnType, int length) {
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
