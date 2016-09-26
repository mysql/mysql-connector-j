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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.mysql.cj.api.x.ColumnDefinition;
import com.mysql.cj.api.x.CreateTableStatement.CreateTableFullStatement;
import com.mysql.cj.api.x.CreateTableStatement.CreateTableLikeStatement;
import com.mysql.cj.api.x.CreateTableStatement.CreateTableSplitStatement;
import com.mysql.cj.api.x.ForeignKeyDefinition;
import com.mysql.cj.api.x.Schema;
import com.mysql.cj.api.x.SelectStatement;
import com.mysql.cj.api.x.Table;
import com.mysql.cj.core.exceptions.FeatureNotAvailableException;
import com.mysql.cj.core.exceptions.MysqlErrorNumbers;
import com.mysql.cj.mysqlx.MysqlxError;

public class CreateTableStatementImpl implements CreateTableSplitStatement, CreateTableFullStatement, CreateTableLikeStatement {

    private Schema schema;
    private String table;
    private String likeTable;
    private boolean reuseExistingObject = false;
    private List<ColumnDefinition<?>> columns = new ArrayList<>();
    private List<String> primaryKeys = new ArrayList<>();
    private Map<String, String[]> indexes = new HashMap<>();
    private Map<String, String[]> uniqueIndexes = new HashMap<>();
    private Map<String, ForeignKeyDefinition> foreignKeys = new HashMap<>();
    private Number initialAutoIncrement;
    private String charset;
    private String collation;
    private String comment;
    private boolean temporary = false;
    private String as;

    public CreateTableStatementImpl(Schema sch, String tableName) {
        this.schema = sch;
        this.table = tableName;
    }

    public CreateTableStatementImpl(Schema sch, String tableName, boolean reuseExistingObject) {
        this.schema = sch;
        this.table = tableName;
        this.reuseExistingObject = reuseExistingObject;
    }

    @Override
    public CreateTableLikeStatement like(String templateTableName) {
        this.likeTable = templateTableName;
        return this;
    }

    @Override
    public CreateTableFullStatement addColumn(ColumnDefinition<?> colDef) {
        this.columns.add(colDef);
        return this;
    }

    @Override
    public CreateTableFullStatement addPrimaryKey(String... pk) {
        this.primaryKeys.addAll(Arrays.asList(pk));
        return this;
    }

    @Override
    public CreateTableFullStatement addIndex(String name, String... column) {
        this.indexes.put(name, column);
        return this;
    }

    @Override
    public CreateTableFullStatement addUniqueIndex(String name, String... column) {
        this.uniqueIndexes.put(name, column);
        return this;
    }

    @Override
    public CreateTableFullStatement addForeignKey(String fkName, ForeignKeyDefinition fkSpec) {
        this.foreignKeys.put(fkName, fkSpec.setName(fkName));
        return this;
    }

    @Override
    public CreateTableFullStatement setInitialAutoIncrement(Number val) {
        this.initialAutoIncrement = val;
        return this;
    }

    @Override
    public CreateTableFullStatement setDefaultCharset(String charsetName) {
        this.charset = charsetName;
        return this;
    }

    @Override
    public CreateTableFullStatement setDefaultCollation(String collationName) {
        this.collation = collationName;
        return this;
    }

    @Override
    public CreateTableFullStatement setComment(String cmt) {
        this.comment = cmt;
        return this;
    }

    @Override
    public CreateTableFullStatement temporary() {
        this.temporary = true;
        return this;
    }

    @Override
    public CreateTableFullStatement as(String select) {
        this.as = select;
        return this;
    }

    @Override
    public CreateTableFullStatement as(SelectStatement select) {
        // TODO implement when the native X Protocol support for createTable() is available
        throw new FeatureNotAvailableException("Not supported");
        //return this;
    }

    @Override
    public Table execute() {
        try {
            // TODO implement using specific X Protocol message when available
            this.schema.getSession().getMysqlxSession().executeSql(toString(), null);
        } catch (MysqlxError ex) {
            if (ex.getErrorCode() != MysqlErrorNumbers.ER_TABLE_EXISTS_ERROR || !this.reuseExistingObject) {
                throw ex;
            }
        }
        return this.schema.getTable(this.table);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("CREATE");
        if (this.temporary) {
            sb.append(" TEMPORARY");
        }
        sb.append(" TABLE");
        if (this.reuseExistingObject) {
            sb.append(" IF NOT EXISTS");
        }
        sb.append(" ").append(this.table);

        if (this.likeTable != null) {
            sb.append(" LIKE ").append(this.likeTable);
            return sb.toString();
        }

        // (create_definition,...)
        if (this.columns.size() > 0) {
            // col_name column_definition
            sb.append(this.columns.stream().map(i -> i.toString()).collect(Collectors.joining(",\n ", " (\n ", "")));

            // | [CONSTRAINT [symbol]] PRIMARY KEY [index_type] (index_col_name,...) [index_option] ...
            if (this.primaryKeys.size() > 0) {
                sb.append(this.primaryKeys.stream().collect(Collectors.joining(", ", ",\n PRIMARY KEY (", ")")));
            }

            // | {INDEX|KEY} [index_name] [index_type] (index_col_name,...) [index_option] ...
            String[] keys = this.indexes.keySet().toArray(new String[] {});
            for (int i = 0; i < keys.length; i++) {
                sb.append(",\n INDEX ").append(keys[i]);
                sb.append(Arrays.stream(this.indexes.get(keys[i])).collect(Collectors.joining(", ", " (", ")")));
            }

            // | [CONSTRAINT [symbol]] UNIQUE [INDEX|KEY] [index_name] [index_type] (index_col_name,...) [index_option] ...
            keys = this.uniqueIndexes.keySet().toArray(new String[] {});
            for (int i = 0; i < keys.length; i++) {
                sb.append(",\n UNIQUE INDEX ").append(keys[i]);
                sb.append(Arrays.stream(this.uniqueIndexes.get(keys[i])).collect(Collectors.joining(", ", " (", ")")));
            }

            // | {FULLTEXT|SPATIAL} [INDEX|KEY] [index_name] (index_col_name,...) [index_option] ...

            // | [CONSTRAINT [symbol]] FOREIGN KEY [index_name] (index_col_name,...) reference_definition
            if (this.foreignKeys.size() > 0) {
                sb.append(this.foreignKeys.values().stream().map(i -> i.toString()).collect(Collectors.joining(",\n ", ",\n ", "")));
            }
            sb.append(")");

        }

        if (this.initialAutoIncrement != null) {
            sb.append("\n AUTO_INCREMENT = ").append(this.initialAutoIncrement.longValue());
        }

        if (this.charset != null && !this.charset.isEmpty()) {
            sb.append("\n DEFAULT CHARACTER SET = ").append(this.charset);
        }
        if (this.collation != null && !this.collation.isEmpty()) {
            sb.append("\n DEFAULT COLLATE = ").append(this.collation);
        }
        if (this.comment != null && !this.comment.isEmpty()) {
            sb.append("\n COMMENT '").append(this.comment).append("'");
        }

        // TODO [IGNORE | REPLACE]
        //sb.append("IGNORE | REPLACE");

        if (this.as != null && !this.as.isEmpty()) {
            sb.append("\n AS ").append(this.as);
        }

        return sb.toString();
    }

}
