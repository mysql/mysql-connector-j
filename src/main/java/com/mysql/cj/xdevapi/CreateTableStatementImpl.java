/*
  Copyright (c) 2016, 2017, Oracle and/or its affiliates. All rights reserved.

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

package com.mysql.cj.xdevapi;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.mysql.cj.api.xdevapi.ColumnDefinition;
import com.mysql.cj.api.xdevapi.CreateTableStatement.CreateTableFullStatement;
import com.mysql.cj.api.xdevapi.CreateTableStatement.CreateTableLikeStatement;
import com.mysql.cj.api.xdevapi.CreateTableStatement.CreateTableSplitStatement;
import com.mysql.cj.api.xdevapi.ForeignKeyDefinition;
import com.mysql.cj.api.xdevapi.Schema;
import com.mysql.cj.api.xdevapi.SelectStatement;
import com.mysql.cj.api.xdevapi.Table;
import com.mysql.cj.core.Messages;
import com.mysql.cj.core.exceptions.FeatureNotAvailableException;
import com.mysql.cj.core.exceptions.MysqlErrorNumbers;
import com.mysql.cj.x.core.MysqlxSession;
import com.mysql.cj.x.core.XDevAPIError;

public class CreateTableStatementImpl implements CreateTableSplitStatement, CreateTableFullStatement, CreateTableLikeStatement {

    private MysqlxSession mysqlxSession;
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

    public CreateTableStatementImpl(MysqlxSession mysqlxSession, Schema sch, String tableName) {
        if (mysqlxSession == null) {
            throw new XDevAPIError(Messages.getString("CreateTableStatement.0", new String[] { "mysqlxSession" }));
        }
        if (sch == null) {
            throw new XDevAPIError(Messages.getString("CreateTableStatement.0", new String[] { "sch" }));
        }
        if (tableName == null) {
            throw new XDevAPIError(Messages.getString("CreateTableStatement.0", new String[] { "tableName" }));
        }
        this.mysqlxSession = mysqlxSession;
        this.schema = sch;
        this.table = tableName;
    }

    public CreateTableStatementImpl(MysqlxSession mysqlxSession, Schema sch, String tableName, boolean reuseExistingObject) {
        if (mysqlxSession == null) {
            throw new XDevAPIError(Messages.getString("CreateTableStatement.0", new String[] { "mysqlxSession" }));
        }
        if (sch == null) {
            throw new XDevAPIError(Messages.getString("CreateTableStatement.0", new String[] { "sch" }));
        }
        if (tableName == null) {
            throw new XDevAPIError(Messages.getString("CreateTableStatement.0", new String[] { "tableName" }));
        }
        this.mysqlxSession = mysqlxSession;
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
        if (colDef == null) {
            throw new XDevAPIError(Messages.getString("CreateTableStatement.0", new String[] { "colDef" }));
        }
        this.columns.add(colDef);
        return this;
    }

    @Override
    public CreateTableFullStatement addPrimaryKey(String... pk) {
        if (pk == null) {
            throw new XDevAPIError(Messages.getString("CreateTableStatement.0", new String[] { "pk" }));
        }
        for (String c : pk) {
            if (c == null) {
                throw new XDevAPIError(Messages.getString("CreateTableStatement.1", new String[] { "pk" }));
            }
            this.primaryKeys.add(c);
        }
        return this;
    }

    @Override
    public CreateTableFullStatement addIndex(String name, String... column) {
        if (name == null) {
            throw new XDevAPIError(Messages.getString("CreateTableStatement.0", new String[] { "name" }));
        }
        if (column == null) {
            throw new XDevAPIError(Messages.getString("CreateTableStatement.0", new String[] { "column" }));
        }
        for (String c : column) {
            if (c == null) {
                throw new XDevAPIError(Messages.getString("CreateTableStatement.1", new String[] { "column" }));
            }
        }
        this.indexes.put(name, column);
        return this;
    }

    @Override
    public CreateTableFullStatement addUniqueIndex(String name, String... column) {
        if (name == null) {
            throw new XDevAPIError(Messages.getString("CreateTableStatement.0", new String[] { "name" }));
        }
        if (column == null) {
            throw new XDevAPIError(Messages.getString("CreateTableStatement.0", new String[] { "column" }));
        }
        for (String c : column) {
            if (c == null) {
                throw new XDevAPIError(Messages.getString("CreateTableStatement.1", new String[] { "column" }));
            }
        }
        this.uniqueIndexes.put(name, column);
        return this;
    }

    @Override
    public CreateTableFullStatement addForeignKey(String fkName, ForeignKeyDefinition fkSpec) {
        if (fkName == null) {
            throw new XDevAPIError(Messages.getString("CreateTableStatement.0", new String[] { "fkName" }));
        }
        if (fkSpec == null) {
            throw new XDevAPIError(Messages.getString("CreateTableStatement.0", new String[] { "fkSpec" }));
        }
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
            this.mysqlxSession.executeSql(toString(), null);
        } catch (XDevAPIError ex) {
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
