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

import java.util.Arrays;
import java.util.stream.Collectors;

import com.mysql.cj.api.x.ForeignKeyDefinition;

public class ForeignKeyDef implements ForeignKeyDefinition {

    private String name;
    protected String[] columns;
    protected String toTable;
    protected String[] toColumns;
    protected ChangeMode onDelete = ChangeMode.RESTRICT;
    protected ChangeMode onUpdate = ChangeMode.RESTRICT;

    @Override
    public ForeignKeyDefinition setName(String fkName) {
        this.name = fkName;
        return this;
    }

    @Override
    public ForeignKeyDefinition fields(String... column) {
        this.columns = column;
        return this;
    }

    @Override
    public ForeignKeyDefinition refersTo(String table, String... column) {
        this.toTable = table;
        this.toColumns = column;
        return this;
    }

    @Override
    public ForeignKeyDefinition onDelete(ChangeMode mode) {
        this.onDelete = mode;
        return this;
    }

    @Override
    public ForeignKeyDefinition onUpdate(ChangeMode mode) {
        this.onUpdate = mode;
        return this;
    }

    /**
     * [CONSTRAINT [symbol]] FOREIGN KEY
     * [index_name] (index_col_name, ...)
     * REFERENCES tbl_name (index_col_name,...)
     * [ON DELETE reference_option]
     * [ON UPDATE reference_option]
     */
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("FOREIGN KEY");

        sb.append(" ").append(this.name);
        sb.append(Arrays.stream(this.columns).collect(Collectors.joining(", ", " (", ")")));

        sb.append(" REFERENCES ").append(this.toTable);
        sb.append(Arrays.stream(this.toColumns).collect(Collectors.joining(", ", " (", ")")));

        if (this.onDelete != ChangeMode.RESTRICT) {
            sb.append(" ON DELETE ").append(this.onDelete.getExpr());
        }
        if (this.onUpdate != ChangeMode.RESTRICT) {
            sb.append(" ON UPDATE ").append(this.onUpdate.getExpr());
        }

        return sb.toString();
    }
}
