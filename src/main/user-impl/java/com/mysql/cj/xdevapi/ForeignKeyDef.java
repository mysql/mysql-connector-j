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

import java.util.Arrays;
import java.util.stream.Collectors;

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

        if (this.columns == null || this.columns.length == 0) {
            throw new XDevAPIError("ForeignKeyDefinition is incomplete, fields are empty.");
        }
        sb.append(Arrays.stream(this.columns).collect(Collectors.joining(", ", " (", ")")));

        if (this.toTable == null) {
            throw new XDevAPIError("ForeignKeyDefinition is incomplete, to-table isn't set.");
        }
        sb.append(" REFERENCES ").append(this.toTable);

        if (this.toColumns == null || this.toColumns.length == 0) {
            throw new XDevAPIError("ForeignKeyDefinition is incomplete, to-columns are empty.");
        }
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
