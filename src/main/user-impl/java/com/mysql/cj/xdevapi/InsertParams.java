/*
 * Copyright (c) 2015, 2023, Oracle and/or its affiliates.
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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.mysql.cj.x.protobuf.MysqlxCrud.Column;
import com.mysql.cj.x.protobuf.MysqlxCrud.Insert.TypedRow;

/**
 * Helper class for collecting parameters for relational insert command.
 */
public class InsertParams {

    private List<Column> projection;
    private List<TypedRow> rows = new LinkedList<>();

    /**
     * Set X Protocol Column objects list for projection.
     *
     * @param projection
     *            projection expressions
     */
    public void setProjection(String[] projection) {
        this.projection = Arrays.stream(projection).map(p -> new ExprParser(p, true).parseTableInsertField()).collect(Collectors.toList());
    }

    /**
     * Get X Protocol Column objects list for projection.
     *
     * @return X Protocol Column objects list
     */
    public Object getProjection() {
        return this.projection;
    }

    /**
     * Add new X Protocol row.
     *
     * @param row
     *            field value expressions for this row
     */
    public void addRow(List<Object> row) {
        this.rows.add(TypedRow.newBuilder().addAllField(row.stream().map(f -> ExprUtil.argObjectToExpr(f, true)).collect(Collectors.toList())).build());
    }

    /**
     * Get X Protocol rows list.
     *
     * @return X Protocol rows list
     */
    public Object getRows() {
        return this.rows;
    }

    /**
     * Fill insert parameters from projection_expression -&gt; value_expression map.
     *
     * @param fieldsAndValues
     *            projection_expression -&gt; value_expression map
     */
    public void setFieldsAndValues(Map<String, Object> fieldsAndValues) {
        this.projection = new ArrayList<>();
        TypedRow.Builder rowBuilder = TypedRow.newBuilder();
        fieldsAndValues.entrySet().stream().forEach(e -> {
            this.projection.add(new ExprParser(e.getKey(), true).parseTableInsertField());
            rowBuilder.addField(ExprUtil.argObjectToExpr(e.getValue(), true));
        });
        this.rows.add(rowBuilder.build());
    }

}
