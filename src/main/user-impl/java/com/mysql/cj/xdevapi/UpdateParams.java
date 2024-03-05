/*
 * Copyright (c) 2015, 2024, Oracle and/or its affiliates.
 *
 * This program is free software; you can redistribute it and/or modify it under the terms of the GNU General Public License, version 2.0, as published by
 * the Free Software Foundation.
 *
 * This program is designed to work with certain software that is licensed under separate terms, as designated in a particular file or component or in
 * included license documentation. The authors of MySQL hereby grant you an additional permission to link the program and your derivative works with the
 * separately licensed software that they have either included with the program or referenced in the documentation.
 *
 * Without limiting anything contained in the foregoing, this file, which is part of MySQL Connector/J, is also subject to the Universal FOSS Exception,
 * version 1.0, a copy of which can be found at http://oss.oracle.com/licenses/universal-foss-exception.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License, version 2.0, for more details.
 *
 * You should have received a copy of the GNU General Public License along with this program; if not, write to the Free Software Foundation, Inc.,
 * 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA
 */

package com.mysql.cj.xdevapi;

import java.util.HashMap;
import java.util.Map;

import com.mysql.cj.x.protobuf.MysqlxExpr.ColumnIdentifier;
import com.mysql.cj.x.protobuf.MysqlxExpr.Expr;

/**
 * Class collecting parameters for {@link Table#update()}.
 */
public class UpdateParams {

    private Map<ColumnIdentifier, Expr> updateOps = new HashMap<>();

    /**
     * Fill update parameters from field -&gt; value_expression map.
     *
     * @param updates
     *            field -&gt; value_expression map
     */
    public void setUpdates(Map<String, Object> updates) {
        updates.entrySet().forEach(e -> addUpdate(e.getKey(), e.getValue()));
    }

    /**
     * Add update parameter.
     *
     * @param path
     *            field name
     * @param value
     *            value expression
     */
    public void addUpdate(String path, Object value) {
        this.updateOps.put(new ExprParser(path, true).parseTableUpdateField(), ExprUtil.argObjectToExpr(value, true));
    }

    /**
     * Get update parameters map.
     *
     * @return X Protocol ColumnIdentifier-&gt;Expr map.
     */
    public Object getUpdates() {
        return this.updateOps;
    }

}
