/*
 * Copyright (c) 2015, 2022, Oracle and/or its affiliates.
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

import com.mysql.cj.Messages;
import com.mysql.cj.x.protobuf.MysqlxCrud.UpdateOperation;
import com.mysql.cj.x.protobuf.MysqlxExpr.ColumnIdentifier;
import com.mysql.cj.x.protobuf.MysqlxExpr.Expr;

/**
 * Representation of a single update operation in a list of operations to be performed by {@link ModifyStatement}.
 * Used internally for transformation of X DevAPI parameters into X Protocol ones.
 */
public class UpdateSpec {

    private UpdateOperation.UpdateType updateType;
    private ColumnIdentifier source;
    private Expr value;

    /**
     * Constructor.
     * 
     * @param updateType
     *            update operation type
     */
    public UpdateSpec(UpdateType updateType) {
        this.updateType = UpdateOperation.UpdateType.valueOf(updateType.name());
        this.source = ColumnIdentifier.getDefaultInstance();
    }

    /**
     * Constructor.
     * 
     * @param updateType
     *            update operation type
     * @param source
     *            document path expression
     */
    public UpdateSpec(UpdateType updateType, String source) {
        this.updateType = UpdateOperation.UpdateType.valueOf(updateType.name());
        if (source == null || source.trim().isEmpty()) {
            throw new XDevAPIError(Messages.getString("ModifyStatement.0", new String[] { "docPath" }));
        }
        // accommodate parser's documentField() handling by removing "$"
        if (source.length() > 0 && source.charAt(0) == '$') {
            source = source.substring(1);
        }
        this.source = new ExprParser(source, false).documentField().getIdentifier();
    }

    /**
     * Get X Protocol update type.
     * 
     * @return X Protocol UpdateOperation.UpdateType
     */
    public Object getUpdateType() {
        return this.updateType;
    }

    /**
     * Get X Protocol ColumnIdentifier.
     * 
     * @return X Protocol MysqlxExpr.ColumnIdentifier
     */
    public Object getSource() {
        return this.source;
    }

    /**
     * Set value to be set by this update operation.
     * 
     * @param value
     *            value expression
     * @return this UpdateSpec
     */
    public UpdateSpec setValue(Object value) {
        this.value = ExprUtil.argObjectToExpr(value, false);
        return this;
    }

    /**
     * Get X Protocol value expression.
     * 
     * @return X Protocol MysqlxExpr.Expr
     */
    public Object getValue() {
        return this.value;
    }
}
