/*
 * Copyright (c) 2015, 2018, Oracle and/or its affiliates. All rights reserved.
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
import java.util.List;
import java.util.stream.Collectors;

import com.mysql.cj.x.protobuf.MysqlxCrud.Collection;
import com.mysql.cj.x.protobuf.MysqlxCrud.Projection;
import com.mysql.cj.x.protobuf.MysqlxExpr.Expr;

public abstract class AbstractFindParams extends FilterParams implements FindParams {
    protected String[] groupBy;
    private List<Expr> grouping;
    String having;
    private Expr groupingCriteria;
    protected String[] projection;
    protected List<Projection> fields;
    protected RowLock lock;
    protected RowLockOptions lockOption;

    public AbstractFindParams(String schemaName, String collectionName, boolean isRelational) {
        super(schemaName, collectionName, isRelational);
    }

    public AbstractFindParams(String schemaName, String collectionName, String criteriaString, boolean isRelational) {
        super(schemaName, collectionName, criteriaString, isRelational);
    }

    protected AbstractFindParams(Collection coll, boolean isRelational) {
        super(coll, isRelational);
    }

    public abstract void setFields(String... projection);

    public Object getFields() {
        return this.fields;
    }

    public void setGrouping(String... groupBy) {
        this.groupBy = groupBy;
        this.grouping = new ExprParser(Arrays.stream(groupBy).collect(Collectors.joining(", ")), isRelational()).parseExprList();
    }

    public Object getGrouping() {
        return this.grouping;
    }

    public void setGroupingCriteria(String having) {
        this.having = having;
        this.groupingCriteria = new ExprParser(having, isRelational()).parse();
    }

    public Object getGroupingCriteria() {
        return this.groupingCriteria;
    }

    public RowLock getLock() {
        return this.lock;
    }

    public void setLock(RowLock rowLock) {
        this.lock = rowLock;
    }

    public RowLockOptions getLockOption() {
        return this.lockOption;
    }

    public void setLockOption(RowLockOptions lockOption) {
        this.lockOption = lockOption;
    }

    @Override
    protected abstract FindParams clone();
}
