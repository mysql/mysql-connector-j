/*
 * Copyright (c) 2015, 2020, Oracle and/or its affiliates.
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
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import com.mysql.cj.exceptions.WrongArgumentException;
import com.mysql.cj.x.protobuf.MysqlxCrud.Collection;
import com.mysql.cj.x.protobuf.MysqlxCrud.Order;
import com.mysql.cj.x.protobuf.MysqlxCrud.Projection;
import com.mysql.cj.x.protobuf.MysqlxDatatypes.Scalar;
import com.mysql.cj.x.protobuf.MysqlxExpr.Expr;

/**
 * Abstract implementation of {@link FilterParams}.
 */
public abstract class AbstractFilterParams implements FilterParams {
    protected Collection collection;
    protected Long limit;
    protected Long offset;
    protected boolean supportsOffset;
    protected String[] orderExpr;
    private List<Order> order;
    protected String criteriaStr;
    private Expr criteria;
    protected Scalar[] args;
    private Map<String, Integer> placeholderNameToPosition;
    protected boolean isRelational;

    protected String[] groupBy;
    private List<Expr> grouping;
    String having;
    private Expr groupingCriteria;
    protected String[] projection;
    protected List<Projection> fields;
    protected RowLock lock;
    protected RowLockOptions lockOption;

    /**
     * Constructor.
     * 
     * @param schemaName
     *            Schema name
     * @param collectionName
     *            Collection name
     * @param supportsOffset
     *            Whether <i>offset</i> is supported or not
     * @param isRelational
     *            Are relational columns identifiers allowed?
     */
    public AbstractFilterParams(String schemaName, String collectionName, boolean supportsOffset, boolean isRelational) {
        this.collection = ExprUtil.buildCollection(schemaName, collectionName);
        this.supportsOffset = supportsOffset;
        this.isRelational = isRelational;
    }

    public Object getCollection() {
        return this.collection;
    }

    public Object getOrder() {
        // type is reserved as hidden knowledge, don't expose protobuf internals
        return this.order;
    }

    public void setOrder(String... orderExpression) {
        this.orderExpr = orderExpression;
        // TODO: does this support placeholders? how do we prevent it?
        this.order = new ExprParser(Arrays.stream(orderExpression).collect(Collectors.joining(", ")), this.isRelational).parseOrderSpec();
    }

    public Long getLimit() {
        return this.limit;
    }

    public void setLimit(Long limit) {
        this.limit = limit;
    }

    public Long getOffset() {
        return this.offset;
    }

    public void setOffset(Long offset) {
        this.offset = offset;
    }

    public boolean supportsOffset() {
        return this.supportsOffset;
    }

    public Object getCriteria() {
        return this.criteria;
    }

    public void setCriteria(String criteriaString) {
        this.criteriaStr = criteriaString;
        ExprParser parser = new ExprParser(criteriaString, this.isRelational);
        this.criteria = parser.parse();
        if (parser.getPositionalPlaceholderCount() > 0) {
            this.placeholderNameToPosition = parser.getPlaceholderNameToPositionMap();
            this.args = new Scalar[parser.getPositionalPlaceholderCount()];
        }
    }

    public Object getArgs() {
        if (this.args == null) {
            return null;
        }
        return Arrays.asList(this.args);
    }

    public void addArg(String name, Object value) {
        if (this.args == null) {
            throw new WrongArgumentException("No placeholders");
        }
        if (this.placeholderNameToPosition.get(name) == null) {
            throw new WrongArgumentException("Unknown placeholder: " + name);
        }
        this.args[this.placeholderNameToPosition.get(name)] = ExprUtil.argObjectToScalar(value);
    }

    public void verifyAllArgsBound() {
        if (this.args != null) {
            IntStream.range(0, this.args.length)
                    // find unbound params
                    .filter(i -> this.args[i] == null)
                    // get the parameter name from the map
                    .mapToObj(i -> this.placeholderNameToPosition.entrySet().stream().filter(e -> e.getValue() == i).map(Map.Entry::getKey).findFirst().get())
                    .forEach(name -> {
                        throw new WrongArgumentException("Placeholder '" + name + "' is not bound");
                    });
        }
    }

    public void clearArgs() {
        if (this.args != null) {
            IntStream.range(0, this.args.length).forEach(i -> this.args[i] = null);
        }
    }

    public boolean isRelational() {
        return this.isRelational;
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
}
