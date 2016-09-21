/*
  Copyright (c) 2015, 2016, Oracle and/or its affiliates. All rights reserved.

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

package com.mysql.cj.mysqlx;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import com.mysql.cj.core.exceptions.WrongArgumentException;
import com.mysql.cj.mysqlx.protobuf.MysqlxCrud.Collection;
import com.mysql.cj.mysqlx.protobuf.MysqlxCrud.Order;
import com.mysql.cj.mysqlx.protobuf.MysqlxDatatypes.Scalar;
import com.mysql.cj.mysqlx.protobuf.MysqlxExpr.Expr;

/**
 * Filter parameters.
 * 
 * @todo better documentation
 */
public class FilterParams {
    private Collection collection;
    private Long limit;
    private Long offset;
    private List<Order> order;
    private Expr criteria;
    private Scalar[] args;
    private Map<String, Integer> placeholderNameToPosition;
    protected boolean isRelational;

    public FilterParams(String schemaName, String collectionName, boolean isRelational) {
        this.collection = ExprUtil.buildCollection(schemaName, collectionName);
        this.isRelational = isRelational;
    }

    public FilterParams(String schemaName, String collectionName, String criteriaString, boolean isRelational) {
        this.collection = ExprUtil.buildCollection(schemaName, collectionName);
        this.isRelational = isRelational;
        setCriteria(criteriaString);
    }

    public Object getCollection() {
        return this.collection;
    }

    public Object getOrder() {
        // type is reserved as hidden knowledge, don't expose protobuf internals
        return this.order;
    }

    public void setOrder(String... orderExpression) {
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

    public Object getCriteria() {
        return this.criteria;
    }

    public void setCriteria(String criteriaString) {
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

    /**
     * Verify that all arguments are bound.
     *
     * @throws WrongArgumentException
     *             if any placeholder argument is not bound
     */
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
}
