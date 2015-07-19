/*
  Copyright (c) 2015, Oracle and/or its affiliates. All rights reserved.

  The MySQL Connector/J is licensed under the terms of the GPLv2
  <http://www.gnu.org/licenses/old-licenses/gpl-2.0.html>, like most MySQL Connectors.
  There are special exceptions to the terms and conditions of the GPLv2 as it is applied to
  this software, see the FLOSS License Exception
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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.mysql.cj.core.exceptions.WrongArgumentException;
import com.mysql.cj.mysqlx.ExprParser;
import com.mysql.cj.mysqlx.ExprUtil;
import com.mysql.cj.mysqlx.protobuf.MysqlxCrud.Order;
import com.mysql.cj.mysqlx.protobuf.MysqlxDatatypes.Any;
import com.mysql.cj.mysqlx.protobuf.MysqlxExpr.Expr;

/**
 * Filter parameters.
 * @todo better documentation
 */
public class FilterParams {
    // TODO: private Expr[] order? or straight to MysqlxCrud.Order? 
    private Long limit;
    private Long offset;
    private List<Order> order;
    private Expr criteria;
    private List<Any> args;
    private Map<String, Integer> placeholderNameToPosition;

    public FilterParams() {
    }

    public FilterParams(String criteriaString) {
        setCriteria(criteriaString);
    }

    public Object getOrder() {
        // type is reserved as hidden knowledge, don't expose protobuf internals
        return this.order;
    }

    public void setOrder(String orderExpression) {
        // TODO: does this support placeholders? how do we prevent it?
        this.order = new ExprParser(orderExpression).parseOrderSpec();
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
        ExprParser parser = new ExprParser(criteriaString);
        this.criteria = parser.parse();
        if (parser.getPositionalPlaceholderCount() > 0) {
            this.placeholderNameToPosition = parser.getPlaceholderNameToPositionMap();
            this.args = new ArrayList<>(parser.getPositionalPlaceholderCount());
        }
    }

    public void addArg(String name, Object value) {
        if (this.args == null) {
            throw new WrongArgumentException("No placeholders");
        }
        if (this.placeholderNameToPosition.get(name) == null) {
            throw new WrongArgumentException("Unknown placeholder :" + name);
        }
        this.args.add(this.placeholderNameToPosition.get(name), ExprUtil.argObjectToAny(value));
    }

    public void clearArgs() {
        this.args = null;
    }
}
