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

package com.mysql.cj.mysqlx.devapi;

import com.mysql.cj.api.x.Statement;
import com.mysql.cj.mysqlx.FilterParams;

/**
 * @todo
 */
public abstract class FilterableStatement<STMT_T, RES_T> implements Statement<STMT_T, RES_T> {
    protected FilterParams filterParams;

    public FilterableStatement(FilterParams filterParams) {
        this.filterParams = filterParams;
    }

    public FilterableStatement(String schemaName, String collectionName, boolean isRelational) {
        this.filterParams = new FilterParams(schemaName, collectionName, isRelational);
    }

    @SuppressWarnings("unchecked")
    public STMT_T where(String searchCondition) {
        this.filterParams.setCriteria(searchCondition);
        return (STMT_T) this;
    }

    public STMT_T sort(String... sortFields) {
        return orderBy(sortFields);
    }

    @SuppressWarnings("unchecked")
    public STMT_T orderBy(String... sortFields) {
        this.filterParams.setOrder(sortFields);
        return (STMT_T) this;
    }

    @SuppressWarnings("unchecked")
    public STMT_T limit(long numberOfRows) {
        this.filterParams.setLimit(numberOfRows);
        return (STMT_T) this;
    }

    public STMT_T skip(long limitOffset) {
        return offset(limitOffset);
    }

    @SuppressWarnings("unchecked")
    public STMT_T offset(long limitOffset) {
        this.filterParams.setOffset(limitOffset);
        return (STMT_T) this;
    }

    /**
     * Is this a relational statement?
     */
    public boolean isRelational() {
        return this.filterParams.isRelational();
    }

    @SuppressWarnings("unchecked")
    public STMT_T clearBindings() {
        this.filterParams.clearArgs();
        return (STMT_T) this;
    }

    @SuppressWarnings("unchecked")
    public STMT_T bind(String argName, Object value) {
        this.filterParams.addArg(argName, value);
        return (STMT_T) this;
    }
}
