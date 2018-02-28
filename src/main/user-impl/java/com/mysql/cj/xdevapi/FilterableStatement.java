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
     * 
     * @return true if relational
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
