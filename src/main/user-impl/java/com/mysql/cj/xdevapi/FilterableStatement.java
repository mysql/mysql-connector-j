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

/**
 * Abstract class, common to several X DevAPI statement classes.
 *
 * @param <STMT_T>
 *            statement interface
 * @param <RES_T>
 *            result interface
 */
public abstract class FilterableStatement<STMT_T, RES_T> extends PreparableStatement<RES_T> implements Statement<STMT_T, RES_T> {

    protected FilterParams filterParams;

    /**
     * Constructor.
     *
     * @param filterParams
     *            {@link FilterParams} object.
     */
    public FilterableStatement(FilterParams filterParams) {
        this.filterParams = filterParams;
    }

    /**
     * Add search condition to this statement.
     *
     * <pre>
     * table.delete().where("age == 13").execute();
     * </pre>
     *
     * @param searchCondition
     *            expression
     * @return this statement
     */
    @SuppressWarnings("unchecked")
    public STMT_T where(String searchCondition) {
        resetPrepareState();
        this.filterParams.setCriteria(searchCondition);
        return (STMT_T) this;
    }

    /**
     * Add sort expressions to this statement. Synonym to {@link #orderBy(String...)}.
     *
     * <pre>
     * DocResult docs = this.collection.find().orderBy("$._id").execute();
     * docs = this.collection.find().sort("$.x", "$.y").execute();
     * </pre>
     *
     * @param sortFields
     *            sort expressions
     * @return this statement
     */
    public STMT_T sort(String... sortFields) {
        return orderBy(sortFields);
    }

    /**
     * Add sort expressions to this statement.
     *
     * <pre>
     * DocResult docs = this.collection.find().orderBy("$._id").execute();
     * docs = this.collection.find().sort("$.x", "$.y").execute();
     * </pre>
     *
     * @param sortFields
     *            sort expressions
     * @return this statement
     */
    @SuppressWarnings({ "unchecked" })
    public STMT_T orderBy(String... sortFields) {
        resetPrepareState();
        this.filterParams.setOrder(sortFields);
        return (STMT_T) this;
    }

    /**
     * Add row limit to this statement.
     *
     * <p>
     * For example, to find only 3 rows:
     * </p>
     *
     * <pre>
     * docs = this.collection.find().orderBy("$._id").limit(3).execute();
     * </pre>
     *
     * @param numberOfRows
     *            maximum rows to process
     * @return this statement
     */
    @SuppressWarnings("unchecked")
    public STMT_T limit(long numberOfRows) {
        if (this.filterParams.getLimit() == null) {
            setReprepareState();
        }
        this.filterParams.setLimit(numberOfRows);
        return (STMT_T) this;
    }

    /**
     * Add maximum number of rows to skip before find others.
     *
     * <p>
     * For example, to skip 2 rows:
     * </p>
     *
     * <pre>
     * docs = this.collection.find().orderBy("$._id").offset(2).execute();
     * </pre>
     *
     * @param limitOffset
     *            number of rows to skip
     * @return this statement
     */
    @SuppressWarnings("unchecked")
    public STMT_T offset(long limitOffset) {
        // Offset depends on Limit. There is no need to re-prepare the statement even if OFFSET was never set before.
        this.filterParams.setOffset(limitOffset);
        return (STMT_T) this;
    }

    /**
     * Are relational columns identifiers allowed in this statement?
     *
     * @return true if allowed
     */
    public boolean isRelational() {
        return this.filterParams.isRelational();
    }

    @Override
    @SuppressWarnings("unchecked")
    public STMT_T clearBindings() {
        this.filterParams.clearArgs();
        return (STMT_T) this;
    }

    @Override
    @SuppressWarnings("unchecked")
    public STMT_T bind(String argName, Object value) {
        this.filterParams.addArg(argName, value);
        return (STMT_T) this;
    }

}
