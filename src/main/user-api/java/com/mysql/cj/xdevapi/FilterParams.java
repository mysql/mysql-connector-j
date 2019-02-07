/*
 * Copyright (c) 2018, 2019, Oracle and/or its affiliates. All rights reserved.
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

import com.mysql.cj.exceptions.WrongArgumentException;

/**
 * Transforms X DevAPI filter parameters into X Protocol message entities.
 * Used internally.
 */
public interface FilterParams {

    /**
     * The type of row lock.
     */
    public enum RowLock {
        /**
         * Lock matching rows against updates.
         */
        SHARED_LOCK(1),
        /**
         * Lock matching rows so no other transactions can read or write to it.
         */
        EXCLUSIVE_LOCK(2);

        private int rowLock;

        private RowLock(int rowLock) {
            this.rowLock = rowLock;
        }

        /**
         * Get the row lock type id.
         * 
         * @return row lock type id
         */
        public int asNumber() {
            return rowLock;
        }
    }

    /**
     * Options that define the behavior while retrieving locked rows.
     */
    public enum RowLockOptions {
        /**
         * Do not wait to acquire row lock, fail with an error if a requested row is locked.
         */
        NOWAIT(1),
        /**
         * Do not wait to acquire a row lock, remove locked rows from the result set.
         */
        SKIP_LOCKED(2);

        private int rowLockOption;

        private RowLockOptions(int rowLockOption) {
            this.rowLockOption = rowLockOption;
        }

        /**
         * Get the row lock option id.
         * 
         * @return row lock option id
         */
        public int asNumber() {
            return rowLockOption;
        }
    }

    /**
     * Get X Protocol Collection object.
     * 
     * @return X Protocol Collection object
     */
    Object getCollection();

    /**
     * Get X Protocol Order objects.
     * 
     * @return List of X Protocol Order objects
     */
    Object getOrder();

    /**
     * Parse order expressions into X Protocol Order objects.
     * 
     * <pre>
     * DocResult docs = this.collection.find().orderBy("$._id").execute();
     * docs = this.collection.find().sort("$.x", "$.y").execute();
     * </pre>
     * 
     * @param orderExpression
     *            order expressions
     * 
     */
    void setOrder(String... orderExpression);

    /**
     * Get max number of rows to filter.
     * 
     * @return limit
     */
    Long getLimit();

    /**
     * Set maximum rows to find.
     * <p>
     * For example, to find the 3 first rows:
     * </p>
     * 
     * <pre>
     * docs = this.collection.find().orderBy("$._id").limit(3).execute();
     * </pre>
     * 
     * @param limit
     *            maximum rows to find
     */
    void setLimit(Long limit);

    /**
     * Get number of rows to skip before finding others.
     * 
     * @return maximum rows to skip
     */
    Long getOffset();

    /**
     * Set number of rows to skip before finding others.
     * <p>
     * For example, to skip 1 row and find other 3 rows:
     * </p>
     * 
     * <pre>
     * docs = this.collection.find().orderBy("$._id").limit(3).skip(1).execute();
     * </pre>
     * 
     * @param offset
     *            maximum rows to skip
     */
    void setOffset(Long offset);

    /**
     * Whether <i>offset</i> clause is supported in the statement or not.
     * <p>
     * Note that setting offset values is always possible, even if they are not supported.
     * </p>
     * 
     * @return
     *         <code>true</code> if <i>offset</i> clause is supported
     */
    boolean supportsOffset();

    /**
     * Get the search criteria.
     * 
     * @return X Protocol Expr object
     */
    Object getCriteria();

    /**
     * Parse criteriaString into X Protocol Expr object.
     * 
     * <pre>
     * docs = this.collection.find("$.x1 = 29 | 15").execute();
     * table.delete().where("age == 13").execute();
     * </pre>
     * 
     * @param criteriaString
     *            expression
     */
    void setCriteria(String criteriaString);

    /**
     * Get binding arguments.
     * 
     * @return List of X Protocol Scalar object
     */
    Object getArgs();

    /**
     * Set binding.
     * 
     * <pre>
     * this.collection.find("a = :arg1 or b = :arg2").bind("arg1", 1).bind("arg2", 2).execute();
     * </pre>
     * 
     * @param name
     *            bind key
     * @param value
     *            bind value
     */
    void addArg(String name, Object value);

    /**
     * Verify that all arguments are bound. Throws {@link WrongArgumentException} if any placeholder argument is not bound.
     */
    void verifyAllArgsBound();

    /**
     * Remove all current bindings.
     */
    void clearArgs();

    /**
     * Are relational columns identifiers allowed?
     * 
     * @return true if allowed
     */
    boolean isRelational();

    /**
     * Parse projection expressions into X Protocol Projection objects.
     * 
     * <pre>
     * collection.find().fields("CAST($.x as SIGNED) as x").execute();
     * table.select("_id, name, birthday, age").execute();
     * table.select("age as age_group, count(name) as cnt, something").execute();
     * </pre>
     * 
     * @param projection
     *            projection expression
     */
    void setFields(String... projection);

    /**
     * Get X Protocol Projection objects.
     * 
     * @return List of X Protocol Projection objects.
     */
    Object getFields();

    /**
     * Parse groupBy expressions into X Protocol Expr objects.
     * 
     * <pre>
     * SelectStatement stmt = table.select("age as age_group, count(name) as cnt, something");
     * stmt.groupBy("something", "age_group");
     * </pre>
     * 
     * @param groupBy
     *            groupBy expression
     */
    void setGrouping(String... groupBy);

    /**
     * Get X Protocol Expr objects for groupBy.
     * 
     * @return List of X Protocol Expr objects
     */
    Object getGrouping();

    /**
     * Parse having expressions into X Protocol Expr objects.
     * 
     * <pre>
     * SelectStatement stmt = table.select("age as age_group, count(name) as cnt, something");
     * stmt.groupBy("something", "age_group");
     * stmt.having("cnt &gt; 1");
     * </pre>
     * 
     * @param having
     *            having expression
     */
    void setGroupingCriteria(String having);

    /**
     * Get X Protocol Expr objects for grouping criteria.
     * 
     * @return List of X Protocol Expr objects
     */
    Object getGroupingCriteria();

    /**
     * Get {@link RowLock} value.
     * 
     * @return {@link RowLock}
     */
    RowLock getLock();

    /**
     * Set {@link RowLock} value.
     * 
     * @param rowLock
     *            {@link RowLock}
     */
    void setLock(RowLock rowLock);

    /**
     * Get {@link RowLockOptions} value.
     * 
     * @return {@link RowLockOptions}
     */
    RowLockOptions getLockOption();

    /**
     * Set {@link RowLockOptions} value.
     * 
     * @param rowLockOption
     *            {@link RowLockOptions}
     */
    void setLockOption(RowLockOptions rowLockOption);
}
