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

/**
 * A statement to <i>find</i> the set of documents according to the given specification.
 */
public interface FindStatement extends Statement<FindStatement, DocResult> {
    /**
     * Add/replace the field projections defining the result.
     * 
     * @param projections
     *            projection expression
     * @return {@link FindStatement}
     */
    FindStatement fields(String... projections);

    /**
     * Add/replace the field projection defining the result.
     * 
     * @param docProjection
     *            projection expression
     * @return {@link FindStatement}
     */
    FindStatement fields(Expression docProjection);

    /**
     * Add/replace the aggregation fields for this query.
     * 
     * @param groupBy
     *            groupBy expression
     * @return {@link FindStatement}
     */
    FindStatement groupBy(String... groupBy);

    /**
     * Add/replace the aggregate criteria for this query.
     * 
     * @param having
     *            having expression
     * @return {@link FindStatement}
     */
    FindStatement having(String having);

    /**
     * Add/replace the order specification for this query.
     * 
     * @param sortFields
     *            sort expression
     * @return {@link FindStatement}
     */
    FindStatement orderBy(String... sortFields);

    /**
     * Add/replace the order specification for this query.
     * <p>
     * Synonym for {@link #orderBy(String...)}
     * 
     * @param sortFields
     *            sort expression
     * @return {@link FindStatement}
     */
    FindStatement sort(String... sortFields);

    /**
     * Add/replace the document offset for this query.
     * 
     * @param limitOffset
     *            number of documents to skip
     * @return {@link FindStatement}
     * @deprecated Deprecated in c/J 8.0.12, please use {@link #offset(long)} instead.
     */
    @Deprecated
    default FindStatement skip(long limitOffset) {
        return offset(limitOffset);
    }

    /**
     * Add/replace the document offset for this query.
     * 
     * @param limitOffset
     *            number of documents to skip
     * @return {@link FindStatement}
     */
    FindStatement offset(long limitOffset);

    /**
     * Add/replace the document limit for this query.
     * 
     * @param numberOfRows
     *            limit
     * @return {@link FindStatement}
     */
    FindStatement limit(long numberOfRows);

    /**
     * Locks matching rows against updates.
     * 
     * @return {@link FindStatement}
     */
    FindStatement lockShared();

    /**
     * Locks matching rows against updates using the provided lock contention option.
     * 
     * @param lockContention
     *            The {@link com.mysql.cj.xdevapi.Statement.LockContention} value to set.
     * @return {@link FindStatement}
     */
    FindStatement lockShared(LockContention lockContention);

    /**
     * Locks matching rows exclusively so no other transactions can read or write to them.
     * 
     * @return {@link FindStatement}
     */
    FindStatement lockExclusive();

    /**
     * Locks matching rows exclusively so no other transactions can read or write to them, using the provided lock contention option.
     * 
     * @param lockContention
     *            The {@link com.mysql.cj.xdevapi.Statement.LockContention} value to set.
     * @return {@link FindStatement}
     */
    FindStatement lockExclusive(LockContention lockContention);
}
