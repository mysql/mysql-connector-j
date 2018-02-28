/*
 * Copyright (c) 2018, Oracle and/or its affiliates. All rights reserved.
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

public interface FindParams {

    /**
     * <code>SHARED_LOCK = 1;</code>
     *
     * <pre>
     * Lock matching rows against updates
     * </pre>
     */
    public static final int SHARED_LOCK = 1;
    /**
     * <code>EXCLUSIVE_LOCK = 2;</code>
     *
     * <pre>
     * Lock matching rows so no other transaction can read or write to it
     * </pre>
     */
    public static final int EXCLUSIVE_LOCK = 2;

    Object getCollection();

    Object getOrder();

    void setOrder(String... orderExpression);

    Long getLimit();

    void setLimit(Long limit);

    Long getOffset();

    void setOffset(Long offset);

    Object getCriteria();

    void setCriteria(String criteriaString);

    Object getArgs();

    void addArg(String name, Object value);

    void verifyAllArgsBound();

    void clearArgs();

    boolean isRelational();

    void setFields(String... projection);

    Object getFields();

    void setGrouping(String... groupBy);

    Object getGrouping();

    void setGroupingCriteria(String having);

    Object getGroupingCriteria();

    int getLock();

    void setLock(int lock);

}
