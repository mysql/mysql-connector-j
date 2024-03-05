/*
 * Copyright (c) 2013, 2024, Oracle and/or its affiliates.
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

package com.mysql.cj.jdbc.jmx;

import java.sql.SQLException;

public interface ReplicationGroupManagerMBean {

    void addReplicaHost(String groupFilter, String host) throws SQLException;

    /**
     * Use {@link #addReplicaHost(String, String)} instead.
     *
     * @param groupFilter
     *            filter
     * @param host
     *            host
     * @throws SQLException
     * @deprecated
     */
    @Deprecated
    default void addSlaveHost(String groupFilter, String host) throws SQLException {
        addReplicaHost(groupFilter, host);
    }

    void removeReplicaHost(String groupFilter, String host) throws SQLException;

    /**
     * Use {@link #removeReplicaHost(String, String)} instead.
     *
     * @param groupFilter
     *            filter
     * @param host
     *            host
     * @throws SQLException
     * @deprecated
     */
    @Deprecated
    default void removeSlaveHost(String groupFilter, String host) throws SQLException {
        removeReplicaHost(groupFilter, host);
    }

    void promoteReplicaToSource(String groupFilter, String host) throws SQLException;

    /**
     * Use {@link #promoteReplicaToSource(String, String)} instead.
     *
     * @param groupFilter
     *            filter
     * @param host
     *            host
     * @throws SQLException
     * @deprecated
     */
    @Deprecated
    default void promoteSlaveToMaster(String groupFilter, String host) throws SQLException {
        promoteReplicaToSource(groupFilter, host);
    }

    void removeSourceHost(String groupFilter, String host) throws SQLException;

    /**
     * Use {@link #removeSourceHost(String, String)} instead.
     *
     * @param groupFilter
     *            filter
     * @param host
     *            host
     * @throws SQLException
     * @deprecated
     */
    @Deprecated
    default void removeMasterHost(String groupFilter, String host) throws SQLException {
        removeSourceHost(groupFilter, host);
    }

    String getSourceHostsList(String group);

    /**
     * Use {@link #getSourceHostsList(String)} instead.
     *
     * @param group
     *            group
     * @return source hosts
     * @deprecated
     */
    @Deprecated
    default String getMasterHostsList(String group) {
        return getSourceHostsList(group);
    }

    String getReplicaHostsList(String group);

    /**
     * Use {@link #getReplicaHostsList(String)} instead.
     *
     * @param group
     *            group
     * @return replica hosts
     * @deprecated
     */
    @Deprecated
    default String getSlaveHostsList(String group) {
        return getReplicaHostsList(group);
    }

    String getRegisteredConnectionGroups();

    int getActiveSourceHostCount(String group);

    /**
     * Use {@link #getActiveSourceHostCount(String)} instead.
     *
     * @param group
     *            group
     * @return count
     * @deprecated
     */
    @Deprecated
    default int getActiveMasterHostCount(String group) {
        return getActiveSourceHostCount(group);
    }

    int getActiveReplicaHostCount(String group);

    /**
     * Use {@link #getActiveReplicaHostCount(String)} instead.
     *
     * @param group
     *            group
     * @return count
     * @deprecated
     */
    @Deprecated
    default int getActiveSlaveHostCount(String group) {
        return getActiveReplicaHostCount(group);
    }

    int getReplicaPromotionCount(String group);

    /**
     * Use {@link #getReplicaPromotionCount(String)} instead.
     *
     * @param group
     *            group
     * @return count
     * @deprecated
     */
    @Deprecated
    default int getSlavePromotionCount(String group) {
        return getReplicaPromotionCount(group);
    }

    long getTotalLogicalConnectionCount(String group);

    long getActiveLogicalConnectionCount(String group);

}
