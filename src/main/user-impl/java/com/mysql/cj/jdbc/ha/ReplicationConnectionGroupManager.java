/*
 * Copyright (c) 2013, 2023, Oracle and/or its affiliates.
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

package com.mysql.cj.jdbc.ha;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

public class ReplicationConnectionGroupManager {

    private static HashMap<String, ReplicationConnectionGroup> GROUP_MAP = new HashMap<>();

    private static com.mysql.cj.jdbc.jmx.ReplicationGroupManager mbean = new com.mysql.cj.jdbc.jmx.ReplicationGroupManager();

    private static boolean hasRegisteredJmx = false;

    public static synchronized ReplicationConnectionGroup getConnectionGroupInstance(String groupName) {
        if (GROUP_MAP.containsKey(groupName)) {
            return GROUP_MAP.get(groupName);
        }
        ReplicationConnectionGroup group = new ReplicationConnectionGroup(groupName);
        GROUP_MAP.put(groupName, group);
        return group;
    }

    public static void registerJmx() throws SQLException {
        if (hasRegisteredJmx) {
            return;
        }

        mbean.registerJmx();
        hasRegisteredJmx = true;
    }

    public static ReplicationConnectionGroup getConnectionGroup(String groupName) {
        return GROUP_MAP.get(groupName);
    }

    public static Collection<ReplicationConnectionGroup> getGroupsMatching(String group) {
        if (group == null || group.equals("")) {
            Set<ReplicationConnectionGroup> s = new HashSet<>();

            s.addAll(GROUP_MAP.values());
            return s;
        }
        Set<ReplicationConnectionGroup> s = new HashSet<>();
        ReplicationConnectionGroup o = GROUP_MAP.get(group);
        if (o != null) {
            s.add(o);
        }
        return s;
    }

    public static void addReplicaHost(String group, String hostPortPair) throws SQLException {
        Collection<ReplicationConnectionGroup> s = getGroupsMatching(group);
        for (ReplicationConnectionGroup cg : s) {
            cg.addReplicaHost(hostPortPair);
        }
    }

    /**
     * Use {@link #addReplicaHost(String, String)} instead.
     *
     * @param group
     *            group name
     * @param hostPortPair
     *            host:port
     * @throws SQLException
     * @deprecated
     */
    @Deprecated
    public static void addSlaveHost(String group, String hostPortPair) throws SQLException {
        addReplicaHost(group, hostPortPair);
    }

    public static void removeReplicaHost(String group, String hostPortPair) throws SQLException {
        removeReplicaHost(group, hostPortPair, true);
    }

    /**
     * Use {@link #removeReplicaHost(String, String)} instead.
     *
     * @param group
     *            group name
     * @param hostPortPair
     *            host:port
     * @throws SQLException
     * @deprecated
     */
    @Deprecated
    public static void removeSlaveHost(String group, String hostPortPair) throws SQLException {
        removeReplicaHost(group, hostPortPair);
    }

    public static void removeReplicaHost(String group, String hostPortPair, boolean closeGently) throws SQLException {
        Collection<ReplicationConnectionGroup> s = getGroupsMatching(group);
        for (ReplicationConnectionGroup cg : s) {
            cg.removeReplicaHost(hostPortPair, closeGently);
        }
    }

    /**
     * Use {@link #removeReplicaHost(String, String, boolean)} instead.
     *
     * @param group
     *            group name
     * @param hostPortPair
     *            host:port
     * @param closeGently
     *            option
     * @throws SQLException
     * @deprecated
     */
    @Deprecated
    public static void removeSlaveHost(String group, String hostPortPair, boolean closeGently) throws SQLException {
        removeReplicaHost(group, hostPortPair, closeGently);
    }

    public static void promoteReplicaToSource(String group, String hostPortPair) throws SQLException {
        Collection<ReplicationConnectionGroup> s = getGroupsMatching(group);
        for (ReplicationConnectionGroup cg : s) {
            cg.promoteReplicaToSource(hostPortPair);
        }
    }

    /**
     * Use {@link #promoteReplicaToSource(String, String)} instead.
     *
     * @param group
     *            group name
     * @param hostPortPair
     *            host:port
     * @throws SQLException
     * @deprecated
     */
    @Deprecated
    public static void promoteSlaveToMaster(String group, String hostPortPair) throws SQLException {
        promoteReplicaToSource(group, hostPortPair);
    }

    public static long getReplicaPromotionCount(String group) throws SQLException {
        Collection<ReplicationConnectionGroup> s = getGroupsMatching(group);
        long promoted = 0;
        for (ReplicationConnectionGroup cg : s) {
            long tmp = cg.getNumberOfReplicaPromotions();
            if (tmp > promoted) {
                promoted = tmp;
            }
        }
        return promoted;
    }

    /**
     * Use {@link #getReplicaPromotionCount(String)} instead.
     *
     * @param group
     *            group name
     * @return count
     * @throws SQLException
     * @deprecated
     */
    @Deprecated
    public static long getSlavePromotionCount(String group) throws SQLException {
        return getReplicaPromotionCount(group);
    }

    public static void removeSourceHost(String group, String hostPortPair) throws SQLException {
        removeSourceHost(group, hostPortPair, true);
    }

    /**
     * Use {@link #removeSourceHost(String, String)} instead.
     *
     * @param group
     *            group name
     * @param hostPortPair
     *            host:port
     * @throws SQLException
     * @deprecated
     */
    @Deprecated
    public static void removeMasterHost(String group, String hostPortPair) throws SQLException {
        removeSourceHost(group, hostPortPair);
    }

    public static void removeSourceHost(String group, String hostPortPair, boolean closeGently) throws SQLException {
        Collection<ReplicationConnectionGroup> s = getGroupsMatching(group);
        for (ReplicationConnectionGroup cg : s) {
            cg.removeSourceHost(hostPortPair, closeGently);
        }
    }

    /**
     * Use {@link #removeSourceHost(String, String, boolean)} instead.
     *
     * @param group
     *            group name
     * @param hostPortPair
     *            host:port
     * @param closeGently
     *            option
     * @throws SQLException
     * @deprecated
     */
    @Deprecated
    public static void removeMasterHost(String group, String hostPortPair, boolean closeGently) throws SQLException {
        removeSourceHost(group, hostPortPair, closeGently);
    }

    public static String getRegisteredReplicationConnectionGroups() {
        Collection<ReplicationConnectionGroup> s = getGroupsMatching(null);
        StringBuilder sb = new StringBuilder();
        String sep = "";
        for (ReplicationConnectionGroup cg : s) {
            String group = cg.getGroupName();
            sb.append(sep);
            sb.append(group);
            sep = ",";
        }
        return sb.toString();
    }

    public static int getNumberOfSourcePromotion(String groupFilter) {
        int total = 0;
        Collection<ReplicationConnectionGroup> s = getGroupsMatching(groupFilter);
        for (ReplicationConnectionGroup cg : s) {
            total += cg.getNumberOfReplicaPromotions();
        }
        return total;
    }

    /**
     * Use {@link #getNumberOfSourcePromotion(String)} instead.
     *
     * @param groupFilter
     *            filter
     * @return count
     * @deprecated
     */
    @Deprecated
    public static int getNumberOfMasterPromotion(String groupFilter) {
        return getNumberOfSourcePromotion(groupFilter);
    }

    public static int getConnectionCountWithHostAsReplica(String groupFilter, String hostPortPair) {
        int total = 0;
        Collection<ReplicationConnectionGroup> s = getGroupsMatching(groupFilter);
        for (ReplicationConnectionGroup cg : s) {
            total += cg.getConnectionCountWithHostAsReplica(hostPortPair);
        }
        return total;
    }

    /**
     * Use {@link #getConnectionCountWithHostAsReplica(String, String)} instead.
     *
     * @param groupFilter
     *            filter
     * @param hostPortPair
     *            host:port
     * @return count
     * @deprecated
     */
    @Deprecated
    public static int getConnectionCountWithHostAsSlave(String groupFilter, String hostPortPair) {
        return getConnectionCountWithHostAsReplica(groupFilter, hostPortPair);
    }

    public static int getConnectionCountWithHostAsSource(String groupFilter, String hostPortPair) {
        int total = 0;
        Collection<ReplicationConnectionGroup> s = getGroupsMatching(groupFilter);
        for (ReplicationConnectionGroup cg : s) {
            total += cg.getConnectionCountWithHostAsSource(hostPortPair);
        }
        return total;
    }

    /**
     * Use {@link #getConnectionCountWithHostAsSource(String, String)} instead.
     *
     * @param groupFilter
     *            filter
     * @param hostPortPair
     *            host:port
     * @return count
     * @deprecated
     */
    @Deprecated
    public static int getConnectionCountWithHostAsMaster(String groupFilter, String hostPortPair) {
        return getConnectionCountWithHostAsSource(groupFilter, hostPortPair);
    }

    public static Collection<String> getReplicaHosts(String groupFilter) {
        Collection<ReplicationConnectionGroup> s = getGroupsMatching(groupFilter);
        Collection<String> hosts = new ArrayList<>();
        for (ReplicationConnectionGroup cg : s) {
            hosts.addAll(cg.getReplicaHosts());
        }
        return hosts;
    }

    /**
     * Use {@link #getReplicaHosts(String)} instead.
     *
     * @param groupFilter
     *            filter
     * @return hosts
     * @deprecated
     */
    @Deprecated
    public static Collection<String> getSlaveHosts(String groupFilter) {
        return getReplicaHosts(groupFilter);
    }

    public static Collection<String> getSourceHosts(String groupFilter) {
        Collection<ReplicationConnectionGroup> s = getGroupsMatching(groupFilter);
        Collection<String> hosts = new ArrayList<>();
        for (ReplicationConnectionGroup cg : s) {
            hosts.addAll(cg.getSourceHosts());
        }
        return hosts;
    }

    /**
     * Use {@link #getSourceHosts(String)} instead.
     *
     * @param groupFilter
     *            filter
     * @return hosts
     * @deprecated
     */
    @Deprecated
    public static Collection<String> getMasterHosts(String groupFilter) {
        return getSourceHosts(groupFilter);
    }

    public static long getTotalConnectionCount(String group) {
        long connections = 0;
        Collection<ReplicationConnectionGroup> s = getGroupsMatching(group);
        for (ReplicationConnectionGroup cg : s) {
            connections += cg.getTotalConnectionCount();
        }
        return connections;
    }

    public static long getActiveConnectionCount(String group) {
        long connections = 0;
        Collection<ReplicationConnectionGroup> s = getGroupsMatching(group);
        for (ReplicationConnectionGroup cg : s) {
            connections += cg.getActiveConnectionCount();
        }
        return connections;
    }

}
