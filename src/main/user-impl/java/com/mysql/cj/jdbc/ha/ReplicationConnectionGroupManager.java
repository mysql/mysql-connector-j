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

    public static void removeReplicaHost(String group, String hostPortPair) throws SQLException {
        removeReplicaHost(group, hostPortPair, true);
    }

    public static void removeReplicaHost(String group, String hostPortPair, boolean closeGently) throws SQLException {
        Collection<ReplicationConnectionGroup> s = getGroupsMatching(group);
        for (ReplicationConnectionGroup cg : s) {
            cg.removeReplicaHost(hostPortPair, closeGently);
        }
    }

    public static void promoteReplicaToSource(String group, String hostPortPair) throws SQLException {
        Collection<ReplicationConnectionGroup> s = getGroupsMatching(group);
        for (ReplicationConnectionGroup cg : s) {
            cg.promoteReplicaToSource(hostPortPair);
        }
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

    public static void removeSourceHost(String group, String hostPortPair) throws SQLException {
        removeSourceHost(group, hostPortPair, true);
    }

    public static void removeSourceHost(String group, String hostPortPair, boolean closeGently) throws SQLException {
        Collection<ReplicationConnectionGroup> s = getGroupsMatching(group);
        for (ReplicationConnectionGroup cg : s) {
            cg.removeSourceHost(hostPortPair, closeGently);
        }
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

    public static int getConnectionCountWithHostAsReplica(String groupFilter, String hostPortPair) {
        int total = 0;
        Collection<ReplicationConnectionGroup> s = getGroupsMatching(groupFilter);
        for (ReplicationConnectionGroup cg : s) {
            total += cg.getConnectionCountWithHostAsReplica(hostPortPair);
        }
        return total;
    }

    public static int getConnectionCountWithHostAsSource(String groupFilter, String hostPortPair) {
        int total = 0;
        Collection<ReplicationConnectionGroup> s = getGroupsMatching(groupFilter);
        for (ReplicationConnectionGroup cg : s) {
            total += cg.getConnectionCountWithHostAsSource(hostPortPair);
        }
        return total;
    }

    public static Collection<String> getReplicaHosts(String groupFilter) {
        Collection<ReplicationConnectionGroup> s = getGroupsMatching(groupFilter);
        Collection<String> hosts = new ArrayList<>();
        for (ReplicationConnectionGroup cg : s) {
            hosts.addAll(cg.getReplicaHosts());
        }
        return hosts;
    }

    public static Collection<String> getSourceHosts(String groupFilter) {
        Collection<ReplicationConnectionGroup> s = getGroupsMatching(groupFilter);
        Collection<String> hosts = new ArrayList<>();
        for (ReplicationConnectionGroup cg : s) {
            hosts.addAll(cg.getSourceHosts());
        }
        return hosts;
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
