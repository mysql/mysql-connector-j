/*
  Copyright (c) 2013, 2014, Oracle and/or its affiliates. All rights reserved.

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

package com.mysql.jdbc;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

public class ReplicationConnectionGroupManager {
    private static HashMap<String, ReplicationConnectionGroup> GROUP_MAP = new HashMap<String, ReplicationConnectionGroup>();

    private static com.mysql.jdbc.jmx.ReplicationGroupManager mbean = new com.mysql.jdbc.jmx.ReplicationGroupManager();

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
            Set<ReplicationConnectionGroup> s = new HashSet<ReplicationConnectionGroup>();

            s.addAll(GROUP_MAP.values());
            return s;
        }
        Set<ReplicationConnectionGroup> s = new HashSet<ReplicationConnectionGroup>();
        ReplicationConnectionGroup o = GROUP_MAP.get(group);
        if (o != null) {
            s.add(o);
        }
        return s;

    }

    public static void addSlaveHost(String group, String host) throws SQLException {
        Collection<ReplicationConnectionGroup> s = getGroupsMatching(group);
        for (ReplicationConnectionGroup cg : s) {
            cg.addSlaveHost(host);
        }
    }

    public static void removeSlaveHost(String group, String host) throws SQLException {
        removeSlaveHost(group, host, true);
    }

    public static void removeSlaveHost(String group, String host, boolean closeGently) throws SQLException {
        Collection<ReplicationConnectionGroup> s = getGroupsMatching(group);
        for (ReplicationConnectionGroup cg : s) {
            cg.removeSlaveHost(host, closeGently);
        }
    }

    public static void promoteSlaveToMaster(String group, String newMasterHost) throws SQLException {
        Collection<ReplicationConnectionGroup> s = getGroupsMatching(group);
        for (ReplicationConnectionGroup cg : s) {
            cg.promoteSlaveToMaster(newMasterHost);
        }

    }

    public static long getSlavePromotionCount(String group) throws SQLException {
        Collection<ReplicationConnectionGroup> s = getGroupsMatching(group);
        long promoted = 0;
        for (ReplicationConnectionGroup cg : s) {
            long tmp = cg.getNumberOfSlavePromotions();
            if (tmp > promoted) {
                promoted = tmp;
            }
        }
        return promoted;

    }

    public static void removeMasterHost(String group, String host) throws SQLException {
        removeMasterHost(group, host, true);
    }

    public static void removeMasterHost(String group, String host, boolean closeGently) throws SQLException {
        Collection<ReplicationConnectionGroup> s = getGroupsMatching(group);
        for (ReplicationConnectionGroup cg : s) {
            cg.removeMasterHost(host, closeGently);
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

    public static int getNumberOfMasterPromotion(String groupFilter) {
        int total = 0;
        Collection<ReplicationConnectionGroup> s = getGroupsMatching(groupFilter);
        for (ReplicationConnectionGroup cg : s) {
            total += cg.getNumberOfSlavePromotions();
        }
        return total;
    }

    public static int getConnectionCountWithHostAsSlave(String groupFilter, String host) {
        int total = 0;
        Collection<ReplicationConnectionGroup> s = getGroupsMatching(groupFilter);
        for (ReplicationConnectionGroup cg : s) {
            total += cg.getConnectionCountWithHostAsSlave(host);
        }
        return total;
    }

    public static int getConnectionCountWithHostAsMaster(String groupFilter, String host) {
        int total = 0;
        Collection<ReplicationConnectionGroup> s = getGroupsMatching(groupFilter);
        for (ReplicationConnectionGroup cg : s) {
            total += cg.getConnectionCountWithHostAsMaster(host);
        }
        return total;
    }

    public static Collection<String> getSlaveHosts(String groupFilter) {
        Collection<ReplicationConnectionGroup> s = getGroupsMatching(groupFilter);
        Collection<String> hosts = new ArrayList<String>();
        for (ReplicationConnectionGroup cg : s) {
            hosts.addAll(cg.getSlaveHosts());
        }
        return hosts;
    }

    public static Collection<String> getMasterHosts(String groupFilter) {
        Collection<ReplicationConnectionGroup> s = getGroupsMatching(groupFilter);
        Collection<String> hosts = new ArrayList<String>();
        for (ReplicationConnectionGroup cg : s) {
            hosts.addAll(cg.getMasterHosts());
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
