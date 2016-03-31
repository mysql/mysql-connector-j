/*
  Copyright (c) 2010, 2016, Oracle and/or its affiliates. All rights reserved.

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

package com.mysql.jdbc;

import java.sql.SQLException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.mysql.jdbc.jmx.LoadBalanceConnectionGroupManager;

public class ConnectionGroupManager {

    private static HashMap<String, ConnectionGroup> GROUP_MAP = new HashMap<String, ConnectionGroup>();

    private static LoadBalanceConnectionGroupManager mbean = new LoadBalanceConnectionGroupManager();

    private static boolean hasRegisteredJmx = false;

    public static synchronized ConnectionGroup getConnectionGroupInstance(String groupName) {
        if (GROUP_MAP.containsKey(groupName)) {
            return GROUP_MAP.get(groupName);
        }
        ConnectionGroup group = new ConnectionGroup(groupName);
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

    public static ConnectionGroup getConnectionGroup(String groupName) {
        return GROUP_MAP.get(groupName);
    }

    private static Collection<ConnectionGroup> getGroupsMatching(String group) {
        if (group == null || group.equals("")) {
            Set<ConnectionGroup> s = new HashSet<ConnectionGroup>();

            s.addAll(GROUP_MAP.values());
            return s;
        }
        Set<ConnectionGroup> s = new HashSet<ConnectionGroup>();
        ConnectionGroup o = GROUP_MAP.get(group);
        if (o != null) {
            s.add(o);
        }
        return s;

    }

    public static void addHost(String group, String hostPortPair, boolean forExisting) {
        Collection<ConnectionGroup> s = getGroupsMatching(group);
        for (ConnectionGroup cg : s) {
            cg.addHost(hostPortPair, forExisting);
        }
    }

    public static int getActiveHostCount(String group) {
        Set<String> active = new HashSet<String>();
        Collection<ConnectionGroup> s = getGroupsMatching(group);
        for (ConnectionGroup cg : s) {
            active.addAll(cg.getInitialHosts());
        }
        return active.size();
    }

    public static long getActiveLogicalConnectionCount(String group) {
        int count = 0;
        Collection<ConnectionGroup> s = getGroupsMatching(group);
        for (ConnectionGroup cg : s) {
            count += cg.getActiveLogicalConnectionCount();
        }
        return count;
    }

    public static long getActivePhysicalConnectionCount(String group) {
        int count = 0;
        Collection<ConnectionGroup> s = getGroupsMatching(group);
        for (ConnectionGroup cg : s) {
            count += cg.getActivePhysicalConnectionCount();
        }
        return count;
    }

    public static int getTotalHostCount(String group) {
        Collection<ConnectionGroup> s = getGroupsMatching(group);
        Set<String> hosts = new HashSet<String>();
        for (ConnectionGroup cg : s) {
            hosts.addAll(cg.getInitialHosts());
            hosts.addAll(cg.getClosedHosts());
        }
        return hosts.size();
    }

    public static long getTotalLogicalConnectionCount(String group) {
        long count = 0;
        Collection<ConnectionGroup> s = getGroupsMatching(group);
        for (ConnectionGroup cg : s) {
            count += cg.getTotalLogicalConnectionCount();
        }
        return count;
    }

    public static long getTotalPhysicalConnectionCount(String group) {
        long count = 0;
        Collection<ConnectionGroup> s = getGroupsMatching(group);
        for (ConnectionGroup cg : s) {
            count += cg.getTotalPhysicalConnectionCount();
        }
        return count;
    }

    public static long getTotalTransactionCount(String group) {
        long count = 0;
        Collection<ConnectionGroup> s = getGroupsMatching(group);
        for (ConnectionGroup cg : s) {
            count += cg.getTotalTransactionCount();
        }
        return count;
    }

    public static void removeHost(String group, String hostPortPair) throws SQLException {
        removeHost(group, hostPortPair, false);
    }

    public static void removeHost(String group, String host, boolean removeExisting) throws SQLException {
        Collection<ConnectionGroup> s = getGroupsMatching(group);
        for (ConnectionGroup cg : s) {
            cg.removeHost(host, removeExisting);
        }
    }

    public static String getActiveHostLists(String group) {
        Collection<ConnectionGroup> s = getGroupsMatching(group);
        Map<String, Integer> hosts = new HashMap<String, Integer>();
        for (ConnectionGroup cg : s) {

            Collection<String> l = cg.getInitialHosts();
            for (String host : l) {
                Integer o = hosts.get(host);
                if (o == null) {
                    o = Integer.valueOf(1);
                } else {
                    o = Integer.valueOf(o.intValue() + 1);
                }
                hosts.put(host, o);

            }
        }

        StringBuilder sb = new StringBuilder();
        String sep = "";
        for (String host : hosts.keySet()) {
            sb.append(sep);
            sb.append(host);
            sb.append('(');
            sb.append(hosts.get(host));
            sb.append(')');
            sep = ",";
        }
        return sb.toString();
    }

    public static String getRegisteredConnectionGroups() {
        Collection<ConnectionGroup> s = getGroupsMatching(null);
        StringBuilder sb = new StringBuilder();
        String sep = "";
        for (ConnectionGroup cg : s) {
            String group = cg.getGroupName();
            sb.append(sep);
            sb.append(group);
            sep = ",";
        }
        return sb.toString();
    }
}
