/*
 * Copyright (c) 2010, 2020, Oracle and/or its affiliates.
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

package com.mysql.cj.jdbc;

import java.sql.SQLException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.mysql.cj.jdbc.jmx.LoadBalanceConnectionGroupManager;

public class ConnectionGroupManager {

    private static HashMap<String, ConnectionGroup> GROUP_MAP = new HashMap<>();

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
            Set<ConnectionGroup> s = new HashSet<>();

            s.addAll(GROUP_MAP.values());
            return s;
        }
        Set<ConnectionGroup> s = new HashSet<>();
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

        Set<String> active = new HashSet<>();
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
        Set<String> hosts = new HashSet<>();
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
        Map<String, Integer> hosts = new HashMap<>();
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
