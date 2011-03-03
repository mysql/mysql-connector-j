/*
  Copyright (c) 2010, 2011, Oracle and/or its affiliates. All rights reserved.

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
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.mysql.jdbc.jmx.LoadBalanceConnectionGroupManager;

public class ConnectionGroupManager {

	private static HashMap GROUP_MAP = new HashMap();

	private static LoadBalanceConnectionGroupManager mbean = new LoadBalanceConnectionGroupManager();
	
	private static boolean hasRegisteredJmx = false;
	
	
	public static synchronized ConnectionGroup getConnectionGroupInstance(String groupName){
		if(GROUP_MAP.containsKey(groupName)){
			return (ConnectionGroup) GROUP_MAP.get(groupName);
		}
		ConnectionGroup group = new ConnectionGroup(groupName);
		GROUP_MAP.put(groupName, group);
		return group;
	}
	
	public static void registerJmx() throws SQLException {
		if(hasRegisteredJmx){
			return;
		}
		
		mbean.registerJmx();
		hasRegisteredJmx = true;
	}
	
	public static ConnectionGroup getConnectionGroup(String groupName){
		return (ConnectionGroup) GROUP_MAP.get(groupName);
	}
	
	private static Collection getGroupsMatching(String group){
		if(group == null || group.equals("")){
			Set s = new HashSet();
			
			s.addAll(GROUP_MAP.values());
			return s;
		}
		Set s = new HashSet();
		Object o = GROUP_MAP.get(group);
		if(o != null){
			s.add(o);
		}
		return s;
		
	}

	public static void addHost(String group, String host, boolean forExisting) {
		Collection s = getGroupsMatching(group);
		for(Iterator i = s.iterator(); i.hasNext();){
			((ConnectionGroup) i.next()).addHost(host, forExisting);
		}
	}

	public static int getActiveHostCount(String group) {
		
		Set active = new HashSet();
		Collection s = getGroupsMatching(group);
		for(Iterator i = s.iterator(); i.hasNext();){
			active.addAll(((ConnectionGroup) i.next()).getInitialHosts());
		}
		return active.size();
	}
	
	

	public static long getActiveLogicalConnectionCount(String group) {
		int count = 0;
		Collection s = getGroupsMatching(group);
		for(Iterator i = s.iterator(); i.hasNext();){
			count += ((ConnectionGroup) i.next()).getActiveLogicalConnectionCount();
		}
		return count;
	}

	public static long getActivePhysicalConnectionCount(String group) {
		int count = 0;
		Collection s = getGroupsMatching(group);
		for(Iterator i = s.iterator(); i.hasNext();){
			count += ((ConnectionGroup) i.next()).getActivePhysicalConnectionCount();
		}
		return count;
	}


	public static int getTotalHostCount(String group) {
		Collection s = getGroupsMatching(group);
		Set hosts = new HashSet();
		for(Iterator i = s.iterator(); i.hasNext();){
			ConnectionGroup cg = (ConnectionGroup) i.next();
			hosts.addAll(cg.getInitialHosts());
			hosts.addAll(cg.getClosedHosts());
		}
		return hosts.size();
	}

	public static long getTotalLogicalConnectionCount(String group) {
		long count = 0;
		Collection s = getGroupsMatching(group);
		for(Iterator i = s.iterator(); i.hasNext();){
			count += ((ConnectionGroup) i.next()).getTotalLogicalConnectionCount();
		}
		return count;
	}

	public static long getTotalPhysicalConnectionCount(String group) {
		long count = 0;
		Collection s = getGroupsMatching(group);
		for(Iterator i = s.iterator(); i.hasNext();){
			count += ((ConnectionGroup) i.next()).getTotalPhysicalConnectionCount();
		}
		return count;
	}

	public static long getTotalTransactionCount(String group) {
		long count = 0;
		Collection s = getGroupsMatching(group);
		for(Iterator i = s.iterator(); i.hasNext();){
			count += ((ConnectionGroup) i.next()).getTotalTransactionCount();
		}
		return count;
	}

	public static void removeHost(String group, String host) throws SQLException {
		removeHost(group, host, false);
	}
	
	public static void removeHost(String group, String host, boolean removeExisting) throws SQLException {
		Collection s = getGroupsMatching(group);
		for(Iterator i = s.iterator(); i.hasNext();){
			((ConnectionGroup) i.next()).removeHost(host, removeExisting);
		}
	}
	
	public static String getActiveHostLists(String group) {
		Collection s = getGroupsMatching(group);
		Map hosts = new HashMap();
		for(Iterator i = s.iterator(); i.hasNext();){
			
			Collection l = ((ConnectionGroup) i.next()).getInitialHosts();
			for(Iterator j = l.iterator(); j.hasNext();  ){
				String host = j.next().toString();
				Object o = hosts.get(host);
				if(o == null){
					o = Integer.valueOf(1);
				} else {
					o = Integer.valueOf(((Integer) o).intValue() + 1);
				}
				hosts.put(host, o);
				
			}
		}
		
		StringBuffer sb = new StringBuffer();
		String sep = "";
		for(Iterator i = hosts.keySet().iterator(); i.hasNext();){
			String host = i.next().toString();
			
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
		Collection s = getGroupsMatching(null);
		StringBuffer sb = new StringBuffer();
		String sep = "";
		for(Iterator i = s.iterator(); i.hasNext();){
			String group = ((ConnectionGroup)i.next()).getGroupName();
			sb.append(sep);
			sb.append(group);
			sep = ",";
		}
		return sb.toString();

	}
	
	
	
}
