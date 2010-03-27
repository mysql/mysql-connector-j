/*
  Copyright (c) 2010, Oracle and/or its affiliates. All rights reserved.

  The MySQL Connector/J is licensed under the terms of the GPL,
  like most MySQL Connectors. There are special exceptions to the
  terms and conditions of the GPL as it is applied to this software,
  see the FLOSS License Exception available on mysql.com.

  This program is free software; you can redistribute it and/or
  modify it under the terms of the GNU General Public License as
  published by the Free Software Foundation; version 2 of the
  License.

  This program is distributed in the hope that it will be useful,  
  but WITHOUT ANY WARRANTY; without even the implied warranty of
  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
  GNU General Public License for more details.

  You should have received a copy of the GNU General Public License
  along with this program; if not, write to the Free Software
  Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
  02110-1301 USA
 
 */
package com.mysql.jdbc;

import java.util.HashMap;

public class ConnectionGroupManager {

	private static HashMap GROUP_MAP = new HashMap();
	public static synchronized ConnectionGroup getConnectionGroupInstance(String groupName){
		if(GROUP_MAP.containsKey(groupName)){
			return (ConnectionGroup) GROUP_MAP.get(groupName);
		}
		ConnectionGroup group = new ConnectionGroup(groupName);
		GROUP_MAP.put(groupName, group);
		return group;
	}
	
	public static ConnectionGroup getConnectionGroup(String groupName){
		return (ConnectionGroup) GROUP_MAP.get(groupName);
	}
	
}
