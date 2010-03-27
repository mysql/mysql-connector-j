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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class ConnectionGroup {
	private String groupName;
	private long connections = 0;
	private long activeConnections = 0;
	private HashMap connectionData = new HashMap();
	private HashMap connectionProxies = new HashMap();
	private List hostList;
	private boolean isInitialized = false;

	ConnectionGroup(String groupName){
		this.groupName = groupName;
	}
	
	public long registerConnectionProxy(LoadBalancingConnectionProxy proxy, List hostList){
		long currentConnectionId;

		synchronized (this){
			if(!this.isInitialized){
				this.hostList = hostList;
				this.isInitialized = true;
			}
			currentConnectionId = ++connections;
			this.connectionProxies.put(new Long(currentConnectionId), proxy);
		}
		this.activeConnections++;
		
		return currentConnectionId;
		
	}
	
	public String getGroupName(){
		return this.groupName;
	}
	
	public List getInitialHostList(){
		return this.hostList;
	}
	
	
	public long getTotalLogicalConnectionCount(){
		return this.connections;
	}
	
	public long getActiveLogicalConnectionCount(){
		return this.activeConnections;
	}
	
	public void closeConnection(long id){
		this.activeConnections--;
		this.connectionProxies.remove(new Long(id));
	}
	
	public void removeHost(String host){
		removeHost(host, false);
	}
	
	public void removeHost(String host, boolean killExistingConnections) {
		this.hostList.remove(host);
		
		if(killExistingConnections){
			
		}
	}
	
	
	public void addHost(String host){
		addHost(host, false);
	}
	
	
	public void addHost(String host, boolean forExisting){
		
		synchronized(this){
			this.hostList.add(host);
		}
		// all new connections will have this host
		if(!forExisting){
			return;
		}
		
		
		// make a local copy to keep synchronization overhead to minimum
		Map proxyMap = new HashMap();
		synchronized(this.connectionProxies){
			proxyMap.putAll(this.connectionProxies);
		}
		
		Set proxies = proxyMap.entrySet();
		
		Iterator i = proxies.iterator();
		while(i.hasNext()){
			LoadBalancingConnectionProxy proxy = (LoadBalancingConnectionProxy) i.next();
			proxy.addHost(host);
		}
		
	}
}
