/*
  Copyright (c) 2010, 2014, Oracle and/or its affiliates. All rights reserved.

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

package com.mysql.jdbc.jmx;

import java.lang.management.ManagementFactory;
import java.sql.SQLException;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import com.mysql.jdbc.ConnectionGroupManager;
import com.mysql.jdbc.SQLError;

public class LoadBalanceConnectionGroupManager implements
		LoadBalanceConnectionGroupManagerMBean {
	
	private boolean isJmxRegistered = false;
	
	public LoadBalanceConnectionGroupManager(){

	}
	
	public synchronized void registerJmx() throws SQLException {
		if(this.isJmxRegistered){
			return;
		}
		MBeanServer mbs = ManagementFactory.getPlatformMBeanServer(); 
		  try {
			ObjectName name = new ObjectName("com.mysql.jdbc.jmx:type=LoadBalanceConnectionGroupManager"); 
			  mbs.registerMBean(this, name);
			  this.isJmxRegistered = true;
		} catch (Exception e) {
			throw SQLError.createSQLException("Uable to register load-balance management bean with JMX", null, e, null);
		} 
		
	}

	public void addHost(String group, String host, boolean forExisting) {
		try {
		ConnectionGroupManager.addHost(group, host, forExisting);
		} catch (Exception e){
			e.printStackTrace();
		}
	}

	public int getActiveHostCount(String group) {
		return ConnectionGroupManager.getActiveHostCount(group);
	}

	public long getActiveLogicalConnectionCount(String group) {
		return ConnectionGroupManager.getActiveLogicalConnectionCount(group);
	}

	public long getActivePhysicalConnectionCount(String group) {
		return ConnectionGroupManager.getActivePhysicalConnectionCount(group);
	}

	public int getTotalHostCount(String group) {
		return ConnectionGroupManager.getTotalHostCount(group);

	}

	public long getTotalLogicalConnectionCount(String group) {
		return ConnectionGroupManager.getTotalLogicalConnectionCount(group);

	}

	public long getTotalPhysicalConnectionCount(String group) {
		return ConnectionGroupManager.getTotalPhysicalConnectionCount(group);

	}

	public long getTotalTransactionCount(String group) {
		return ConnectionGroupManager.getTotalTransactionCount(group);

	}

	public void removeHost(String group, String host) throws SQLException {
		ConnectionGroupManager.removeHost(group, host);

	}

	public String getActiveHostsList(String group) {
		return ConnectionGroupManager.getActiveHostLists(group);
	}

	public String getRegisteredConnectionGroups() {
		return ConnectionGroupManager.getRegisteredConnectionGroups();
	}

	public void stopNewConnectionsToHost(String group, String host)
			throws SQLException {
		ConnectionGroupManager.removeHost(group, host);
		
	}
	
	

}
