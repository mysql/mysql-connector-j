/*
 Copyright (c) 2013, Oracle and/or its affiliates. All rights reserved.

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

import com.mysql.jdbc.ReplicationConnectionGroupManager;
import com.mysql.jdbc.SQLError;

public class ReplicationGroupManager implements ReplicationGroupManagerMBean {
	private boolean isJmxRegistered = false;

	public synchronized void registerJmx() throws SQLException {
		if(this.isJmxRegistered){
			return;
		}
		MBeanServer mbs = ManagementFactory.getPlatformMBeanServer(); 
		  try {
			ObjectName name = new ObjectName("com.mysql.jdbc.jmx:type=ReplicationGroupManager"); 
			  mbs.registerMBean(this, name);
			  this.isJmxRegistered = true;
		} catch (Exception e) {
			throw SQLError.createSQLException("Uable to register load-balance management bean with JMX", null, e, null);
		} 
		
	}

	public void addSlaveHost(String groupFilter, String host) throws SQLException {
		ReplicationConnectionGroupManager.addSlaveHost(groupFilter, host);
	}

	public void removeSlaveHost(String groupFilter, String host) throws SQLException {
		ReplicationConnectionGroupManager.removeSlaveHost(groupFilter, host);
	}

	public void promoteSlaveToMaster(String groupFilter, String host) throws SQLException {
		ReplicationConnectionGroupManager.promoteSlaveToMaster(groupFilter, host);
	}
	
	

}
