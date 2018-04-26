/*
 * Copyright (c) 2013, 2018, Oracle and/or its affiliates. All rights reserved.
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

package com.mysql.cj.jdbc.jmx;

import java.lang.management.ManagementFactory;
import java.sql.SQLException;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import com.mysql.cj.Messages;
import com.mysql.cj.jdbc.exceptions.SQLError;
import com.mysql.cj.jdbc.ha.ReplicationConnectionGroup;
import com.mysql.cj.jdbc.ha.ReplicationConnectionGroupManager;

public class ReplicationGroupManager implements ReplicationGroupManagerMBean {
    private boolean isJmxRegistered = false;

    public synchronized void registerJmx() throws SQLException {
        if (this.isJmxRegistered) {
            return;
        }
        MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
        try {
            ObjectName name = new ObjectName("com.mysql.cj.jdbc.jmx:type=ReplicationGroupManager");
            mbs.registerMBean(this, name);
            this.isJmxRegistered = true;
        } catch (Exception e) {
            throw SQLError.createSQLException(Messages.getString("ReplicationGroupManager.0"), null, e, null);
        }

    }

    @Override
    public void addSlaveHost(String groupFilter, String host) throws SQLException {
        ReplicationConnectionGroupManager.addSlaveHost(groupFilter, host);
    }

    @Override
    public void removeSlaveHost(String groupFilter, String host) throws SQLException {
        ReplicationConnectionGroupManager.removeSlaveHost(groupFilter, host);
    }

    @Override
    public void promoteSlaveToMaster(String groupFilter, String host) throws SQLException {
        ReplicationConnectionGroupManager.promoteSlaveToMaster(groupFilter, host);

    }

    @Override
    public void removeMasterHost(String groupFilter, String host) throws SQLException {
        ReplicationConnectionGroupManager.removeMasterHost(groupFilter, host);

    }

    @Override
    public String getMasterHostsList(String group) {
        StringBuilder sb = new StringBuilder("");
        boolean found = false;
        for (String host : ReplicationConnectionGroupManager.getMasterHosts(group)) {
            if (found) {
                sb.append(",");
            }
            found = true;
            sb.append(host);
        }
        return sb.toString();
    }

    @Override
    public String getSlaveHostsList(String group) {
        StringBuilder sb = new StringBuilder("");
        boolean found = false;
        for (String host : ReplicationConnectionGroupManager.getSlaveHosts(group)) {
            if (found) {
                sb.append(",");
            }
            found = true;
            sb.append(host);
        }
        return sb.toString();

    }

    @Override
    public String getRegisteredConnectionGroups() {
        StringBuilder sb = new StringBuilder("");
        boolean found = false;
        for (ReplicationConnectionGroup group : ReplicationConnectionGroupManager.getGroupsMatching(null)) {
            if (found) {
                sb.append(",");
            }
            found = true;
            sb.append(group.getGroupName());
        }
        return sb.toString();
    }

    @Override
    public int getActiveMasterHostCount(String group) {
        return ReplicationConnectionGroupManager.getMasterHosts(group).size();
    }

    @Override
    public int getActiveSlaveHostCount(String group) {
        return ReplicationConnectionGroupManager.getSlaveHosts(group).size();
    }

    @Override
    public int getSlavePromotionCount(String group) {
        return ReplicationConnectionGroupManager.getNumberOfMasterPromotion(group);
    }

    @Override
    public long getTotalLogicalConnectionCount(String group) {
        return ReplicationConnectionGroupManager.getTotalConnectionCount(group);
    }

    @Override
    public long getActiveLogicalConnectionCount(String group) {
        return ReplicationConnectionGroupManager.getActiveConnectionCount(group);
    }

}
