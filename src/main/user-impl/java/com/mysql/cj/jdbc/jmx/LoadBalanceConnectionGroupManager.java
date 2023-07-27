/*
 * Copyright (c) 2010, 2023, Oracle and/or its affiliates.
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
import com.mysql.cj.jdbc.ConnectionGroupManager;
import com.mysql.cj.jdbc.exceptions.SQLError;

public class LoadBalanceConnectionGroupManager implements LoadBalanceConnectionGroupManagerMBean {

    private boolean isJmxRegistered = false;

    public LoadBalanceConnectionGroupManager() {
    }

    public synchronized void registerJmx() throws SQLException {
        if (this.isJmxRegistered) {
            return;
        }
        MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
        try {
            ObjectName name = new ObjectName("com.mysql.cj.jdbc.jmx:type=LoadBalanceConnectionGroupManager");
            mbs.registerMBean(this, name);
            this.isJmxRegistered = true;
        } catch (Exception e) {
            throw SQLError.createSQLException(Messages.getString("LoadBalanceConnectionGroupManager.0"), null, e, null);
        }
    }

    @Override
    public void addHost(String group, String host, boolean forExisting) {
        try {
            ConnectionGroupManager.addHost(group, host, forExisting);
        } catch (Exception e) {
            e.printStackTrace(); // TODO log error normally instead of sysout
        }
    }

    @Override
    public int getActiveHostCount(String group) {
        return ConnectionGroupManager.getActiveHostCount(group);
    }

    @Override
    public long getActiveLogicalConnectionCount(String group) {
        return ConnectionGroupManager.getActiveLogicalConnectionCount(group);
    }

    @Override
    public long getActivePhysicalConnectionCount(String group) {
        return ConnectionGroupManager.getActivePhysicalConnectionCount(group);
    }

    @Override
    public int getTotalHostCount(String group) {
        return ConnectionGroupManager.getTotalHostCount(group);
    }

    @Override
    public long getTotalLogicalConnectionCount(String group) {
        return ConnectionGroupManager.getTotalLogicalConnectionCount(group);
    }

    @Override
    public long getTotalPhysicalConnectionCount(String group) {
        return ConnectionGroupManager.getTotalPhysicalConnectionCount(group);
    }

    @Override
    public long getTotalTransactionCount(String group) {
        return ConnectionGroupManager.getTotalTransactionCount(group);
    }

    @Override
    public void removeHost(String group, String host) throws SQLException {
        ConnectionGroupManager.removeHost(group, host);
    }

    @Override
    public String getActiveHostsList(String group) {
        return ConnectionGroupManager.getActiveHostLists(group);
    }

    @Override
    public String getRegisteredConnectionGroups() {
        return ConnectionGroupManager.getRegisteredConnectionGroups();
    }

    @Override
    public void stopNewConnectionsToHost(String group, String host) throws SQLException {
        ConnectionGroupManager.removeHost(group, host);
    }

}
