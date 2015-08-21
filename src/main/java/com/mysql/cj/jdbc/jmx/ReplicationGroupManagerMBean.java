/*
  Copyright (c) 2013, 2015, Oracle and/or its affiliates. All rights reserved.

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

package com.mysql.cj.jdbc.jmx;

import java.sql.SQLException;

public interface ReplicationGroupManagerMBean {

    void addSlaveHost(String groupFilter, String host) throws SQLException;

    void removeSlaveHost(String groupFilter, String host) throws SQLException;

    void promoteSlaveToMaster(String groupFilter, String host) throws SQLException;

    void removeMasterHost(String groupFilter, String host) throws SQLException;

    String getMasterHostsList(String group);

    String getSlaveHostsList(String group);

    String getRegisteredConnectionGroups();

    int getActiveMasterHostCount(String group);

    int getActiveSlaveHostCount(String group);

    int getSlavePromotionCount(String group);

    long getTotalLogicalConnectionCount(String group);

    long getActiveLogicalConnectionCount(String group);

}
