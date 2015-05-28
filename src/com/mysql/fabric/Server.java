/*
  Copyright (c) 2013, 2015, Oracle and/or its affiliates. All rights reserved.

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

package com.mysql.fabric;

/**
 * Database server, as represented by Fabric.
 */
public class Server implements Comparable<Server> {
    private String groupName;
    private String uuid;
    private String hostname;
    private int port;
    private ServerMode mode;
    private ServerRole role;
    private double weight;

    public Server(String groupName, String uuid, String hostname, int port, ServerMode mode, ServerRole role, double weight) {
        this.groupName = groupName; // may be null
        this.uuid = uuid;
        this.hostname = hostname;
        this.port = port;
        this.mode = mode;
        this.role = role;
        this.weight = weight;
        assert (uuid != null && !"".equals(uuid));
        assert (hostname != null && !"".equals(hostname));
        assert (port > 0);
        assert (mode != null);
        assert (role != null);
        assert (weight > 0.0);
    }

    public String getGroupName() {
        return this.groupName;
    }

    public String getUuid() {
        return this.uuid;
    }

    public String getHostname() {
        return this.hostname;
    }

    public int getPort() {
        return this.port;
    }

    public ServerMode getMode() {
        return this.mode;
    }

    public ServerRole getRole() {
        return this.role;
    }

    public double getWeight() {
        return this.weight;
    }

    public String getHostPortString() {
        return this.hostname + ":" + this.port;
    }

    public boolean isMaster() {
        return this.role == ServerRole.PRIMARY;
    }

    public boolean isSlave() {
        return this.role == ServerRole.SECONDARY || this.role == ServerRole.SPARE;
    }

    @Override
    public String toString() {
        return String.format("Server[%s, %s:%d, %s, %s, weight=%s]", this.uuid, this.hostname, this.port, this.mode, this.role, this.weight);
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof Server)) {
            return false;
        }
        Server s = (Server) o;
        return s.getUuid().equals(getUuid());
    }

    @Override
    public int hashCode() {
        return getUuid().hashCode();
    }

    public int compareTo(Server other) {
        return getUuid().compareTo(other.getUuid());
    }
}
