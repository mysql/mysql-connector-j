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

package com.mysql.fabric;

import java.util.Set;

/**
 * Server Group - a set of servers responsible for the same set of data
 */
public class ServerGroup {
    private String name;
    private Set<Server> servers;

    public ServerGroup(String name, Set<Server> servers) {
        this.name = name;
        this.servers = servers;
    }

    public String getName() {
        return this.name;
    }

    public Set<Server> getServers() {
        return this.servers;
    }

    /**
     * Find the master server for this group.
     * @return the master server, or null if there's no master for the current group state
     */
    public Server getMaster() {
        for (Server s : this.servers) {
            if (s.getRole() == ServerRole.PRIMARY) {
                return s;
            }
        }
        return null;
    }

    /**
     * Lookup a server in this group for the matching host:port string.
     * @return the server, if found. null otherwise
     */
    public Server getServer(String hostPortString) {
        for (Server s : this.servers) {
            if (s.getHostPortString().equals(hostPortString)) {
                return s;
            }
        }
        return null;
    }

    @Override
    public String toString() {
        return String.format("Group[name=%s, servers=%s]", this.name, this.servers);
    }
}
