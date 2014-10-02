/*
  Copyright (c) 2013, 2014, Oracle and/or its affiliates. All rights reserved.

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

package testsuite.fabric;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Set;

import com.mysql.fabric.Server;
import com.mysql.fabric.ServerGroup;
import com.mysql.fabric.ServerMode;
import com.mysql.fabric.ServerRole;
import com.mysql.fabric.ShardMapping;
import com.mysql.fabric.proto.xmlrpc.XmlRpcClient;

/**
 * Tests for `mysqlfabric dump.*'.
 * 
 * Depends on standard Fabric test setup (which as of yet is not defined).
 * Updates to this test setup will require changes to the tests.
 */
public class TestDumpCommands extends BaseFabricTestCase {

    private XmlRpcClient client;

    public TestDumpCommands() throws Exception {
        super();
    }

    @Override
    public void setUp() throws Exception {
        if (this.isSetForFabricTest) {
            this.client = new XmlRpcClient(this.fabricUrl, this.fabricUsername, this.fabricPassword);
        }
    }

    public static Comparator<Server> serverHostnamePortSorter = new Comparator<Server>() {
        public int compare(Server s1, Server s2) {
            int l;
            l = s1.getHostname().compareTo(s2.getHostname());
            if (l != 0) {
                return l;
            }

            l = ((Integer) s1.getPort()).compareTo(s2.getPort());
            return l;
        }
    };

    /**
     * Test the Client.getServers() without a match pattern (all servers).
     */
    public void testDumpServersAll() throws Exception {
        if (!this.isSetForFabricTest) {
            return;
        }
        Set<ServerGroup> serverGroups = this.client.getServerGroups().getData();
        List<Server> servers = new ArrayList<Server>();
        for (ServerGroup g : serverGroups) {
            servers.addAll(g.getServers());
        }
        Collections.sort(servers, serverHostnamePortSorter);
        assertEquals(3, servers.size());

        assertEquals("fabric_test1_global", servers.get(0).getGroupName());
        assertEquals("fabric_test1_shard1", servers.get(1).getGroupName());
        assertEquals("fabric_test1_shard2", servers.get(2).getGroupName());

        assertEquals((this.globalHost != null ? this.globalHost : "127.0.0.1"), servers.get(0).getHostname());
        assertEquals((this.shard1Host != null ? this.shard1Host : "127.0.0.1"), servers.get(1).getHostname());
        assertEquals((this.shard2Host != null ? this.shard2Host : "127.0.0.1"), servers.get(2).getHostname());

        assertEquals((this.globalPort != null ? Integer.valueOf(this.globalPort) : 3401), servers.get(0).getPort());
        assertEquals((this.shard1Port != null ? Integer.valueOf(this.shard1Port) : 3402), servers.get(1).getPort());
        assertEquals((this.shard2Port != null ? Integer.valueOf(this.shard2Port) : 3403), servers.get(2).getPort());

        assertEquals(ServerMode.READ_WRITE, servers.get(0).getMode());
        assertEquals(ServerRole.PRIMARY, servers.get(0).getRole());
        assertEquals(ServerMode.READ_WRITE, servers.get(1).getMode());
        assertEquals(ServerRole.PRIMARY, servers.get(1).getRole());
        assertEquals(ServerMode.READ_WRITE, servers.get(2).getMode());
        assertEquals(ServerRole.PRIMARY, servers.get(2).getRole());
    }

    /**
     * Test the Client.getShardMaps() without a match pattern (all maps).
     */
    public void testDumpShardMapsAll() throws Exception {
        if (!this.isSetForFabricTest) {
            return;
        }
        Set<ShardMapping> shardMappings = this.client.getShardMappings().getData();
        assertEquals(1, shardMappings.size());
        ShardMapping shardMapping = shardMappings.iterator().next();

        assertEquals(1, shardMapping.getShardTables().size());
    }
}
