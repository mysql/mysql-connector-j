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

package com.mysql.fabric.proto.xmlrpc;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.mysql.fabric.FabricCommunicationException;
import com.mysql.fabric.Server;
import com.mysql.fabric.ServerGroup;
import com.mysql.fabric.ServerMode;
import com.mysql.fabric.ServerRole;
import com.mysql.fabric.ShardIndex;
import com.mysql.fabric.ShardMapping;
import com.mysql.fabric.ShardMappingFactory;
import com.mysql.fabric.ShardTable;
import com.mysql.fabric.ShardingType;

import com.mysql.fabric.DumpResponse;
import com.mysql.fabric.FabricStateResponse;
import com.mysql.fabric.Response;

/**
 * Fabric client using the XML-RPC protocol.
 */
public class XmlRpcClient {
	private XmlRpcMethodCaller methodCaller;

	public XmlRpcClient(String url, String username, String password) throws FabricCommunicationException {
		this.methodCaller = new InternalXmlRpcMethodCaller(url);
		if (username != null && !"".equals(username) && password != null) {
			this.methodCaller = new AuthenticatedXmlRpcMethodCaller(this.methodCaller,
																	url, username, password);
		}
	}

	/**
	 * Unmarshall a response representing a server.
	 *
	 * `sharding.lookup_servers' returns ['38dc041b-de86-11e2-a891-e281dccd6dba', '127.0.0.1:3402', True]
	 * `dump.servers' returns ['38dc041b-de86-11e2-a891-e281dccd6dba', 'shard1', '127.0.0.1', '3402', 3, 3, 1.0]
	 */
	private static Server unmarshallServer(List serverData) {
		Server s = null;
		if (serverData.size() <= 4) {
			// first format
			String hostnameAndPort[] = ((String)serverData.get(1)).split(":");
			String hostname = hostnameAndPort[0];
			int port = Integer.valueOf(hostnameAndPort[1]);
			ServerRole role = (Boolean)serverData.get(2) ? ServerRole.PRIMARY : ServerRole.SECONDARY;
			// get(3) = "Running" (optional...) shows up on getGroup(), but not getServersForKey()
			s = new Server(null/* group name not known */, (String)serverData.get(0), hostname,
						   port, ServerMode.READ_WRITE, role, 1.0);
		} else if (serverData.size() == 7) {
			// second format
			String uuid = (String)serverData.get(0);
			String groupName = (String)serverData.get(1);
			String hostname = (String)serverData.get(2);
			int port = Integer.valueOf((String)serverData.get(3));
			ServerMode mode = ServerMode.getFromConstant((Integer)serverData.get(4));
			ServerRole role = ServerRole.getFromConstant((Integer)serverData.get(5));
			double weight = (Double)serverData.get(6);
			s = new Server(groupName, uuid, hostname, port, mode, role, weight);
		}
		return s;
	}

    /**
     * Convert a list of string/string/bool to Server objects.
     */
    private static Set<Server> toServerSet(List<List> l) throws FabricCommunicationException {
		Set<Server> servers = new HashSet<Server>();
		for (List serverData : l) {
			Server s = unmarshallServer(serverData);
			if (s == null) {
				throw new FabricCommunicationException("Unknown format of server object");
			}
			servers.add(s);
		}
		return servers;
    }

	/**
	 * Call a method and return the result only if the call is successful.
	 *
	 * @throws FabricCommunicationException If comm fails or the server reports that the method call failed.
	 */
	private Object errorSafeCallMethod(String methodName, Object args[]) throws FabricCommunicationException {
		List<?> responseData = this.methodCaller.call(methodName, args);
		Response response = new Response(responseData);
		if (!response.isSuccessful()) {
			throw new FabricCommunicationException("Call failed to method `" + methodName + "':\n" + response.getTraceString());
		}
		return response.getReturnValue();
	}

	/**
	 * Call a dump.* method.
	 */
	private DumpResponse callDumpMethod(String methodName, Object args[])
		throws FabricCommunicationException {
		List<?> responseData = this.methodCaller.call(methodName, args);
		return new DumpResponse(responseData);
	}

	/**
	 * Return a list of Fabric servers.
	 */
    public List<String> getFabricNames() throws FabricCommunicationException {
		return callDumpMethod("dump.fabric_nodes", new Object[] {}).getReturnValue();
    }

	/**
	 * Return a list of groups present in this fabric.
	 */
    public Set<String> getGroupNames() throws FabricCommunicationException {
		Set<String> groupNames = new HashSet<String>();
		for (HashMap<String, String> wrapped : (List<HashMap<String, String>>)errorSafeCallMethod("group.lookup_groups", null)) {
			groupNames.add(wrapped.get("group_id"));
		}
		return groupNames;
    }

    public ServerGroup getServerGroup(String groupName) throws FabricCommunicationException {
		Set<ServerGroup> groups = getServerGroups(groupName).getData();
		if (groups.size() == 1) {
			return groups.iterator().next();
		}
		return null;
    }

    public Set<Server> getServersForKey(String tableName, int key) throws FabricCommunicationException {
		return toServerSet((List<List>)errorSafeCallMethod("sharding.lookup_servers", new Object[] {tableName, key}));
    }

	/**
	 * Facade for "dump.servers".
	 */
	public FabricStateResponse<Set<ServerGroup>> getServerGroups(String groupPattern) throws FabricCommunicationException {
		int version = 0; // necessary but unused
		DumpResponse response = callDumpMethod("dump.servers", new Object[] {version, groupPattern});
		// collect all servers by group name
		Map<String, Set<Server>> serversByGroupName = new HashMap<String, Set<Server>>();
		for (List server : (List<List>) response.getReturnValue()) {
			Server s = unmarshallServer(server);
			if (serversByGroupName.get(s.getGroupName()) == null) {
				serversByGroupName.put(s.getGroupName(), new HashSet<Server>());
			}
			serversByGroupName.get(s.getGroupName()).add(s);
		}
		// create group set
		Set<ServerGroup> serverGroups = new HashSet<ServerGroup>();
		for (Map.Entry<String, Set<Server>> entry : serversByGroupName.entrySet()) {
			ServerGroup g = new ServerGroup(entry.getKey(), entry.getValue());
			serverGroups.add(g);
		}
		return new FabricStateResponse<Set<ServerGroup>>(serverGroups, response.getTtl());
	}

	public FabricStateResponse<Set<ServerGroup>> getServerGroups() throws FabricCommunicationException {
		return getServerGroups("");
	}

	private FabricStateResponse<Set<ShardTable>> getShardTables(String shardMappingId) throws FabricCommunicationException {
		int version = 0;
		Object args[] = new Object[] {version, shardMappingId};
		DumpResponse tablesResponse = callDumpMethod("dump.shard_tables", args);
		Set<ShardTable> tables = new HashSet<ShardTable>();
		// construct the tables
		for (List rawTable : (List<List>) tablesResponse.getReturnValue()) {
			String database = (String)rawTable.get(0);
			String table = (String)rawTable.get(1);
			String column = (String)rawTable.get(2);
			String mappingId = (String)rawTable.get(3);
			ShardTable st = new ShardTable(database, table, column);
			tables.add(st);
		}
		return new FabricStateResponse<Set<ShardTable>>(tables, tablesResponse.getTtl());
	}

	private FabricStateResponse<Set<ShardIndex>> getShardIndices(String shardMappingId) throws FabricCommunicationException {
		int version = 0;
		Object args[] = new Object[] {version, shardMappingId};
		DumpResponse indexResponse = callDumpMethod("dump.shard_index", args);
		Set<ShardIndex> indices = new HashSet<ShardIndex>();

		// construct the index
		for (List rawIndexEntry : (List<List>) indexResponse.getReturnValue()) {
			String bound = (String)rawIndexEntry.get(0);
			String mappingId = (String)rawIndexEntry.get(1);
			String shardId = (String)rawIndexEntry.get(2);
			String groupName = (String)rawIndexEntry.get(3);
			ShardIndex si = new ShardIndex(bound, Integer.valueOf(shardId), groupName);
			indices.add(si);
		}
		return new FabricStateResponse<Set<ShardIndex>>(indices, indexResponse.getTtl());
	}

	/**
	 * Retrieve a set of complete shard mappings. The returned mappings include all information
	 * available about the mapping.
	 * @param shardMappingIdPattern the shard mapping id to retrieve
	 */
	public FabricStateResponse<Set<ShardMapping>> getShardMappings(String shardMappingIdPattern) throws FabricCommunicationException {
		int version = 0;
		Object args[] = new Object[] {version, shardMappingIdPattern}; // common to all calls
		DumpResponse mapsResponse = callDumpMethod("dump.shard_maps", args);
		// use the lowest ttl of all the calls
		long minExpireTimeMillis = System.currentTimeMillis() + (1000 * mapsResponse.getTtl());

		// construct the maps
		Set<ShardMapping> mappings = new HashSet<ShardMapping>();
		for (List rawMapping : (List<List>) mapsResponse.getReturnValue()) {
			String mappingId = (String)rawMapping.get(0);
			ShardingType shardingType = ShardingType.valueOf((String)rawMapping.get(1));
			String globalGroupName = (String)rawMapping.get(2);

			FabricStateResponse<Set<ShardTable>> tables = getShardTables(mappingId);
			FabricStateResponse<Set<ShardIndex>> indices = getShardIndices(mappingId);

			if (tables.getExpireTimeMillis() < minExpireTimeMillis)
				minExpireTimeMillis = tables.getExpireTimeMillis();
			if (indices.getExpireTimeMillis() < minExpireTimeMillis)
				minExpireTimeMillis = indices.getExpireTimeMillis();

			ShardMapping m = new ShardMappingFactory().createShardMapping(mappingId, shardingType, globalGroupName,
																		  tables.getData(), indices.getData());
			mappings.add(m);
		}

		return new FabricStateResponse<Set<ShardMapping>>(mappings, minExpireTimeMillis);
	}

	public FabricStateResponse<Set<ShardMapping>> getShardMappings() throws FabricCommunicationException {
		return getShardMappings("");
	}

	/**
	 * Create a new HA group.
	 */
	public void createGroup(String groupName) throws FabricCommunicationException {
		errorSafeCallMethod("group.create", new Object[] {groupName});
	}

	/**
	 * Create a new server in the given group.
	 */
	public void createServerInGroup(String groupName, String hostname, int port)
		throws FabricCommunicationException {
		errorSafeCallMethod("group.add", new Object[] {groupName, hostname + ":" + port});
	}

	/**
	 * Create a new shard mapping.
	 *
	 * @param type method by which data is distributed to shards
	 * @param globalGroupName name of global group of the shard mapping
	 * @returns id of the new shard mapping.
	 */
	public int createShardMapping(ShardingType type, String globalGroupName) throws FabricCommunicationException {
		return (Integer)errorSafeCallMethod("sharding.create_definition", new Object[] {type.toString(), globalGroupName});
	}

	public void createShardTable(int shardMappingId, String database, String table, String column) throws FabricCommunicationException {
		errorSafeCallMethod("sharding.add_table", new Object[] {shardMappingId, database + "." + table, column});
	}

	public void createShardIndex(int shardMappingId, String groupNameLowerBoundList) throws FabricCommunicationException {
		String status = "ENABLED";
		errorSafeCallMethod("sharding.add_shard", new Object[] {shardMappingId, groupNameLowerBoundList, status});
	}

	public void promoteServerInGroup(String groupName, String hostname, int port) throws FabricCommunicationException {
		ServerGroup serverGroup = getServerGroup(groupName);
		for (Server s : serverGroup.getServers()) {
			if (s.getHostname().equals(hostname) &&
				s.getPort() == port) {
				errorSafeCallMethod("group.promote", new Object[] {groupName, s.getUuid()});
				break;
			}
		}
	}

	public void reportServerError(Server server, String errorDescription, boolean forceFaulty) throws FabricCommunicationException {
		String reporter = "MySQL Connector/J";
		String command = "threat.report_error";
		if (forceFaulty) {
			command = "threat.report_failure";
		}
		errorSafeCallMethod(command, new Object[] {server.getUuid(), reporter, errorDescription});
	}
}
