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
	 */
	private static Server unmarshallServer(Map serverData) {
		ServerMode mode;
		ServerRole role;

		String host;
		int port;

		// dump.servers returns integer mode/status
		if (Integer.class.equals(serverData.get("mode").getClass())) {
			mode = ServerMode.getFromConstant((Integer) serverData.get("mode"));
			role = ServerRole.getFromConstant((Integer) serverData.get("status"));
			host = (String) serverData.get("host");
			port = (Integer) serverData.get("port");
		} else {
			// sharding.lookup_servers returns a different format
			mode = Enum.valueOf(ServerMode.class, (String) serverData.get("mode"));
			role = Enum.valueOf(ServerRole.class, (String) serverData.get("status"));
			String hostnameAndPort[] = ((String) serverData.get("address")).split(":");
			host = hostnameAndPort[0];
			port = Integer.valueOf(hostnameAndPort[1]);
		}
		Server s = new Server((String) serverData.get("group_id"),
							  (String) serverData.get("server_uuid"),
							  host, port, mode, role,
							  (Double) serverData.get("weight"));
		return s;
	}

    /**
     * Convert a list of string/string/bool to Server objects.
     */
    private static Set<Server> toServerSet(List<Map> l) throws FabricCommunicationException {
		Set<Server> servers = new HashSet<Server>();
		for (Map serverData : l) {
			servers.add(unmarshallServer(serverData));
		}
		return servers;
    }

	/**
	 * Call a method and return the result only if the call is successful.
	 *
	 * @throws FabricCommunicationException If comm fails or the server reports that the method call failed.
	 */
	private Response errorSafeCallMethod(String methodName, Object args[]) throws FabricCommunicationException {
		List<?> responseData = this.methodCaller.call(methodName, args);
		Response response = new Response(responseData);
		if (response.getErrorMessage() != null) {
			throw new FabricCommunicationException("Call failed to method `" + methodName + "':\n" + response.getErrorMessage());
		}
		return response;
	}

	/**
	 * Return a list of Fabric servers.
	 */
    public Set<String> getFabricNames() throws FabricCommunicationException {
		Response resp = errorSafeCallMethod("dump.fabric_nodes", new Object[] {});
		Set<String> names = new HashSet<String>();
		for (Map node : resp.getResultSet()) {
			names.add(node.get("host") + ":" + node.get("port"));
		}
		return names;
    }

	/**
	 * Return a list of groups present in this fabric.
	 */
    public Set<String> getGroupNames() throws FabricCommunicationException {
		Set<String> groupNames = new HashSet<String>();
		for (Map row : errorSafeCallMethod("group.lookup_groups", null).getResultSet()) {
			groupNames.add((String) row.get("group_id"));
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
		Response r = errorSafeCallMethod("sharding.lookup_servers", new Object[] {tableName, key});
		return toServerSet(r.getResultSet());
    }

	/**
	 * Facade for "dump.servers". Will not return empty server groups.
	 */
	public FabricStateResponse<Set<ServerGroup>> getServerGroups(String groupPattern) throws FabricCommunicationException {
		int version = 0; // necessary but unused
		Response response = errorSafeCallMethod("dump.servers", new Object[] {version, groupPattern});
		// collect all servers by group name
		Map<String, Set<Server>> serversByGroupName = new HashMap<String, Set<Server>>();
		for (Map server : response.getResultSet()) {
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

	private FabricStateResponse<Set<ShardTable>> getShardTables(int shardMappingId) throws FabricCommunicationException {
		int version = 0;
		Object args[] = new Object[] {version, String.valueOf(shardMappingId)};
		Response tablesResponse = errorSafeCallMethod("dump.shard_tables", args);
		Set<ShardTable> tables = new HashSet<ShardTable>();
		// construct the tables
		for (Map rawTable : tablesResponse.getResultSet()) {
			String database = (String) rawTable.get("schema_name");
			String table = (String) rawTable.get("table_name");
			String column = (String) rawTable.get("column_name");
			ShardTable st = new ShardTable(database, table, column);
			tables.add(st);
		}
		return new FabricStateResponse<Set<ShardTable>>(tables, tablesResponse.getTtl());
	}

	private FabricStateResponse<Set<ShardIndex>> getShardIndices(int shardMappingId) throws FabricCommunicationException {
		int version = 0;
		Object args[] = new Object[] {version, String.valueOf(shardMappingId)};
		Response indexResponse = errorSafeCallMethod("dump.shard_index", args);
		Set<ShardIndex> indices = new HashSet<ShardIndex>();

		// construct the index
		for (Map rawIndexEntry : indexResponse.getResultSet()) {
			String bound = (String) rawIndexEntry.get("lower_bound");
			int shardId = (Integer) rawIndexEntry.get("shard_id");
			String groupName = (String) rawIndexEntry.get("group_id");
			ShardIndex si = new ShardIndex(bound, shardId, groupName);
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
		Response mapsResponse = errorSafeCallMethod("dump.shard_maps", args);
		// use the lowest ttl of all the calls
		long minExpireTimeMillis = System.currentTimeMillis() + (1000 * mapsResponse.getTtl());

		// construct the maps
		Set<ShardMapping> mappings = new HashSet<ShardMapping>();
		for (Map rawMapping : mapsResponse.getResultSet()) {
			int mappingId = (Integer) rawMapping.get("mapping_id");
			ShardingType shardingType = ShardingType.valueOf((String) rawMapping.get("type_name"));
			String globalGroupName = (String) rawMapping.get("global_group_id");

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
		Response r = errorSafeCallMethod("sharding.create_definition", new Object[] {type.toString(), globalGroupName});
		return (Integer) r.getResultSet().get(0).get("result");
	}

	public void createShardTable(int shardMappingId, String database, String table, String column) throws FabricCommunicationException {
		errorSafeCallMethod("sharding.add_table", new Object[] {shardMappingId, database + "." + table, column});
	}

	public void createShardIndex(int shardMappingId, String groupNameLowerBoundList) throws FabricCommunicationException {
		String status = "ENABLED";
		errorSafeCallMethod("sharding.add_shard", new Object[] {shardMappingId, groupNameLowerBoundList, status});
	}

	public void addServerToGroup(String groupName, String hostname, int port)
		throws FabricCommunicationException {
		errorSafeCallMethod("group.add", new Object[] {groupName, hostname + ":" + port});
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
