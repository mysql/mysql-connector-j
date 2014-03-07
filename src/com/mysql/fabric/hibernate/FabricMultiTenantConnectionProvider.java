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

package com.mysql.fabric.hibernate;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import org.hibernate.service.jdbc.connections.spi.MultiTenantConnectionProvider;

import com.mysql.fabric.*;

/**
 * Multi-tenancy connection provider for Hibernate 4.
 *
 * http://docs.jboss.org/hibernate/orm/4.1/javadocs/org/hibernate/service/jdbc/connections/spi/MultiTenantConnectionProvider.html
 */
public class FabricMultiTenantConnectionProvider implements MultiTenantConnectionProvider {

	private static final long serialVersionUID = 1L;

	private FabricConnection fabricConnection;
	private String database;
	private String table; // the sharded table
    private String user;
    private String password;
	private ShardMapping shardMapping;
	private ServerGroup globalGroup;

    public FabricMultiTenantConnectionProvider(String fabricUrl, String database, String table, String user, String password, String fabricUser, String fabricPassword) {
		try {
			this.fabricConnection = new FabricConnection(fabricUrl, fabricUser, fabricPassword);
			this.database = database;
			this.table = table;
			this.user = user;
			this.password = password;
			this.shardMapping = this.fabricConnection.getShardMapping(this.database, this.table);
			this.globalGroup = this.fabricConnection.getServerGroup(this.shardMapping.getGlobalGroupName());
		} catch(FabricCommunicationException ex) {
			throw new RuntimeException(ex);
		}
    }

	/**
	 * Find a server with mode READ_WRITE in the given server group and create a JDBC connection to it.
	 *
	 * @returns a {@link Connection} to an arbitrary MySQL server
	 * @throws SQLException if connection fails or a READ_WRITE server is not contained in the group
	 */
    private Connection getReadWriteConnectionFromServerGroup(ServerGroup serverGroup) throws SQLException {
		for (Server s : serverGroup.getServers()) {
			if (ServerMode.READ_WRITE.equals(s.getMode())) {
				String jdbcUrl = String.format("jdbc:mysql://%s:%s/%s", s.getHostname(), s.getPort(), this.database);
				return DriverManager.getConnection(jdbcUrl, this.user, this.password);
			}
		}
		throw new SQLException("Unable to find r/w server for chosen shard mapping in group " + serverGroup.getName());
    }

	/**
	 * Get a connection that be used to access data or metadata not specific to any shard/tenant.
	 * The returned connection is a READ_WRITE connection to the global group of the shard mapping
	 * for the database and table association with this connection provider.
	 */
    public Connection getAnyConnection() throws SQLException {
		return getReadWriteConnectionFromServerGroup(this.globalGroup);
    }

	/**
	 * Get a connection to access data association with the provided `tenantIdentifier' (or shard
	 * key in Fabric-speak). The returned connection is a READ_WRITE connection.
	 */
    public Connection getConnection(String tenantIdentifier) throws SQLException {
		String serverGroupName = this.shardMapping.getGroupNameForKey(tenantIdentifier);
		try {
			ServerGroup serverGroup = this.fabricConnection.getServerGroup(serverGroupName);
			return getReadWriteConnectionFromServerGroup(serverGroup);
		} catch(FabricCommunicationException ex) {
			throw new RuntimeException(ex);
		}
    }

	/**
	 * Release a non-shard-specific connection.
	 */
    public void releaseAnyConnection(Connection connection) throws SQLException {
		try {
			connection.close();
		} catch(Exception ex) {
			throw new RuntimeException(ex);
		}
    }

	/**
	 * Release a connection specific to `tenantIdentifier'.
	 */
    public void releaseConnection(String tenantIdentifier, Connection connection) throws SQLException {
		releaseAnyConnection(connection);
    }

	/**
	 * We don't track connections.
	 * @returns false
	 */
    public boolean supportsAggressiveRelease() {
		return false;
    }

    public boolean isUnwrappableAs(Class unwrapType) {
		return false;
    }

    public <T> T unwrap(Class<T> unwrapType) {
		return null;
    }
}
