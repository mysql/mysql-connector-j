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

package com.mysql.fabric.jdbc;

import java.sql.SQLException;
import java.util.Set;
import com.mysql.fabric.ServerGroup;

/**
 *
 */
public interface FabricMySQLConnection extends com.mysql.jdbc.MySQLConnection {
	/**
	 * Clear all the state that is used to determine which server to
	 * send queries to.
	 */
	void clearServerSelectionCriteria() throws SQLException;

	/**
	 * Set the shard key for the data being accessed.
	 */
	void setShardKey(String shardKey) throws SQLException;

	/**
	 * Get the shard key for the data being accessed.
	 */
	String getShardKey();

	/**
	 * Set the table being accessed. Can be a table name or a
	 * "database.table" pair. The table must be known by Fabric
	 * as a sharded table.
	 */
	void setShardTable(String shardTable) throws SQLException;

	/**
	 * Get the table being accessed.
	 */
	String getShardTable();

	/**
	 * Set the server group name to connect to. Direct server group selection
	 * is mutually exclusive of sharded data access.
	 */
	void setServerGroupName(String serverGroupName) throws SQLException;

	/**
	 * Get the server group name when using direct server group selection.
	 */
	String getServerGroupName();

	/**
	 * Get the current server group.
	 * @returns The currently chosen group if sufficient server group selection
	 *          criteria has been provided. Otherwise null.
	 */
	ServerGroup getCurrentServerGroup();

	/**
	 * Clear the list of tables for the last query. This also clears the
	 * shard mapping/table and must be given again for the next query via
	 * {@link setShardTable} or {@addQueryTable}.
	 */
	void clearQueryTables() throws SQLException;

	/**
	 * Add a table to the set of tables used for the next query on this connection.
	 * This is used for:
	 * <ul>
	 * <li>Choosing a shard given the tables used</li>
	 * <li>Preventing cross-shard queries</li>
	 * </ul>
	 */
	void addQueryTable(String tableName) throws SQLException;

	/**
	 * The set of tables to be used in the next query on this connection.
	 */
	Set<String> getQueryTables();
}
