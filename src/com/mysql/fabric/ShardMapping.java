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

package com.mysql.fabric;

import java.util.Collections;
import java.util.Set;

/**
 * A shard mapping representing this set of sharded data.
 */
public abstract class ShardMapping {
	private String mappingId;
	private ShardingType shardingType;
	private String globalGroupName;
	protected Set<ShardTable> shardTables;
	protected Set<ShardIndex> shardIndices;

	public ShardMapping(String mappingId, ShardingType shardingType, String globalGroupName,
						Set<ShardTable> shardTables, Set<ShardIndex> shardIndices) {
		this.mappingId = mappingId;
		this.shardingType = shardingType;
		this.globalGroupName = globalGroupName;
		this.shardTables = shardTables;
		this.shardIndices = shardIndices;
	}

	/**
	 * Lookup the server group that stores the given key.
	 */
	public String getGroupNameForKey(String key) {
		return getShardIndexForKey(key).getGroupName();
	}

	/**
	 * Decide which shard index stores the given key.
	 */
	protected abstract ShardIndex getShardIndexForKey(String key);

	/**
	 * The ID of this mapping.
	 */
	public String getMappingId() {
		return this.mappingId;
	}

	/**
	 * The {@link ShardingType} of this mapping.
	 */
	public ShardingType getShardingType() {
		return this.shardingType;
	}

	/**
	 * The name of the global group for this shard map.
	 */
	public String getGlobalGroupName() {
		return this.globalGroupName;
	}

	/**
	 * Return the set of tables sharded in this mapping.
	 */
	public Set<ShardTable> getShardTables() {
		return Collections.unmodifiableSet(this.shardTables);
	}

	/**
	 * Return the set of shards in this mapping.
	 */
	public Set<ShardIndex> getShardIndices() {
		return Collections.unmodifiableSet(this.shardIndices);
	}
}
