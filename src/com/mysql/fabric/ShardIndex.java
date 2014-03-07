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

/**
 * A shard index represents the physical location of a segment of data. The data segment
 * is identified by it's key's relation to the `bound' value.
 */
public class ShardIndex {
	private String bound;
	private Integer shardId;
	private String groupName;

	public ShardIndex(String bound, Integer shardId, String groupName) {
		this.bound = bound;
		this.shardId = shardId;
		this.groupName = groupName;
	}

	/**
	 * The bound that the key will be compared to. This is treated different based on the
	 * ShardingType.
	 */
	public String getBound() {
		return this.bound;
	}

	/**
	 * A unique identified for this shard.
	 */
	public Integer getShardId() {
		return this.shardId;
	}

	/**
	 * The name of the group in the data for this shard resides.
	 */
	public String getGroupName() {
		return this.groupName;
	}
}
