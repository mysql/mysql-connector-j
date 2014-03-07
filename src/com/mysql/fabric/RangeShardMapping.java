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

import java.util.Comparator;
import java.util.Set;
import java.util.TreeSet;

/**
 * A shard mapping that partitions data by ranges.
 */
public class RangeShardMapping extends ShardMapping {
	/**
	 * A sorter that sorts shard indices from highest to lowest based on the integer
	 * value of their bounds. For a range shard mapping, the bound is a lowest bound.
	 */
	private static class RangeShardIndexSorter implements Comparator<ShardIndex> {
		public int compare(ShardIndex i1, ShardIndex i2) {
			Integer bound1, bound2;
			bound1 = Integer.parseInt(i1.getBound());
			bound2 = Integer.parseInt(i2.getBound());
			return bound2.compareTo(bound1); // this reverses it
		}
		// singleton instance
		public static final RangeShardIndexSorter instance  = new RangeShardIndexSorter();
	}

	public RangeShardMapping(String mappingId, ShardingType shardingType, String globalGroupName,
							 Set<ShardTable> shardTables, Set<ShardIndex> shardIndices) {
		// sort shard indices eagerly so {@link getShardIndexForKey} has them in the necessary order
		super(mappingId, shardingType, globalGroupName, shardTables, new TreeSet<ShardIndex>(RangeShardIndexSorter.instance));
		this.shardIndices.addAll(shardIndices);
	}

	/**
	 * Search through the shard indicies to find the shard holding this key. Range-based sharding
	 * defines a lower bound for each partition with the upper bound being one less than the lower bound
	 * of the next highest shard. There is no upper bound for the shard with the highest lower bound.
	 */
	protected ShardIndex getShardIndexForKey(String stringKey) {
		Integer key = -1;
		key = Integer.parseInt(stringKey);
		for (ShardIndex i : this.shardIndices) {
			Integer lowerBound = Integer.valueOf(i.getBound());
			if (key >= lowerBound)
				return i;
		}
		return null;
	}
}
