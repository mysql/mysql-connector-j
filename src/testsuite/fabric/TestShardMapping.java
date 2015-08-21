/*
  Copyright (c) 2013, 2014, Oracle and/or its affiliates. All rights reserved.

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

package testsuite.fabric;

import java.util.HashSet;
import java.util.Set;

import junit.framework.TestCase;

import com.mysql.fabric.HashShardMapping;
import com.mysql.fabric.RangeShardMapping;
import com.mysql.fabric.ShardIndex;
import com.mysql.fabric.ShardMapping;
import com.mysql.fabric.ShardingType;

/**
 * Tests for shard mappings.
 */
public class TestShardMapping extends TestCase {
    /**
     * Test that a range based shard mapping looks up groups for keys correctly.
     */
    public void testRangeShardMappingKeyLookup() throws Exception {
        final String globalGroupName = "My global group";

        final int lowerBounds[] = new int[] { 1, 10000, 1001, 400, 1000, 470 };

        final int lowestLowerBound = 1;

        // setup the mapping
        Set<ShardIndex> shardIndices = new HashSet<ShardIndex>();
        int shardId = 0; // shard id's added sequentially increasing from 0
        for (Integer lowerBound : lowerBounds) {
            ShardIndex i = new ShardIndex(String.valueOf(lowerBound), shardId, "shard_group_" + shardId);
            shardId++;
            shardIndices.add(i);
        }
        ShardMapping mapping = new RangeShardMapping(5000, ShardingType.RANGE, globalGroupName, null, shardIndices);

        // try adding a second shard index with a lower bound that conflicts with an existing one this should be prohibited 
        // mapping.addShardIndex(new ShardIndex(mapping, "1", 0, ""));

        // test looking up a key out of range doesn't work
        try {
            mapping.getGroupNameForKey(String.valueOf(lowestLowerBound - 1));
            fail("Looking up a key with a value below the lowest bound is invalid");
        } catch (Exception ex) {
        }
        try {
            mapping.getGroupNameForKey(String.valueOf(lowestLowerBound - 1000));
            fail("Looking up a key with a value below the lowest bound is invalid");
        } catch (Exception ex) {
        }

        // test key lookups for lower bound values
        for (shardId = 0; shardId < lowerBounds.length; ++shardId) {
            int lowerBound = lowerBounds[shardId];
            String groupName = mapping.getGroupNameForKey(String.valueOf(lowerBound));
            assertEquals("Exact lookup for key " + lowerBound, "shard_group_" + shardId, groupName);
        }
    }

    public void testHashShardMappingKeyLookup() throws Exception {
        final String globalGroupName = "My global group";

        final String lowerBounds[] = new String[] {
        /* 0 = */"AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
        /* 1 = */"66666666666666666666666666666666",
        /* 2 = */"2809A05A22A4A9C1882A580BCC0AD8A6",
        /* 3 = */"DDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDD" };

        // setup the mapping
        Set<ShardIndex> shardIndices = new HashSet<ShardIndex>();
        int shardId = 0; // shard id's added sequentially increasing from 0
        for (String lowerBound : lowerBounds) {
            ShardIndex i = new ShardIndex(lowerBound, shardId, "server_group_" + shardId);
            shardId++;
            shardIndices.add(i);
        }
        ShardMapping mapping = new HashShardMapping(5000, ShardingType.HASH, globalGroupName, null, shardIndices);

        // test lookups mapping of test value to the group it maps to test values are hashed with MD5 and compared to lowerBounds values
        String testPairs[][] = new String[][] {
                // exact match should be in that shard
                new String[] { "Jess", "server_group_2" }, // hash = 2809a05a22a4a9c1882a580bcc0ad8a6
                new String[] { "x", "server_group_1" }, // hash = 9dd4e461268c8034f5c8564e155c67a6
                new String[] { "X", "server_group_3" }, // hash = 02129bb861061d1a052c592e2dc6b383
                new String[] { "Y", "server_group_2" }, // hash = 57cec4137b614c87cb4e24a3d003a3e0
                new String[] { "g", "server_group_0" }, // hash = b2f5ff47436671b6e533d8dc3614845d
                // leading zeroes
                new String[] { "168", "server_group_3" }, // hash = 006f52e9102a8d3be2fe5614f42ba989
        };

        for (String[] testPair : testPairs) {
            String key = testPair[0];
            String serverGroup = testPair[1];
            assertEquals(serverGroup, mapping.getGroupNameForKey(key));
        }

        // test a random set of values. we should never return null
        for (int i = 0; i < 1000; ++i) {
            assertNotNull(mapping.getGroupNameForKey("" + i));
        }
    }
}
