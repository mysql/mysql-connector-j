/*
  Copyright (c) 2013, 2017, Oracle and/or its affiliates. All rights reserved.

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

import java.math.BigInteger;
import java.security.MessageDigest;
import java.util.Comparator;
import java.util.Set;
import java.util.TreeSet;

/**
 * A shard mapping with ranges defined by hashing key values. Lookups work essentially
 * the same as in {@link RangeShardMapping} but strings are compared as opposed to ints.
 */
public class HashShardMapping extends ShardMapping {
    private static class ReverseShardIndexSorter implements Comparator<ShardIndex> {
        public int compare(ShardIndex i1, ShardIndex i2) {
            return i2.getBound().compareTo(i1.getBound());
        }

        // singleton instance
        public static final ReverseShardIndexSorter instance = new ReverseShardIndexSorter();
    }

    private static final MessageDigest md5Hasher;

    static {
        try {
            md5Hasher = MessageDigest.getInstance("MD5");
        } catch (java.security.NoSuchAlgorithmException ex) {
            throw new ExceptionInInitializerError(ex);
        }
    }

    public HashShardMapping(int mappingId, ShardingType shardingType, String globalGroupName, Set<ShardTable> shardTables, Set<ShardIndex> shardIndices) {
        super(mappingId, shardingType, globalGroupName, shardTables, new TreeSet<ShardIndex>(ReverseShardIndexSorter.instance));
        this.shardIndices.addAll(shardIndices);
    }

    @Override
    protected ShardIndex getShardIndexForKey(String stringKey) {
        String hashedKey;
        synchronized (md5Hasher) {
            hashedKey = new BigInteger(/* unsigned/positive */1, md5Hasher.digest(stringKey.getBytes())).toString(16).toUpperCase();
        }

        // pad out to 32 digits
        for (int i = 0; i < (32 - hashedKey.length()); ++i) {
            hashedKey = "0" + hashedKey;
        }

        for (ShardIndex i : this.shardIndices) {
            if (i.getBound().compareTo(hashedKey) <= 0) {
                return i;
            }
        }

        // default to the first (highest) bound,
        // implementing wrapping
        return this.shardIndices.iterator().next();
    }
}
