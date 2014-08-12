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

package com.mysql.jdbc;

import java.sql.SQLException;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class PerVmServerConfigCacheFactory implements CacheAdapterFactory<String, Map<String, String>> {
    static final ConcurrentHashMap<String, Map<String, String>> serverConfigByUrl = new ConcurrentHashMap<String, Map<String, String>>();

    private static final CacheAdapter<String, Map<String, String>> serverConfigCache = new CacheAdapter<String, Map<String, String>>() {

        public Map<String, String> get(String key) {
            return serverConfigByUrl.get(key);
        }

        public void put(String key, Map<String, String> value) {
            serverConfigByUrl.putIfAbsent(key, value);
        }

        public void invalidate(String key) {
            serverConfigByUrl.remove(key);
        }

        public void invalidateAll(Set<String> keys) {
            for (String key : keys) {
                serverConfigByUrl.remove(key);
            }
        }

        public void invalidateAll() {
            serverConfigByUrl.clear();
        }
    };

    public CacheAdapter<String, Map<String, String>> getInstance(Connection forConn, String url, int cacheMaxSize, int maxKeySize,
            Properties connectionProperties) throws SQLException {
        return serverConfigCache;
    }
}
