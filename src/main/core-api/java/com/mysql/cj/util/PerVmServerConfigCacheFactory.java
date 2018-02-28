/*
 * Copyright (c) 2013, 2018, Oracle and/or its affiliates. All rights reserved.
 *
 * This program is free software; you can redistribute it and/or modify it under
 * the terms of the GNU General Public License, version 2.0, as published by the
 * Free Software Foundation.
 *
 * This program is also distributed with certain software (including but not
 * limited to OpenSSL) that is licensed under separate terms, as designated in a
 * particular file or component or in included license documentation. The
 * authors of MySQL hereby grant you an additional permission to link the
 * program and your derivative works with the separately licensed software that
 * they have included with MySQL.
 *
 * Without limiting anything contained in the foregoing, this file, which is
 * part of MySQL Connector/J, is also subject to the Universal FOSS Exception,
 * version 1.0, a copy of which can be found at
 * http://oss.oracle.com/licenses/universal-foss-exception.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 * FOR A PARTICULAR PURPOSE. See the GNU General Public License, version 2.0,
 * for more details.
 *
 * You should have received a copy of the GNU General Public License along with
 * this program; if not, write to the Free Software Foundation, Inc.,
 * 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA
 */

package com.mysql.cj.util;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import com.mysql.cj.CacheAdapter;
import com.mysql.cj.CacheAdapterFactory;

public class PerVmServerConfigCacheFactory implements CacheAdapterFactory<String, Map<String, String>> {
    static final ConcurrentHashMap<String, Map<String, String>> serverConfigByUrl = new ConcurrentHashMap<>();

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

    public CacheAdapter<String, Map<String, String>> getInstance(Object syncMutex, String url, int cacheMaxSize, int maxKeySize) {
        return serverConfigCache;
    }
}
