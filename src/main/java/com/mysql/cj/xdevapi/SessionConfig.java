/*
  Copyright (c) 2017, Oracle and/or its affiliates. All rights reserved.

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

package com.mysql.cj.xdevapi;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import com.mysql.cj.core.exceptions.ExceptionFactory;
import com.mysql.cj.core.exceptions.WrongArgumentException;
import com.mysql.cj.core.util.StringUtils;

/**
 * A session configuration object. Contains all the information to create sessions and to customize client applications and persistent storage of session
 * configurations.
 */
public class SessionConfig {
    private final String name;
    private String uri;
    private Map<String, String> appData = new HashMap<>();

    /**
     * Constructs a {@link SessionConfig} with the given name and URI.
     * 
     * @param name
     *            the session configuration name or identifier.
     * @param uri
     *            the session URI.
     */
    public SessionConfig(String name, String uri) {
        if (StringUtils.isEmptyOrWhitespaceOnly(name)) {
            ExceptionFactory.createException(WrongArgumentException.class, "Session configuration name cannot be null or empty.");
        }
        this.name = name;
        setUri(uri);
    }

    /**
     * Retrieves this session configuration name.
     * 
     * @return the session configuration name.
     */
    public String getName() {
        return this.name;
    }

    /**
     * Retrieves this session configuration URI.
     * 
     * @return the session configuration URI.
     */
    public String getUri() {
        return this.uri;
    }

    /**
     * Sets this session configuration URI to the given value.
     * 
     * @param uri
     *            the new URI value to set.
     */
    public void setUri(String uri) {
        if (StringUtils.isEmptyOrWhitespaceOnly(uri)) {
            ExceptionFactory.createException(WrongArgumentException.class, "Session configuration URI cannot be null or empty.");
        }
        this.uri = uri;
    }

    /**
     * Adds the given key/value entry into the client application data map for this session configuration.
     * 
     * @param key
     *            the key of the application data attribute.
     * @param value
     *            the value associated to the given key.
     */
    public void setAppData(String key, String value) {
        if (StringUtils.isEmptyOrWhitespaceOnly(key)) {
            ExceptionFactory.createException(WrongArgumentException.class, "Application Data key cannot be null or empty.");
        }
        this.appData.put(key, value);
    }

    /**
     * Deletes the given key from the client application data map for this session configuration.
     * 
     * @param key
     *            the key to delete from the application data map.
     */
    public void deleteAppData(String key) {
        this.appData.remove(key);
    }

    /**
     * Retrieves the client application data value associated to the given key.
     * 
     * @param key
     *            of the attribute to retrieve.
     * @return the value associated to the given key in the client application data map or {@code null} if the map doesn't contain the key.
     */
    public String getAppData(String key) {
        return this.appData.get(key);
    }

    /**
     * Retrieves the full client application data map.
     * 
     * @return the client application data map.
     */
    public Map<String, String> getAppData() {
        return Collections.unmodifiableMap(this.appData);
    }

    /**
     * Saves this session configuration structure using the {@link SessionConfigManager} API.
     */
    public void save() {
        SessionConfigManager.save(this);
    }
}
