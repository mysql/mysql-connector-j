/*
  Copyright (c) 2016, Oracle and/or its affiliates. All rights reserved.

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

package com.mysql.cj.core.conf.url;

import static com.mysql.cj.core.conf.PropertyDefinitions.DBNAME_PROPERTY_KEY;
import static com.mysql.cj.core.util.StringUtils.isNullOrEmpty;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import com.mysql.cj.api.conf.DatabaseUrlContainer;
import com.mysql.cj.core.conf.PropertyDefinitions;

/**
 * This class holds the following MySQL host information:
 * <ul>
 * <li>host: an IP or host name.
 * <li>port: the port number or 0 if not known.
 * <li>user info: an {@link UserInfo} instance containing the user name and password.
 * <li>host properties: host specific connection arguments.
 * </ul>
 */
public class HostInfo implements DatabaseUrlContainer {
    private static final String HOST_PORT_SEPARATOR = ":";

    private final DatabaseUrlContainer originalUrl;
    private final String host;
    private final int port;
    private final String user;
    private final String password;
    private final Map<String, String> hostProperties = new HashMap<>();

    /**
     * Constructs an empty {@link HostInfo} instance.
     */
    public HostInfo() {
        this(null, null, -1, null, null);
    }

    /**
     * Constructs a {@link HostInfo} instance initialized with the provided host, port and user info.
     * 
     * @param url
     *            a reference to the original database URL that produced this host info
     * @param host
     *            the host ip or name
     * @param port
     *            the port
     * @param user
     *            the user name
     * @param password
     *            the user's password
     */
    public HostInfo(DatabaseUrlContainer url, String host, int port, String user, String password) {
        this(url, host, port, user, password, null);
    }

    /**
     * Constructs a {@link HostInfo} instance initialized with the provided host, port, user, password and connection arguments.
     * 
     * @param url
     *            a reference to the original database URL that produced this host info
     * @param host
     *            the host ip or name
     * @param port
     *            the port
     * @param user
     *            the user name
     * @param password
     *            this user's password
     * @param properties
     *            a connection arguments map.
     */
    public HostInfo(DatabaseUrlContainer url, String host, int port, String user, String password, Map<String, String> properties) {
        this.originalUrl = url;
        this.host = host;
        this.port = port;
        this.user = user;
        this.password = password;
        if (properties != null) {
            this.hostProperties.putAll(properties);
        }
    }

    /**
     * Returns the host.
     * 
     * @return the host
     */
    public String getHost() {
        return this.host;
    }

    /**
     * Returns the port.
     * 
     * @return the port
     */
    public int getPort() {
        return this.port;
    }

    /**
     * Returns a host:port representation of this host.
     * 
     * @return the host:port representation of this host
     */
    public String getHostPortPair() {
        return this.host + HOST_PORT_SEPARATOR + this.port;
    }

    /**
     * Returns the user name.
     * 
     * @return the user name
     */
    public String getUser() {
        return this.user;
    }

    /**
     * Returns the password.
     * 
     * @return the password
     */
    public String getPassword() {
        return this.password;
    }

    /**
     * Returns the properties specific to this host.
     * 
     * @return this host specific properties
     */
    public Map<String, String> getHostProperties() {
        return Collections.unmodifiableMap(this.hostProperties);
    }

    /**
     * Returns the connection argument for the given key.
     * 
     * @return the connection argument for the given key
     */
    public String getProperty(String key) {
        return this.hostProperties.get(key);
    }

    /**
     * Shortcut to the database connection argument.
     * 
     * @return the database name
     */
    public String getDatabase() {
        String database = this.hostProperties.get(DBNAME_PROPERTY_KEY);
        return isNullOrEmpty(database) ? "" : database;
    }

    /**
     * Exposes this host info as a single properties instance. The values for host, port, user and password are added to the properties map with their standard
     * keys.
     * 
     * @return a {@link Properties} instance containing the full host information.
     */
    public Properties exposeAsProperties() {
        Properties props = new Properties();
        this.hostProperties.entrySet().stream().forEach(e -> props.setProperty(e.getKey(), e.getValue() == null ? "" : e.getValue()));
        props.setProperty(PropertyDefinitions.HOST_PROPERTY_KEY, getHost());
        props.setProperty(PropertyDefinitions.PORT_PROPERTY_KEY, String.valueOf(getPort()));
        props.setProperty(PropertyDefinitions.PNAME_user, getUser());
        props.setProperty(PropertyDefinitions.PNAME_password, getPassword());
        return props;
    }

    /**
     * Returns the original database URL that produced this host info.
     * 
     * @return the original database URL
     */
    @Override
    public String getDatabaseUrl() {
        return this.originalUrl != null ? this.originalUrl.getDatabaseUrl() : "";
    }

    /**
     * Returns a string representation of this object.
     * 
     * @return a string representation of this object
     */
    @Override
    public String toString() {
        StringBuilder asStr = new StringBuilder(super.toString());
        asStr.append(String.format(" :: {host: \"%s\", port: %d, user: %s, password: %s, hostProperties: %s}", this.host, this.port, this.user, this.password,
                this.hostProperties));
        return asStr.toString();
    }
}
