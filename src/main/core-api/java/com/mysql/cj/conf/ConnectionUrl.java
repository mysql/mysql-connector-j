/*
 * Copyright (c) 2016, 2018, Oracle and/or its affiliates. All rights reserved.
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

package com.mysql.cj.conf;

import static com.mysql.cj.util.StringUtils.isNullOrEmpty;

import java.io.IOException;
import java.io.InputStream;
import java.sql.DriverManager;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

import com.mysql.cj.Messages;
import com.mysql.cj.exceptions.CJException;
import com.mysql.cj.exceptions.ExceptionFactory;
import com.mysql.cj.exceptions.InvalidConnectionAttributeException;
import com.mysql.cj.exceptions.WrongArgumentException;
import com.mysql.cj.util.LRUCache;
import com.mysql.cj.util.Util;

/**
 * A container for a database URL and a collection of given connection arguments.
 * The connection string is parsed and split by its components, each of which is then processed and fixed according to the needs of the connection type.
 * This abstract class holds all common behavior to all connection string types. Its subclasses must implement their own specifics such as classifying hosts by
 * type or apply validation rules.
 */
public abstract class ConnectionUrl implements DatabaseUrlContainer {
    private static final String DEFAULT_HOST = "localhost";
    private static final int DEFAULT_PORT = 3306;

    private static final LRUCache<String, ConnectionUrl> connectionUrlCache = new LRUCache<>(100);
    private static final ReadWriteLock rwLock = new ReentrantReadWriteLock();

    /**
     * The rules describing the number of hosts a database URL may contain.
     */
    public enum HostsCardinality {
        SINGLE {
            @Override
            public boolean assertSize(int n) {
                return n == 1;
            }
        },
        MULTIPLE {
            @Override
            public boolean assertSize(int n) {
                return n > 1;
            }
        },
        ONE_OR_MORE {
            @Override
            public boolean assertSize(int n) {
                return n >= 1;
            }
        };

        public abstract boolean assertSize(int n);
    }

    /**
     * The database URL type which is determined by the protocol section of the connection string.
     */
    public enum Type {
        SINGLE_CONNECTION("jdbc:mysql:", HostsCardinality.SINGLE), //
        FAILOVER_CONNECTION("jdbc:mysql:", HostsCardinality.MULTIPLE), //
        LOADBALANCE_CONNECTION("jdbc:mysql:loadbalance:", HostsCardinality.ONE_OR_MORE), //
        REPLICATION_CONNECTION("jdbc:mysql:replication:", HostsCardinality.ONE_OR_MORE), //
        XDEVAPI_SESSION("mysqlx:", HostsCardinality.ONE_OR_MORE);

        private String protocol;
        private HostsCardinality cardinality;

        private Type(String protocol, HostsCardinality cardinality) {
            this.protocol = protocol;
            this.cardinality = cardinality;
        }

        public String getProtocol() {
            return this.protocol;
        }

        public HostsCardinality getCardinality() {
            return this.cardinality;
        }

        /**
         * Returns the {@link Type} corresponding to the given protocol and number of hosts, if any. Otherwise throws an {@link IllegalArgumentException}.
         * Calling this method with the argument n lower than 0 skips the hosts cardinality validation. This should be used for URL protocol validation only as
         * the returned {@link Type} won't won't reliable represent the database URL type.
         * 
         * @param protocol
         *            the protocol
         * @param n
         *            the number of hosts in the database URL
         * @return the {@link Type} corresponding to the given protocol and number of hosts
         */
        public static Type fromValue(String protocol, int n) {
            for (Type t : values()) {
                if (t.getProtocol().equalsIgnoreCase(protocol) && (n < 0 || t.getCardinality().assertSize(n))) {
                    return t;
                }
            }
            if (n < 0) {
                throw ExceptionFactory.createException(WrongArgumentException.class, Messages.getString("ConnectionString.5", new Object[] { protocol }));
            }
            throw ExceptionFactory.createException(WrongArgumentException.class, Messages.getString("ConnectionString.6", new Object[] { protocol, n }));
        }
    }

    protected Type type;
    protected String originalConnStr;
    protected String originalDatabase;
    protected List<HostInfo> hosts = new ArrayList<>();
    protected Map<String, String> properties = new HashMap<>();
    ConnectionPropertiesTransform propertiesTransformer;

    /**
     * Static factory method that returns either a new instance of a {@link ConnectionUrl} or a cached one.
     * Returns "null" it can't handle the connection string.
     * 
     * @param connString
     *            the connection string
     * @param info
     *            the connection arguments map
     * @return an instance of a {@link ConnectionUrl} or "null" if isn't able to handle the connection string
     */
    public static ConnectionUrl getConnectionUrlInstance(String connString, Properties info) {
        if (connString == null) {
            throw ExceptionFactory.createException(WrongArgumentException.class, Messages.getString("ConnectionString.0"));
        }
        String connStringCacheKey = buildConnectionStringCacheKey(connString, info);
        ConnectionUrl connectionString;

        rwLock.readLock().lock();
        connectionString = connectionUrlCache.get(connStringCacheKey);
        if (connectionString == null) {
            rwLock.readLock().unlock();
            rwLock.writeLock().lock();
            try {
                // Check again, in the meantime it could have been cached by another thread.
                connectionString = connectionUrlCache.get(connStringCacheKey);
                if (connectionString == null) {
                    ConnectionUrlParser connStrParser = ConnectionUrlParser.parseConnectionString(connString);
                    try {
                        Type.fromValue(connStrParser.getScheme(), -1);
                    } catch (WrongArgumentException e) {
                        return new ConnectionUrl(connString) {
                        };
                    }

                    switch (Type.fromValue(connStrParser.getScheme(), connStrParser.getHosts().size())) {
                        case SINGLE_CONNECTION:
                            connectionString = (ConnectionUrl) Util.getInstance("com.mysql.cj.conf.url.SingleConnectionUrl",
                                    new Class<?>[] { ConnectionUrlParser.class, Properties.class }, new Object[] { connStrParser, info }, null);
                            break;
                        case FAILOVER_CONNECTION:
                            connectionString = (ConnectionUrl) Util.getInstance("com.mysql.cj.conf.url.FailoverConnectionUrl",
                                    new Class<?>[] { ConnectionUrlParser.class, Properties.class }, new Object[] { connStrParser, info }, null);
                            break;
                        case LOADBALANCE_CONNECTION:
                            connectionString = (ConnectionUrl) Util.getInstance("com.mysql.cj.conf.url.LoadbalanceConnectionUrl",
                                    new Class<?>[] { ConnectionUrlParser.class, Properties.class }, new Object[] { connStrParser, info }, null);
                            break;
                        case REPLICATION_CONNECTION:
                            connectionString = (ConnectionUrl) Util.getInstance("com.mysql.cj.conf.url.ReplicationConnectionUrl",
                                    new Class<?>[] { ConnectionUrlParser.class, Properties.class }, new Object[] { connStrParser, info }, null);
                            break;
                        case XDEVAPI_SESSION:
                            connectionString = (ConnectionUrl) Util.getInstance("com.mysql.cj.conf.url.XDevAPIConnectionUrl",
                                    new Class<?>[] { ConnectionUrlParser.class, Properties.class }, new Object[] { connStrParser, info }, null);
                            break;
                        default:
                            return new ConnectionUrl(connString) {
                            };
                    }
                    connectionUrlCache.put(connStringCacheKey, connectionString);
                }
                rwLock.readLock().lock();
            } finally {
                rwLock.writeLock().unlock();
            }
        }
        rwLock.readLock().unlock();
        return connectionString;
    }

    /**
     * Builds a connection URL cache map key based on the connection string itself plus the string representation of the given connection properties.
     * 
     * @param connString
     *            the connection string
     * @param info
     *            the connection arguments map
     * @return a connection string cache map key
     */
    private static String buildConnectionStringCacheKey(String connString, Properties info) {
        StringBuilder sbKey = new StringBuilder(connString);
        sbKey.append("§");
        sbKey.append(
                info == null ? null : info.stringPropertyNames().stream().map(k -> k + "=" + info.getProperty(k)).collect(Collectors.joining(", ", "{", "}")));
        return sbKey.toString();
    }

    /**
     * Checks if this {@link ConnectionUrl} is able to process the given database URL.
     * 
     * @param connString
     *            the connection string
     * @return true if this class is able to process the given URL, false otherwise
     */
    public static boolean acceptsUrl(String connString) {
        if (connString == null) {
            throw ExceptionFactory.createException(WrongArgumentException.class, Messages.getString("ConnectionString.0"));
        }
        try {
            ConnectionUrlParser connStringParser = ConnectionUrlParser.parseConnectionString(connString);
            Type.fromValue(connStringParser.getScheme(), -1);
        } catch (Throwable t) {
            return false;
        }
        return true;
    }

    /**
     * Empty constructor. Required for subclasses initialization.
     */
    protected ConnectionUrl() {
    }

    /**
     * Constructor for unsupported URLs
     * 
     * @param origUrl
     *            URLs
     */
    public ConnectionUrl(String origUrl) {
        this.originalConnStr = origUrl;
    }

    /**
     * Constructs an instance of {@link ConnectionUrl}, performing all the required initializations.
     * 
     * @param connStrParser
     *            a {@link ConnectionUrlParser} instance containing the parsed version of the original connection string
     * @param info
     *            the connection arguments map
     */
    protected ConnectionUrl(ConnectionUrlParser connStrParser, Properties info) {
        this.originalConnStr = connStrParser.getDatabaseUrl();
        this.originalDatabase = connStrParser.getPath() == null ? "" : connStrParser.getPath();
        collectProperties(connStrParser, info); // Fill properties before filling hosts info.
        collectHostsInfo(connStrParser);
    }

    /**
     * Joins the connection arguments from the connection string with the ones from the given connection arguments map collecting them in a single map.
     * Additionally may also collect other connection arguments from configuration files.
     * 
     * @param connStrParser
     *            the {@link ConnectionUrlParser} from where to collect the properties
     * @param info
     *            the connection arguments map
     */
    protected void collectProperties(ConnectionUrlParser connStrParser, Properties info) {
        // Fill in the properties from the connection string.
        this.properties.putAll(connStrParser.getProperties());

        // Properties passed in override the ones from the connection string.
        if (info != null) {
            info.stringPropertyNames().stream().forEach(k -> this.properties.put(k, info.getProperty(k)));
        }

        // Collect properties from additional sources. 
        processColdFusionAutoConfiguration();
        setupPropertiesTransformer();
        expandPropertiesFromConfigFiles(this.properties);
        injectPerTypeProperties(this.properties);
    }

    /**
     * Checks if the conditions for the Cold Fusion auto configuration file are met. If so, adds a reference to its configuration file so that it can be loaded
     * afterwards.
     */
    protected void processColdFusionAutoConfiguration() {
        if (Util.isColdFusion()) {
            String autoConfigCf = this.properties.get(PropertyDefinitions.PNAME_autoConfigureForColdFusion);
            if (autoConfigCf == null || autoConfigCf.equalsIgnoreCase("TRUE") || autoConfigCf.equalsIgnoreCase("YES")) {
                String currentConfigFiles = this.properties.get(PropertyDefinitions.PNAME_useConfigs);
                StringBuilder newConfigFiles = new StringBuilder();
                if (currentConfigFiles != null) {
                    newConfigFiles.append(currentConfigFiles).append(",");
                }
                newConfigFiles.append("coldFusion");
                this.properties.put(PropertyDefinitions.PNAME_useConfigs, newConfigFiles.toString());
            }
        }
    }

    /**
     * Sets up the {@link ConnectionPropertiesTransform} if one was provided.
     */
    protected void setupPropertiesTransformer() {
        String propertiesTransformClassName = this.properties.get(PropertyDefinitions.PNAME_propertiesTransform);
        if (!isNullOrEmpty(propertiesTransformClassName)) {
            try {
                this.propertiesTransformer = (ConnectionPropertiesTransform) Class.forName(propertiesTransformClassName).newInstance();
            } catch (InstantiationException | IllegalAccessException | ClassNotFoundException | CJException e) {
                throw ExceptionFactory.createException(InvalidConnectionAttributeException.class,
                        Messages.getString("ConnectionString.9", new Object[] { propertiesTransformClassName, e.toString() }), e);
            }
        }
    }

    /**
     * Expands the connection argument "useConfig" by reading the mentioned configuration files.
     * 
     * @param props
     *            a connection arguments map from where to read the "useConfig" property and where to save the loaded properties.
     */
    protected void expandPropertiesFromConfigFiles(Map<String, String> props) {
        // Properties from config files should not override the existing ones.
        String configFiles = props.get(PropertyDefinitions.PNAME_useConfigs);
        if (!isNullOrEmpty(configFiles)) {
            Properties configProps = getPropertiesFromConfigFiles(configFiles);
            configProps.stringPropertyNames().stream().filter(k -> !props.containsKey(k)).forEach(k -> props.put(k, configProps.getProperty(k)));
        }
    }

    /**
     * Returns a map containing the properties read from the given configuration files. Multiple files can be referenced using a comma as separator.
     * 
     * @param configFiles
     *            the list of the configuration files to read
     * @return the map containing all the properties read
     */
    public static Properties getPropertiesFromConfigFiles(String configFiles) {
        Properties configProps = new Properties();
        for (String configFile : configFiles.split(",")) {
            try (InputStream configAsStream = ConnectionUrl.class.getResourceAsStream("/com/mysql/cj/configurations/" + configFile + ".properties")) {
                if (configAsStream == null) {
                    throw ExceptionFactory.createException(InvalidConnectionAttributeException.class,
                            Messages.getString("ConnectionString.10", new Object[] { configFile }));
                }
                configProps.load(configAsStream);
            } catch (IOException e) {
                throw ExceptionFactory.createException(InvalidConnectionAttributeException.class,
                        Messages.getString("ConnectionString.11", new Object[] { configFile }), e);
            }
        }
        return configProps;
    }

    /**
     * Subclasses must override this method if they need to inject additional properties in the connection arguments map while it's being constructed.
     * 
     * @param props
     *            the properties already containing all known connection arguments
     */
    protected void injectPerTypeProperties(Map<String, String> props) {
        return;
    }

    /**
     * Collects the hosts information from the {@link ConnectionUrlParser}.
     * 
     * @param connStrParser
     *            the {@link ConnectionUrlParser} from where to collect the hosts information
     */
    protected void collectHostsInfo(ConnectionUrlParser connStrParser) {
        connStrParser.getHosts().stream().map(this::fixHostInfo).forEach(this.hosts::add);
    }

    /**
     * Fixes the host information by moving data around and filling in missing data.
     * Applies properties transformations to the collected properties if {@link ConnectionPropertiesTransform} was declared in the connection arguments.
     * 
     * @param hi
     *            the host information data to fix
     * @return a new {@link HostInfo} with all required data
     */
    protected HostInfo fixHostInfo(HostInfo hi) {
        Map<String, String> hostProps = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);

        hostProps.putAll(this.properties); // Add global connection arguments.
        hostProps.putAll(hi.getHostProperties()); // Add/override host specific connection arguments.
        hostProps.put(PropertyDefinitions.DBNAME_PROPERTY_KEY, getDatabase()); // Add the database name

        hostProps = preprocessPerTypeHostProperties(hostProps);

        String host = hostProps.remove(PropertyDefinitions.HOST_PROPERTY_KEY);
        if (!isNullOrEmpty(hi.getHost())) {
            host = hi.getHost();
        } else if (isNullOrEmpty(host)) {
            host = getDefaultHost();
        }

        String portAsString = hostProps.remove(PropertyDefinitions.PORT_PROPERTY_KEY);
        int port = hi.getPort();
        if (port == -1 && !isNullOrEmpty(portAsString)) {
            try {
                port = Integer.valueOf(portAsString);
            } catch (NumberFormatException e) {
                throw ExceptionFactory.createException(WrongArgumentException.class,
                        Messages.getString("ConnectionString.7", new Object[] { hostProps.get(PropertyDefinitions.PORT_PROPERTY_KEY) }), e);
            }
        }
        if (port == -1) {
            port = getDefaultPort();
        }

        String user = hostProps.remove(PropertyDefinitions.PNAME_user);
        if (!isNullOrEmpty(hi.getUser())) {
            user = hi.getUser();
        } else if (isNullOrEmpty(user)) {
            user = getDefaultUser();
        }

        boolean isPasswordless = hi.isPasswordless();
        String password = hostProps.remove(PropertyDefinitions.PNAME_password);
        if (!isPasswordless) {
            password = hi.getPassword();
        } else if (password == null) {
            password = getDefaultPassword();
            isPasswordless = true;
        } else {
            isPasswordless = false;
        }

        expandPropertiesFromConfigFiles(hostProps);
        fixKeysCase(hostProps);
        fixProtocolDependencies(hostProps);

        return buildHostInfo(host, port, user, password, isPasswordless, hostProps);
    }

    /**
     * Subclasses should override this to perform any required preprocessing on the host information properties.
     * 
     * @param hostProps
     *            the host properties map to process
     * @return
     *         the processed host properties map
     */
    protected Map<String, String> preprocessPerTypeHostProperties(Map<String, String> hostProps) {
        return hostProps;
    }

    /**
     * Returns the default host. Subclasses must override this method if they have different default host value.
     * 
     * @return the default host
     */
    public String getDefaultHost() {
        return DEFAULT_HOST;
    }

    /**
     * Returns the default port. Subclasses must override this method if they have different default port value.
     * 
     * @return the default port
     */
    public int getDefaultPort() {
        return DEFAULT_PORT;
    }

    /**
     * Returns the default user. Usually the one provided in the method {@link DriverManager#getConnection(String, String, String)} or as connection argument.
     * 
     * @return the default user
     */
    public String getDefaultUser() {
        String user = this.properties.get(PropertyDefinitions.PNAME_user);
        return isNullOrEmpty(user) ? "" : user;
    }

    /**
     * Returns the default password. Usually the one provided in the method {@link DriverManager#getConnection(String, String, String)} or as connection
     * argument.
     * 
     * @return the default password
     */
    public String getDefaultPassword() {
        String password = this.properties.get(PropertyDefinitions.PNAME_password);
        return isNullOrEmpty(password) ? "" : password;
    }

    /**
     * Fixes the case for alternate host syntax main properties.
     * 
     * @param hostProps
     *            the host properties map to fix
     */
    protected void fixKeysCase(Map<String, String> hostProps) {
        for (String key : Arrays.asList(PropertyDefinitions.PROTOCOL_PROPERTY_KEY, PropertyDefinitions.PATH_PROPERTY_KEY, PropertyDefinitions.TYPE_PROPERTY_KEY,
                PropertyDefinitions.ADDRESS_PROPERTY_KEY, PropertyDefinitions.PRIORITY_PROPERTY_KEY)) {
            if (hostProps.containsKey(key)) {
                hostProps.put(key, hostProps.remove(key));
            }
        }
    }

    /**
     * Fixes the protocol (TCP vs PIPE) dependencies for the given host properties map.
     * 
     * @param hostProps
     *            the host properties map to fix
     */
    protected void fixProtocolDependencies(Map<String, String> hostProps) {
        String protocol = hostProps.get(PropertyDefinitions.PROTOCOL_PROPERTY_KEY);
        if (!isNullOrEmpty(protocol) && protocol.equalsIgnoreCase("PIPE")) {
            if (!hostProps.containsKey(PropertyDefinitions.PNAME_socketFactory)) {
                hostProps.put(PropertyDefinitions.PNAME_socketFactory, "com.mysql.cj.protocol.NamedPipeSocketFactory");
            }
            if (hostProps.containsKey(PropertyDefinitions.PATH_PROPERTY_KEY) && !hostProps.containsKey(PropertyDefinitions.NAMED_PIPE_PROP_NAME)) {
                hostProps.put(PropertyDefinitions.NAMED_PIPE_PROP_NAME, hostProps.get(PropertyDefinitions.PATH_PROPERTY_KEY));
            }
        }
    }

    /**
     * Returns this connection URL type.
     * 
     * @return the connection URL type
     */
    public Type getType() {
        return this.type;
    }

    /**
     * Returns the original database URL that produced this connection string.
     * 
     * @return the original database URL
     */
    @Override
    public String getDatabaseUrl() {
        return this.originalConnStr;
    }

    /**
     * Returns the database from this connection URL. Note that a "DBNAME" property overrides the database identified in the connection string.
     * 
     * @return the database name
     */
    public String getDatabase() {
        return this.properties.containsKey(PropertyDefinitions.DBNAME_PROPERTY_KEY) ? this.properties.get(PropertyDefinitions.DBNAME_PROPERTY_KEY)
                : this.originalDatabase;
    }

    /**
     * Returns the number of hosts in this connection URL.
     * 
     * @return the number of hosts
     */
    public int hostsCount() {
        return this.hosts.size();
    }

    /**
     * Returns the single or first host info structure.
     * 
     * @return the first host info structure
     */
    public HostInfo getMainHost() {
        return this.hosts.isEmpty() ? null : this.hosts.get(0);
    }

    /**
     * Returns a list of the hosts in this connection URL.
     * 
     * @return the hosts list from this connection URL
     */
    public List<HostInfo> getHostsList() {
        return Collections.unmodifiableList(this.hosts);
    }

    /**
     * Returns an existing host info with the same host:port part or spawns a new isolated host info based on this connection URL if none was found.
     * 
     * @param hostPortPair
     *            the host:port part to search for
     * @return the existing host info or a new independent one
     */
    public HostInfo getHostOrSpawnIsolated(String hostPortPair) {
        return getHostOrSpawnIsolated(hostPortPair, this.hosts);
    }

    /**
     * Returns an existing host info with the same host:port part or spawns a new isolated host info based on this connection URL if none was found.
     * 
     * @param hostPortPair
     *            the host:port part to search for
     * @param hostsList
     *            the hosts list from where to search the host list
     * @return the existing host info or a new independent one
     */
    public HostInfo getHostOrSpawnIsolated(String hostPortPair, List<HostInfo> hostsList) {
        for (HostInfo hi : hostsList) {
            if (hostPortPair.equals(hi.getHostPortPair())) {
                return hi;
            }
        }

        ConnectionUrlParser.Pair<String, Integer> hostAndPort = ConnectionUrlParser.parseHostPortPair(hostPortPair);
        String host = hostAndPort.left;
        Integer port = hostAndPort.right;
        String user = getDefaultUser();
        String password = getDefaultPassword();

        return buildHostInfo(host, port, user, password, true, this.properties);
    }

    /**
     * Creates a new {@link HostInfo} structure with the given components, passing through the properties transformer if there is one defined in this connection
     * string;
     * 
     * @param host
     *            the host
     * @param port
     *            the port
     * @param user
     *            the user name
     * @param password
     *            the password
     * @param isDefaultPwd
     *            no password was provided in the connection URL or arguments?
     * @param hostProps
     *            the host properties map
     * @return a new instance of {@link HostInfo}
     */
    private HostInfo buildHostInfo(String host, int port, String user, String password, boolean isDefaultPwd, Map<String, String> hostProps) {
        // Apply properties transformations if needed.
        if (this.propertiesTransformer != null) {
            Properties props = new Properties();
            props.putAll(hostProps);

            props.setProperty(PropertyDefinitions.HOST_PROPERTY_KEY, host);
            props.setProperty(PropertyDefinitions.PORT_PROPERTY_KEY, String.valueOf(port));
            props.setProperty(PropertyDefinitions.PNAME_user, user);
            props.setProperty(PropertyDefinitions.PNAME_password, password);

            Properties transformedProps = this.propertiesTransformer.transformProperties(props);

            host = transformedProps.getProperty(PropertyDefinitions.HOST_PROPERTY_KEY);
            try {
                port = Integer.parseInt(transformedProps.getProperty(PropertyDefinitions.PORT_PROPERTY_KEY));
            } catch (NumberFormatException e) {
                throw ExceptionFactory.createException(WrongArgumentException.class,
                        Messages.getString("ConnectionString.8",
                                new Object[] { PropertyDefinitions.PORT_PROPERTY_KEY, transformedProps.getProperty(PropertyDefinitions.PORT_PROPERTY_KEY) }),
                        e);
            }
            user = transformedProps.getProperty(PropertyDefinitions.PNAME_user);
            password = transformedProps.getProperty(PropertyDefinitions.PNAME_password);

            List<String> surplusKeys = Arrays.asList(PropertyDefinitions.HOST_PROPERTY_KEY, PropertyDefinitions.PORT_PROPERTY_KEY,
                    PropertyDefinitions.PNAME_user, PropertyDefinitions.PNAME_password);
            Map<String, String> transformedHostProps = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
            transformedProps.stringPropertyNames().stream().filter(k -> !surplusKeys.contains(k))
                    .forEach(k -> transformedHostProps.put(k, transformedProps.getProperty(k)));
            hostProps = transformedHostProps;
        }

        return new HostInfo(this, host, port, user, password, isDefaultPwd, hostProps);
    }

    /**
     * Returns the original (common to all hosts) connection arguments as provided in the connection string query section.
     * 
     * @return the original (common to all hosts) connection arguments
     */
    public Map<String, String> getOriginalProperties() {
        return Collections.unmodifiableMap(this.properties);
    }

    /**
     * Returns a {@link Properties} instance containing the connection arguments extracted from the URL query section, i.e., per host attributes are excluded.
     * Applies properties transformations to the collected properties if {@link ConnectionPropertiesTransform} was declared in the connection arguments.
     *
     * @return a {@link Properties} instance containing the common connection arguments.
     */
    public Properties getConnectionArgumentsAsProperties() {
        Properties props = new Properties();
        if (this.properties != null) {
            props.putAll(this.properties);
        }

        return this.propertiesTransformer != null ? this.propertiesTransformer.transformProperties(props) : props;
    }

    /**
     * Returns a string representation of this object.
     * 
     * @return a string representation of this object
     */
    @Override
    public String toString() {
        StringBuilder asStr = new StringBuilder(super.toString());
        asStr.append(String.format(" :: {type: \"%s\", hosts: %s, database: \"%s\", properties: %s, propertiesTransformer: %s}", this.type, this.hosts,
                this.originalDatabase, this.properties, this.propertiesTransformer));
        return asStr.toString();
    }
}
