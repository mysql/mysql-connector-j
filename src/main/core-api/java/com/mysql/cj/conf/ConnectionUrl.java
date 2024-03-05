/*
 * Copyright (c) 2016, 2024, Oracle and/or its affiliates.
 *
 * This program is free software; you can redistribute it and/or modify it under the terms of the GNU General Public License, version 2.0, as published by
 * the Free Software Foundation.
 *
 * This program is designed to work with certain software that is licensed under separate terms, as designated in a particular file or component or in
 * included license documentation. The authors of MySQL hereby grant you an additional permission to link the program and your derivative works with the
 * separately licensed software that they have either included with the program or referenced in the documentation.
 *
 * Without limiting anything contained in the foregoing, this file, which is part of MySQL Connector/J, is also subject to the Universal FOSS Exception,
 * version 1.0, a copy of which can be found at http://oss.oracle.com/licenses/universal-foss-exception.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License, version 2.0, for more details.
 *
 * You should have received a copy of the GNU General Public License along with this program; if not, write to the Free Software Foundation, Inc.,
 * 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA
 */

package com.mysql.cj.conf;

import static com.mysql.cj.util.StringUtils.isNullOrEmpty;

import java.io.IOException;
import java.io.InputStream;
import java.sql.DriverManager;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

import javax.naming.NamingException;

import com.mysql.cj.Messages;
import com.mysql.cj.exceptions.CJException;
import com.mysql.cj.exceptions.ExceptionFactory;
import com.mysql.cj.exceptions.InvalidConnectionAttributeException;
import com.mysql.cj.exceptions.UnsupportedConnectionStringException;
import com.mysql.cj.exceptions.WrongArgumentException;
import com.mysql.cj.util.DnsSrv;
import com.mysql.cj.util.DnsSrv.SrvRecord;
import com.mysql.cj.util.LRUCache;
import com.mysql.cj.util.Util;

/**
 * A container for a database URL and a collection of given connection arguments.
 * The connection string is parsed and split by its components, each of which is then processed and fixed according to the needs of the connection type.
 * This abstract class holds all common behavior to all connection string types. Its subclasses must implement their own specifics such as classifying hosts by
 * type or apply validation rules.
 */
public abstract class ConnectionUrl implements DatabaseUrlContainer {

    public static final String DEFAULT_HOST = "localhost";
    public static final int DEFAULT_PORT = 3306;

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
     * The database URL type which is determined by the scheme section of the connection string.
     */
    public enum Type {

        // DNS SRV schemes (cardinality is validated by implementing classes):
        FAILOVER_DNS_SRV_CONNECTION("jdbc:mysql+srv:", HostsCardinality.ONE_OR_MORE, "com.mysql.cj.conf.url.FailoverDnsSrvConnectionUrl"), //
        LOADBALANCE_DNS_SRV_CONNECTION("jdbc:mysql+srv:loadbalance:", HostsCardinality.ONE_OR_MORE, "com.mysql.cj.conf.url.LoadBalanceDnsSrvConnectionUrl"), //
        REPLICATION_DNS_SRV_CONNECTION("jdbc:mysql+srv:replication:", HostsCardinality.ONE_OR_MORE, "com.mysql.cj.conf.url.ReplicationDnsSrvConnectionUrl"), //
        XDEVAPI_DNS_SRV_SESSION("mysqlx+srv:", HostsCardinality.ONE_OR_MORE, "com.mysql.cj.conf.url.XDevApiDnsSrvConnectionUrl"), //
        // Standard schemes:
        SINGLE_CONNECTION("jdbc:mysql:", HostsCardinality.SINGLE, "com.mysql.cj.conf.url.SingleConnectionUrl", PropertyKey.dnsSrv, FAILOVER_DNS_SRV_CONNECTION), //
        FAILOVER_CONNECTION("jdbc:mysql:", HostsCardinality.MULTIPLE, "com.mysql.cj.conf.url.FailoverConnectionUrl", PropertyKey.dnsSrv,
                FAILOVER_DNS_SRV_CONNECTION), //
        LOADBALANCE_CONNECTION("jdbc:mysql:loadbalance:", HostsCardinality.ONE_OR_MORE, "com.mysql.cj.conf.url.LoadBalanceConnectionUrl", PropertyKey.dnsSrv,
                LOADBALANCE_DNS_SRV_CONNECTION), //
        REPLICATION_CONNECTION("jdbc:mysql:replication:", HostsCardinality.ONE_OR_MORE, "com.mysql.cj.conf.url.ReplicationConnectionUrl", PropertyKey.dnsSrv,
                REPLICATION_DNS_SRV_CONNECTION), //
        XDEVAPI_SESSION("mysqlx:", HostsCardinality.ONE_OR_MORE, "com.mysql.cj.conf.url.XDevApiConnectionUrl", PropertyKey.xdevapiDnsSrv,
                XDEVAPI_DNS_SRV_SESSION);

        private String scheme;
        private HostsCardinality cardinality;
        private String implementingClass;
        private PropertyKey dnsSrvPropertyKey;
        private Type alternateDnsSrvType;

        private Type(String scheme, HostsCardinality cardinality, String implementingClass) {
            this(scheme, cardinality, implementingClass, null, null);
        }

        private Type(String scheme, HostsCardinality cardinality, String implementingClass, PropertyKey dnsSrvPropertyKey, Type alternateDnsSrvType) {
            this.scheme = scheme;
            this.cardinality = cardinality;
            this.implementingClass = implementingClass;
            this.dnsSrvPropertyKey = dnsSrvPropertyKey;
            this.alternateDnsSrvType = alternateDnsSrvType;
        }

        public String getScheme() {
            return this.scheme;
        }

        public HostsCardinality getCardinality() {
            return this.cardinality;
        }

        public String getImplementingClass() {
            return this.implementingClass;
        }

        public PropertyKey getDnsSrvPropertyKey() {
            return this.dnsSrvPropertyKey;
        }

        public Type getAlternateDnsSrvType() {
            return this.alternateDnsSrvType;
        }

        /**
         * Returns the {@link Type} corresponding to the given scheme and number of hosts, if any.
         * Otherwise throws an {@link UnsupportedConnectionStringException}.
         * Calling this method with the argument n lower than 0 skips the hosts cardinality validation.
         *
         * @param scheme
         *            one of supported schemes
         * @param n
         *            the number of hosts in the database URL
         * @return the {@link Type} corresponding to the given protocol and number of hosts
         */
        public static Type fromValue(String scheme, int n) {
            for (Type t : values()) {
                if (t.getScheme().equalsIgnoreCase(scheme) && (n < 0 || t.getCardinality().assertSize(n))) {
                    return t;
                }
            }
            if (n < 0) {
                throw ExceptionFactory.createException(UnsupportedConnectionStringException.class,
                        Messages.getString("ConnectionString.5", new Object[] { scheme }));
            }
            throw ExceptionFactory.createException(UnsupportedConnectionStringException.class,
                    Messages.getString("ConnectionString.6", new Object[] { scheme, n }));
        }

        /**
         * Instantiates a class that implements the right type of connection URLs for the given {@link ConnectionUrlParser}.
         *
         * @param parser
         *            the {@link ConnectionUrlParser} containing the URL components.
         * @param info
         *            a connection properties map to add to the {@link ConnectionUrl} structure.
         * @return
         *         an instance of {@link ConnectionUrl}.
         */
        public static ConnectionUrl getConnectionUrlInstance(ConnectionUrlParser parser, Properties info) {
            int hostsCount = parser.getHosts().size();
            Type type = fromValue(parser.getScheme(), hostsCount);
            PropertyKey dnsSrvPropKey = type.getDnsSrvPropertyKey();
            Map<String, String> parsedProperties;

            // Check if the Type must be replaced by a DNS SRV one.
            if (dnsSrvPropKey != null && type.getAlternateDnsSrvType() != null) {
                if (info != null && info.containsKey(dnsSrvPropKey.getKeyName())) { // Properties map prevails over connection string options.
                    if ((Boolean) PropertyDefinitions.getPropertyDefinition(dnsSrvPropKey).parseObject(info.getProperty(dnsSrvPropKey.getKeyName()), null)) {
                        type = fromValue(type.getAlternateDnsSrvType().getScheme(), hostsCount);
                    }
                } else if ((parsedProperties = parser.getProperties()).containsKey(dnsSrvPropKey.getKeyName()) && (Boolean) PropertyDefinitions
                        .getPropertyDefinition(dnsSrvPropKey).parseObject(parsedProperties.get(dnsSrvPropKey.getKeyName()), null)) {
                    type = fromValue(type.getAlternateDnsSrvType().getScheme(), hostsCount);
                }
            }

            return type.getImplementingInstance(parser, info);
        }

        /**
         * Checks if the given scheme corresponds to one of the connection types the driver supports.
         *
         * @param scheme
         *            scheme part from connection string, like "jdbc:mysql:"
         * @return true if the given scheme is supported by driver
         */
        public static boolean isSupported(String scheme) {
            for (Type t : values()) {
                if (t.getScheme().equalsIgnoreCase(scheme)) {
                    return true;
                }
            }
            return false;
        }

        /**
         * Instantiates a class that implements this type of connection URLs with the given arguments.
         *
         * @param parser
         *            the {@link ConnectionUrlParser} containing the URL components.
         * @param info
         *            a connection properties map to add to the {@link ConnectionUrl} structure.
         * @return
         *         an instance of {@link ConnectionUrl}.
         */
        private ConnectionUrl getImplementingInstance(ConnectionUrlParser parser, Properties info) {
            return Util.getInstance(ConnectionUrl.class, this.implementingClass, new Class<?>[] { ConnectionUrlParser.class, Properties.class },
                    new Object[] { parser, info }, null);
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
        ConnectionUrl connectionUrl;

        rwLock.readLock().lock();
        connectionUrl = connectionUrlCache.get(connStringCacheKey);
        if (connectionUrl == null) {
            rwLock.readLock().unlock();
            rwLock.writeLock().lock();
            try {
                // Check again, in the meantime it could have been cached by another thread.
                connectionUrl = connectionUrlCache.get(connStringCacheKey);
                if (connectionUrl == null) {
                    ConnectionUrlParser connStrParser = ConnectionUrlParser.parseConnectionString(connString);
                    connectionUrl = Type.getConnectionUrlInstance(connStrParser, info);
                    connectionUrlCache.put(connStringCacheKey, connectionUrl);
                }
                rwLock.readLock().lock();
            } finally {
                rwLock.writeLock().unlock();
            }
        }
        rwLock.readLock().unlock();
        return connectionUrl;
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
        sbKey.append("\u00A7"); // Section sign.
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
        return ConnectionUrlParser.isConnectionStringSupported(connString);
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
        connStrParser.getProperties().entrySet().stream().forEach(e -> this.properties.put(PropertyKey.normalizeCase(e.getKey()), e.getValue()));

        // Properties passed in override the ones from the connection string.
        if (info != null) {
            info.stringPropertyNames().stream().forEach(k -> this.properties.put(PropertyKey.normalizeCase(k), info.getProperty(k)));
        }

        // Collect properties from additional sources.
        setupPropertiesTransformer();
        expandPropertiesFromConfigFiles(this.properties);
        injectPerTypeProperties(this.properties);
    }

    /**
     * Sets up the {@link ConnectionPropertiesTransform} if one was provided.
     */
    protected void setupPropertiesTransformer() {
        String propertiesTransformClassName = this.properties.get(PropertyKey.propertiesTransform.getKeyName());
        if (!isNullOrEmpty(propertiesTransformClassName)) {
            try {
                this.propertiesTransformer = Util.getInstance(ConnectionPropertiesTransform.class, propertiesTransformClassName, null, null, null);
            } catch (CJException e) {
                throw ExceptionFactory.createException(InvalidConnectionAttributeException.class, Messages.getString("ConnectionString.9"), e);
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
        String configFiles = props.get(PropertyKey.useConfigs.getKeyName());
        if (!isNullOrEmpty(configFiles)) {
            Properties configProps = getPropertiesFromConfigFiles(configFiles);
            configProps.stringPropertyNames().stream().map(PropertyKey::normalizeCase).filter(k -> !props.containsKey(k))
                    .forEach(k -> props.put(k, configProps.getProperty(k)));
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
     * Some acceptable property values have changed in c/J 8.0 but old values remain hard-coded in widely used software.
     * So, old values must be accepted and translated to new ones.
     *
     * @param props
     *            the host properties map to fix
     */
    protected void replaceLegacyPropertyValues(Map<String, String> props) {
        // Workaround for zeroDateTimeBehavior=convertToNull hard-coded in NetBeans
        String zeroDateTimeBehavior = props.get(PropertyKey.zeroDateTimeBehavior.getKeyName());
        if (zeroDateTimeBehavior != null && zeroDateTimeBehavior.equalsIgnoreCase("convertToNull")) {
            props.put(PropertyKey.zeroDateTimeBehavior.getKeyName(), "CONVERT_TO_NULL");
        }
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
        Map<String, String> hostProps = new HashMap<>();

        // Add global connection arguments.
        hostProps.putAll(this.properties);
        // Add/override host specific connection arguments.
        hi.getHostProperties().entrySet().stream().forEach(e -> hostProps.put(PropertyKey.normalizeCase(e.getKey()), e.getValue()));
        // Add the database name.
        if (!hostProps.containsKey(PropertyKey.DBNAME.getKeyName())) {
            hostProps.put(PropertyKey.DBNAME.getKeyName(), getDatabase());
        }

        preprocessPerTypeHostProperties(hostProps);

        String host = hostProps.remove(PropertyKey.HOST.getKeyName());
        if (!isNullOrEmpty(hi.getHost())) {
            host = hi.getHost();
        } else if (isNullOrEmpty(host)) {
            host = getDefaultHost();
        }

        String portAsString = hostProps.remove(PropertyKey.PORT.getKeyName());
        int port = hi.getPort();
        if (port == HostInfo.NO_PORT && !isNullOrEmpty(portAsString)) {
            try {
                port = Integer.parseInt(portAsString);
            } catch (NumberFormatException e) {
                throw ExceptionFactory.createException(WrongArgumentException.class,
                        Messages.getString("ConnectionString.7", new Object[] { hostProps.get(PropertyKey.PORT.getKeyName()) }), e);
            }
        }
        if (port == HostInfo.NO_PORT) {
            port = getDefaultPort();
        }

        String user = hostProps.remove(PropertyKey.USER.getKeyName());
        if (!isNullOrEmpty(hi.getUser())) {
            user = hi.getUser();
        } else if (isNullOrEmpty(user)) {
            user = getDefaultUser();
        }

        String password = hostProps.remove(PropertyKey.PASSWORD.getKeyName());
        if (hi.getPassword() != null) { // Password can be specified as empty string.
            password = hi.getPassword();
        } else if (isNullOrEmpty(password)) {
            password = getDefaultPassword();
        }

        expandPropertiesFromConfigFiles(hostProps);
        fixProtocolDependencies(hostProps);
        replaceLegacyPropertyValues(hostProps);

        return buildHostInfo(host, port, user, password, hostProps);
    }

    /**
     * Subclasses should override this to perform any required pre-processing on the host information properties.
     *
     * @param hostProps
     *            the host properties map to process
     */
    protected void preprocessPerTypeHostProperties(Map<String, String> hostProps) {
        // To be overridden in subclasses if needed.
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
        return this.properties.get(PropertyKey.USER.getKeyName());
    }

    /**
     * Returns the default password. Usually the one provided in the method {@link DriverManager#getConnection(String, String, String)} or as connection
     * argument.
     *
     * @return the default password
     */
    public String getDefaultPassword() {
        return this.properties.get(PropertyKey.PASSWORD.getKeyName());
    }

    /**
     * Fixes the protocol (TCP vs PIPE) dependencies for the given host properties map.
     *
     * @param hostProps
     *            the host properties map to fix
     */
    protected void fixProtocolDependencies(Map<String, String> hostProps) {
        String protocol = hostProps.get(PropertyKey.PROTOCOL.getKeyName());
        if (!isNullOrEmpty(protocol) && protocol.equalsIgnoreCase("PIPE")) {
            if (!hostProps.containsKey(PropertyKey.socketFactory.getKeyName())) {
                hostProps.put(PropertyKey.socketFactory.getKeyName(), "com.mysql.cj.protocol.NamedPipeSocketFactory");
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
        return this.properties.containsKey(PropertyKey.DBNAME.getKeyName()) ? this.properties.get(PropertyKey.DBNAME.getKeyName()) : this.originalDatabase;
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
        return getHostsList(HostsListView.ALL);
    }

    /**
     * Returns a list of the hosts in this connection URL, filtered for the given view.
     *
     * By default returns all hosts. Subclasses should override this method in order to implement support for different views, usually by splitting the global
     * hosts into smaller sub-lists.
     *
     * @param view
     *            the type of the view to use in the returned list of hosts. This argument is ignored in this implementation.
     * @return
     *         the hosts list from this connection URL, filtered for the given view.
     */
    public List<HostInfo> getHostsList(HostsListView view) {
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

        return buildHostInfo(host, port, user, password, this.properties);
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
     * @param hostProps
     *            the host properties map
     * @return a new instance of {@link HostInfo}
     */
    protected HostInfo buildHostInfo(String host, int port, String user, String password, Map<String, String> hostProps) {
        // Apply properties transformations if needed.
        if (this.propertiesTransformer != null) {
            Properties props = new Properties();
            props.putAll(hostProps);

            props.setProperty(PropertyKey.HOST.getKeyName(), host);
            props.setProperty(PropertyKey.PORT.getKeyName(), String.valueOf(port));
            if (user != null) {
                props.setProperty(PropertyKey.USER.getKeyName(), user);
            }
            if (password != null) {
                props.setProperty(PropertyKey.PASSWORD.getKeyName(), password);
            }

            Properties transformedProps = this.propertiesTransformer.transformProperties(props);

            host = transformedProps.getProperty(PropertyKey.HOST.getKeyName());
            try {
                port = Integer.parseInt(transformedProps.getProperty(PropertyKey.PORT.getKeyName()));
            } catch (NumberFormatException e) {
                throw ExceptionFactory.createException(WrongArgumentException.class, Messages.getString("ConnectionString.8",
                        new Object[] { PropertyKey.PORT.getKeyName(), transformedProps.getProperty(PropertyKey.PORT.getKeyName()) }), e);
            }
            user = transformedProps.getProperty(PropertyKey.USER.getKeyName());
            password = transformedProps.getProperty(PropertyKey.PASSWORD.getKeyName());

            Map<String, String> transformedHostProps = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
            transformedProps.stringPropertyNames().stream().forEach(k -> transformedHostProps.put(k, transformedProps.getProperty(k)));
            // Remove surplus keys.
            transformedHostProps.remove(PropertyKey.HOST.getKeyName());
            transformedHostProps.remove(PropertyKey.PORT.getKeyName());
            transformedHostProps.remove(PropertyKey.USER.getKeyName());
            transformedHostProps.remove(PropertyKey.PASSWORD.getKeyName());

            hostProps = transformedHostProps;
        }

        return new HostInfo(this, host, port, user, password, hostProps);
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
     * Returns a hosts list built from the result of the DNS SRV lookup for the original host name.
     *
     * @param srvHost
     *            the {@link HostInfo} from where to get the DNS SRV service name to lookup.
     * @return
     *         the hosts list from the result of the DNS SRV lookup, filtered for the given view.
     */
    public List<HostInfo> getHostsListFromDnsSrv(HostInfo srvHost) {
        String srvServiceName = srvHost.getHost();
        List<SrvRecord> srvRecords = null;

        try {
            srvRecords = DnsSrv.lookupSrvRecords(srvServiceName);
        } catch (NamingException e) {
            throw ExceptionFactory.createException(Messages.getString("ConnectionString.26", new Object[] { srvServiceName }), e);
        }
        if (srvRecords == null || srvRecords.size() == 0) {
            throw ExceptionFactory.createException(Messages.getString("ConnectionString.26", new Object[] { srvServiceName }));
        }

        return Collections.unmodifiableList(srvRecordsToHostsList(srvRecords, srvHost));
    }

    /**
     * Converts a list of DNS SRV records into a hosts list.
     *
     * @param srvRecords
     *            the list of DNS SRV records.
     * @param baseHostInfo
     *            the {@link HostInfo} to use as source of all common host specific options.
     * @return
     *         a list of hosts.
     */
    private List<HostInfo> srvRecordsToHostsList(List<SrvRecord> srvRecords, HostInfo baseHostInfo) {
        return srvRecords.stream()
                .map(s -> buildHostInfo(s.getTarget(), s.getPort(), baseHostInfo.getUser(), baseHostInfo.getPassword(), baseHostInfo.getHostProperties()))
                .collect(Collectors.toList());
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
