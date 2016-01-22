/*
  Copyright (c) 2015, 2016, Oracle and/or its affiliates. All rights reserved.

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

package com.mysql.cj.core;

import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Properties;
import java.util.StringTokenizer;

import com.mysql.cj.api.MysqlConnection;
import com.mysql.cj.api.conf.ConnectionPropertiesTransform;
import com.mysql.cj.api.conf.PropertySet;
import com.mysql.cj.core.conf.PropertyDefinitions;
import com.mysql.cj.core.exceptions.CJException;
import com.mysql.cj.core.exceptions.ExceptionFactory;
import com.mysql.cj.core.exceptions.InvalidConnectionAttributeException;
import com.mysql.cj.core.exceptions.WrongArgumentException;
import com.mysql.cj.core.io.NamedPipeSocketFactory;
import com.mysql.cj.core.util.StringUtils;
import com.mysql.cj.core.util.Util;

public class ConnectionString implements Serializable {

    private static final long serialVersionUID = 2456319605323399348L;

    public enum ConnectionStringType {
        SINGLE_CONNECTION("jdbc:mysql://") {
        },

        LOADBALANCING_CONNECTION("jdbc:mysql:loadbalance://") {

            @Override
            void fillPropertiesFromUrl(String url, Properties parsedProperties, Properties masterProps, Properties slavesProps, List<String> slaveHostList,
                    List<String> masterHostList) {
                super.fillPropertiesFromUrl(url, parsedProperties, masterProps, slavesProps, slaveHostList, masterHostList);

                // People tend to drop this in, it doesn't make sense
                parsedProperties.remove(PropertyDefinitions.PNAME_roundRobinLoadBalance);
            }

        },
        FAILOVER_CONNECTION("jdbc:mysql://") {

            @Override
            void fillPropertiesFromUrl(String url, Properties parsedProperties, Properties masterProps, Properties slavesProps, List<String> slaveHostList,
                    List<String> masterHostList) {
                super.fillPropertiesFromUrl(url, parsedProperties, masterProps, slavesProps, slaveHostList, masterHostList);

                // People tend to drop this in, it doesn't make sense
                parsedProperties.remove(PropertyDefinitions.PNAME_roundRobinLoadBalance);
            }

        },
        REPLICATION_CONNECTION("jdbc:mysql:replication://") {

            @Override
            void fillPropertiesFromUrl(String url, Properties parsedProperties, Properties masterProps, Properties slavesProps, List<String> slaveHostList,
                    List<String> masterHostList) {
                super.fillPropertiesFromUrl(url, parsedProperties, masterProps, slavesProps, slaveHostList, masterHostList);

                masterProps.putAll((Properties) parsedProperties.clone());
                slavesProps.putAll((Properties) parsedProperties.clone());

                // Marker used for further testing later on, also when debugging
                slavesProps.setProperty(PropertyDefinitions.PNAME_testsuite_slave_properties, "true");

                int numHosts = Integer.parseInt(parsedProperties.getProperty(PropertyDefinitions.NUM_HOSTS_PROPERTY_KEY));

                if (numHosts < 2) {
                    throw ExceptionFactory.createException(InvalidConnectionAttributeException.class, Messages.getString("NonRegisteringDriver.41"));
                }

                String firstHost = masterProps.getProperty(PropertyDefinitions.HOST_PROPERTY_KEY + ".1") + ":"
                        + masterProps.getProperty(PropertyDefinitions.PORT_PROPERTY_KEY + ".1");

                boolean usesExplicitServerType = isHostPropertiesList(firstHost);

                for (int i = 0; i < numHosts; i++) {
                    int index = i + 1;

                    masterProps.remove(PropertyDefinitions.HOST_PROPERTY_KEY + "." + index);
                    masterProps.remove(PropertyDefinitions.PORT_PROPERTY_KEY + "." + index);
                    slavesProps.remove(PropertyDefinitions.HOST_PROPERTY_KEY + "." + index);
                    slavesProps.remove(PropertyDefinitions.PORT_PROPERTY_KEY + "." + index);

                    String host = parsedProperties.getProperty(PropertyDefinitions.HOST_PROPERTY_KEY + "." + index);
                    String port = parsedProperties.getProperty(PropertyDefinitions.PORT_PROPERTY_KEY + "." + index);
                    if (usesExplicitServerType) {
                        if (isHostMaster(host)) {
                            masterHostList.add(host);
                        } else {
                            slaveHostList.add(host);
                        }
                    } else {
                        if (i == 0) {
                            masterHostList.add(host + ":" + port);
                        } else {
                            slaveHostList.add(host + ":" + port);
                        }
                    }
                }

                slavesProps.remove(PropertyDefinitions.NUM_HOSTS_PROPERTY_KEY);
                masterProps.remove(PropertyDefinitions.NUM_HOSTS_PROPERTY_KEY);
                masterProps.remove(PropertyDefinitions.HOST_PROPERTY_KEY);
                masterProps.remove(PropertyDefinitions.PORT_PROPERTY_KEY);
                slavesProps.remove(PropertyDefinitions.HOST_PROPERTY_KEY);
                slavesProps.remove(PropertyDefinitions.PORT_PROPERTY_KEY);
            }

        },
        /**
         * Driver will create Fabric MySQL connection for URLs of the form:
         * <i>jdbc:mysql:fabric://host:port/?fabricShardTable=employees.employees&amp;fabricShardKey=4621</i>.
         */
        FABRIC_CONNECTION("jdbc:mysql:fabric://") {

            @Override
            void fillPropertiesFromUrl(String url, Properties parsedProperties, Properties masterProps, Properties slavesProps, List<String> slaveHostList,
                    List<String> masterHostList) {
                super.fillPropertiesFromUrl(url, parsedProperties, masterProps, slavesProps, slaveHostList, masterHostList);

                parsedProperties.setProperty(PropertyDefinitions.PNAME_fabricProtocol, "http");
            }

        },
        X_SESSION("mysql:x://") {

        };

        public String urlPrefix;

        private ConnectionStringType(String urlPrefix) {
            this.urlPrefix = urlPrefix;
        }

        /**
         * 
         * @param url
         * @param parsedProperties
         * @param masterProps
         * @param slavesProps
         * @param slaveHostList
         * @param masterHostList
         */
        void fillPropertiesFromUrl(String url, Properties parsedProperties, Properties masterProps, Properties slavesProps, List<String> slaveHostList,
                List<String> masterHostList) {
        }

    }

    // -------------------------------------------------------------

    public class HostInfo implements Serializable {

        private static final long serialVersionUID = 8987442526763929923L;

        private String host;
        private int port;

        public HostInfo(String host, int port) {
            this.host = host;
            this.port = port;
        }

        public String getHost() {
            return this.host;
        }

        public int getPort() {
            return this.port;
        }

    }

    public ConnectionStringType connectionStringType;
    private String url = null;
    private Properties properties = null;
    private Properties masterProps;
    private Properties slavesProps;
    private List<String> slaveHostList = new ArrayList<String>();
    private List<String> masterHostList = new ArrayList<String>();

    public ConnectionString(String url, Properties info) {
        this.url = url;
        this.properties = parseUrl(url, info);

        if (this.properties == null) {
            return;
        }

        if (StringUtils.startsWithIgnoreCase(url, ConnectionStringType.LOADBALANCING_CONNECTION.urlPrefix)) {
            this.connectionStringType = ConnectionStringType.LOADBALANCING_CONNECTION;

        } else if (StringUtils.startsWithIgnoreCase(url, ConnectionStringType.REPLICATION_CONNECTION.urlPrefix)) {
            this.connectionStringType = ConnectionStringType.REPLICATION_CONNECTION;
            this.masterProps = (Properties) this.properties.clone();
            this.slavesProps = (Properties) this.properties.clone();

        } else if (!"1".equals(this.properties.getProperty(PropertyDefinitions.NUM_HOSTS_PROPERTY_KEY))) {
            this.connectionStringType = ConnectionStringType.FAILOVER_CONNECTION;

        } else if (StringUtils.startsWithIgnoreCase(url, ConnectionStringType.X_SESSION.urlPrefix)) {
            this.connectionStringType = ConnectionStringType.X_SESSION;

        } else {
            this.connectionStringType = ConnectionStringType.SINGLE_CONNECTION;
        }

        this.connectionStringType.fillPropertiesFromUrl(url, this.properties, this.masterProps, this.slavesProps, this.slaveHostList, this.masterHostList);

    }

    public String getUrl() {
        return this.url;
    }

    public String getDatabase() {
        return database(this.properties);
    }

    public Properties getProperties() {
        return this.properties;
    }

    public Properties getMasterProps() {
        return this.masterProps;
    }

    public Properties getSlavesProps() {
        return this.slavesProps;
    }

    public List<String> getSlaveHostList() {
        return this.slaveHostList;
    }

    public List<String> getMasterHostList() {
        return this.masterHostList;
    }

    private static final String ALLOWED_QUOTES = "\"'";

    @SuppressWarnings("deprecation")
    public static Properties parseUrl(String url, Properties defaults) {

        if (url == null) {
            /*
             * According to JDBC spec:
             * Exception is thrown if a database access error occurs or the url is null
             */
            throw ExceptionFactory.createException(WrongArgumentException.class, Messages.getString("ConnectionString.0"));

        } else if (!StringUtils.startsWithIgnoreCase(url, ConnectionStringType.SINGLE_CONNECTION.urlPrefix)
                && !StringUtils.startsWithIgnoreCase(url, ConnectionStringType.LOADBALANCING_CONNECTION.urlPrefix)
                && !StringUtils.startsWithIgnoreCase(url, ConnectionStringType.FAILOVER_CONNECTION.urlPrefix)
                && !StringUtils.startsWithIgnoreCase(url, ConnectionStringType.REPLICATION_CONNECTION.urlPrefix)
                && !StringUtils.startsWithIgnoreCase(url, ConnectionStringType.FABRIC_CONNECTION.urlPrefix)
                && !StringUtils.startsWithIgnoreCase(url, ConnectionStringType.X_SESSION.urlPrefix)) {
            /*
             * According to JDBC spec:
             * The driver should return "null" if it realizes it is the wrong kind
             * of driver to connect to the given URL. This will be common, as when
             * the JDBC driver manager is asked to connect to a given URL it passes
             * the URL to each loaded driver in turn.
             */
            return null;
        }

        Properties urlProps = (defaults != null) ? new Properties(defaults) : new Properties();

        int beginningOfSlashes = url.indexOf("//");

        /*
         * Parse parameters after the ? in the URL and remove them from the
         * original URL.
         */
        int index = url.indexOf("?");

        if (index != -1) {
            String paramString = url.substring(index + 1, url.length());
            url = url.substring(0, index);

            StringTokenizer queryParams = new StringTokenizer(paramString, "&");

            while (queryParams.hasMoreTokens()) {
                String parameterValuePair = queryParams.nextToken();

                int indexOfEquals = StringUtils.indexOfIgnoreCase(0, parameterValuePair, "=");

                String parameter = null;
                String value = null;

                if (indexOfEquals != -1) {
                    parameter = parameterValuePair.substring(0, indexOfEquals);

                    if (indexOfEquals + 1 < parameterValuePair.length()) {
                        value = parameterValuePair.substring(indexOfEquals + 1);
                    }
                }

                if ((value != null && value.length() > 0) && (parameter != null && parameter.length() > 0)) {
                    try {
                        urlProps.setProperty(parameter, URLDecoder.decode(value, "UTF-8"));
                    } catch (UnsupportedEncodingException badEncoding) {
                        // punt
                        urlProps.setProperty(parameter, URLDecoder.decode(value));
                    } catch (NoSuchMethodError nsme) {
                        // punt again
                        urlProps.setProperty(parameter, URLDecoder.decode(value));
                    }
                }
            }
        }

        url = url.substring(beginningOfSlashes + 2);

        String hostStuff = null;

        int slashIndex = StringUtils.indexOfIgnoreCase(0, url, "/", ALLOWED_QUOTES, ALLOWED_QUOTES, StringUtils.SEARCH_MODE__ALL);

        if (slashIndex != -1) {
            hostStuff = url.substring(0, slashIndex);

            if ((slashIndex + 1) < url.length()) {
                urlProps.setProperty(PropertyDefinitions.DBNAME_PROPERTY_KEY, url.substring((slashIndex + 1), url.length()));
            }
        } else {
            hostStuff = url;
        }

        int numHosts = 0;

        if ((hostStuff != null) && (hostStuff.trim().length() > 0)) {
            List<String> hosts = StringUtils.split(hostStuff, ",", ALLOWED_QUOTES, ALLOWED_QUOTES, false);

            for (String hostAndPort : hosts) {
                numHosts++;

                String[] hostPortPair = parseHostPortPair(hostAndPort);

                if (hostPortPair[PropertyDefinitions.HOST_NAME_INDEX] != null && hostPortPair[PropertyDefinitions.HOST_NAME_INDEX].trim().length() > 0) {
                    urlProps.setProperty(PropertyDefinitions.HOST_PROPERTY_KEY + "." + numHosts, hostPortPair[PropertyDefinitions.HOST_NAME_INDEX]);
                } else {
                    urlProps.setProperty(PropertyDefinitions.HOST_PROPERTY_KEY + "." + numHosts, "localhost");
                }

                if (hostPortPair[PropertyDefinitions.PORT_NUMBER_INDEX] != null) {
                    urlProps.setProperty(PropertyDefinitions.PORT_PROPERTY_KEY + "." + numHosts, hostPortPair[PropertyDefinitions.PORT_NUMBER_INDEX]);
                } else {
                    urlProps.setProperty(PropertyDefinitions.PORT_PROPERTY_KEY + "." + numHosts, "3306");
                }
            }
        } else {
            numHosts = 1;
            urlProps.setProperty(PropertyDefinitions.HOST_PROPERTY_KEY + ".1", "localhost");
            urlProps.setProperty(PropertyDefinitions.PORT_PROPERTY_KEY + ".1", "3306");
        }

        urlProps.setProperty(PropertyDefinitions.NUM_HOSTS_PROPERTY_KEY, String.valueOf(numHosts));
        urlProps.setProperty(PropertyDefinitions.HOST_PROPERTY_KEY, urlProps.getProperty(PropertyDefinitions.HOST_PROPERTY_KEY + ".1"));
        urlProps.setProperty(PropertyDefinitions.PORT_PROPERTY_KEY, urlProps.getProperty(PropertyDefinitions.PORT_PROPERTY_KEY + ".1"));

        String propertiesTransformClassName = urlProps.getProperty(PropertyDefinitions.PNAME_propertiesTransform);

        if (propertiesTransformClassName != null) {
            try {
                ConnectionPropertiesTransform propTransformer = (ConnectionPropertiesTransform) Class.forName(propertiesTransformClassName).newInstance();

                urlProps = propTransformer.transformProperties(urlProps);
            } catch (InstantiationException | IllegalAccessException | ClassNotFoundException | CJException e) {
                throw ExceptionFactory.createException(InvalidConnectionAttributeException.class,
                        Messages.getString("NonRegisteringDriver.38", new Object[] { propertiesTransformClassName, e.toString() }), e);
            }
        }

        if (Util.isColdFusion() && urlProps.getProperty(PropertyDefinitions.PNAME_autoConfigureForColdFusion, "true").equalsIgnoreCase("true")) {
            String configs = urlProps.getProperty(PropertyDefinitions.PNAME_useConfigs);

            StringBuilder newConfigs = new StringBuilder();

            if (configs != null) {
                newConfigs.append(configs);
                newConfigs.append(",");
            }

            newConfigs.append("coldFusion");

            urlProps.setProperty(PropertyDefinitions.PNAME_useConfigs, newConfigs.toString());
        }

        // If we use a config, it actually should get overridden by anything in the URL or passed-in properties

        String configNames = null;

        if (defaults != null) {
            configNames = defaults.getProperty(PropertyDefinitions.PNAME_useConfigs);
        }

        if (configNames == null) {
            configNames = urlProps.getProperty(PropertyDefinitions.PNAME_useConfigs);
        }

        if (configNames != null) {
            List<String> splitNames = StringUtils.split(configNames, ",", true);

            Properties configProps = new Properties();

            Iterator<String> namesIter = splitNames.iterator();

            while (namesIter.hasNext()) {
                String configName = namesIter.next();

                try {
                    InputStream configAsStream = MysqlConnection.class.getResourceAsStream("../configurations/" + configName + ".properties");

                    if (configAsStream == null) {
                        throw ExceptionFactory.createException(InvalidConnectionAttributeException.class,
                                Messages.getString("NonRegisteringDriver.39", new Object[] { configName }));
                    }
                    configProps.load(configAsStream);
                } catch (IOException ioEx) {
                    throw ExceptionFactory.createException(InvalidConnectionAttributeException.class,
                            Messages.getString("NonRegisteringDriver.40", new Object[] { configName }));
                }
            }

            Iterator<Object> propsIter = urlProps.keySet().iterator();

            while (propsIter.hasNext()) {
                String key = propsIter.next().toString();
                String property = urlProps.getProperty(key);
                configProps.setProperty(key, property);
            }

            urlProps = configProps;
        }

        // Properties passed in should override ones in URL

        if (defaults != null) {
            Iterator<Object> propsIter = defaults.keySet().iterator();

            while (propsIter.hasNext()) {
                String key = propsIter.next().toString();
                if (!key.equals(PropertyDefinitions.NUM_HOSTS_PROPERTY_KEY)) {
                    String property = defaults.getProperty(key);
                    urlProps.setProperty(key, property);
                }
            }
        }

        return urlProps;
    }

    /**
     * Parses hostPortPair in the form of [host][:port] into an array, with the
     * element of index HOST_NAME_INDEX being the host (or null if not
     * specified), and the element of index PORT_NUMBER_INDEX being the port (or
     * null if not specified).
     * 
     * @param hostPortPair
     *            host and port in form of of [host][:port]
     * 
     * @return array containing host and port as Strings
     * 
     */
    public static String[] parseHostPortPair(String hostPortPair) {

        String[] splitValues = new String[2];

        if (StringUtils.startsWithIgnoreCaseAndWs(hostPortPair, "address")) {
            splitValues[PropertyDefinitions.HOST_NAME_INDEX] = hostPortPair.trim();
            splitValues[PropertyDefinitions.PORT_NUMBER_INDEX] = null;

            return splitValues;
        }

        int portIndex = hostPortPair.indexOf(":");

        String hostname = null;

        if (portIndex != -1) {
            if ((portIndex + 1) < hostPortPair.length()) {
                String portAsString = hostPortPair.substring(portIndex + 1);
                hostname = hostPortPair.substring(0, portIndex);

                splitValues[PropertyDefinitions.HOST_NAME_INDEX] = hostname;

                splitValues[PropertyDefinitions.PORT_NUMBER_INDEX] = portAsString;
            } else {
                throw ExceptionFactory.createException(InvalidConnectionAttributeException.class, Messages.getString("NonRegisteringDriver.37"));
            }
        } else {
            splitValues[PropertyDefinitions.HOST_NAME_INDEX] = hostPortPair;
            splitValues[PropertyDefinitions.PORT_NUMBER_INDEX] = null;
        }

        return splitValues;
    }

    /**
     * Returns the hostname property
     * 
     * @param props
     *            the java.util.Properties instance to retrieve the hostname
     *            from.
     * 
     * @return the hostname
     */
    public static String host(Properties props) {
        return props.getProperty(PropertyDefinitions.HOST_PROPERTY_KEY, "localhost");
    }

    /**
     * Returns the port number property
     * 
     * @param props
     *            the properties to get the port number from
     * 
     * @return the port number
     */
    public static int port(Properties props) {
        return Integer.parseInt(props.getProperty(PropertyDefinitions.PORT_PROPERTY_KEY, "3306"));
    }

    /**
     * Returns the database property from <code>props</code>
     * 
     * @param props
     *            the Properties to look for the database property.
     * 
     * @return the database name.
     */
    public static String database(Properties props) {
        String databaseToConnectTo = props.getProperty(PropertyDefinitions.DBNAME_PROPERTY_KEY);
        return databaseToConnectTo == null ? "" : databaseToConnectTo;
    }

    public static List<String> getHosts(Properties props) {

        int numHosts = Integer.parseInt(props.getProperty(PropertyDefinitions.NUM_HOSTS_PROPERTY_KEY));

        List<String> hostList = new ArrayList<String>();

        for (int i = 0; i < numHosts; i++) {
            int index = i + 1;

            hostList.add(props.getProperty(PropertyDefinitions.HOST_PROPERTY_KEY + "." + index) + ":"
                    + props.getProperty(PropertyDefinitions.PORT_PROPERTY_KEY + "." + index));
        }

        return hostList;
    }

    public static boolean isHostPropertiesList(String host) {
        return host != null && StringUtils.startsWithIgnoreCase(host, "address=");
    }

    /**
     * Expands hosts of the form address=(protocol=tcp)(host=localhost)(port=3306)
     * into a java.util.Properties. Special characters (in this case () and =) must be quoted.
     * Any values that are string-quoted ("" or '') are also stripped of quotes.
     */
    public static Properties expandHostKeyValues(String host) {
        Properties hostProps = new Properties();

        if (isHostPropertiesList(host)) {
            host = host.substring("address=".length() + 1);
            List<String> hostPropsList = StringUtils.split(host, ")", "'\"", "'\"", true);

            for (String propDef : hostPropsList) {
                if (propDef.startsWith("(")) {
                    propDef = propDef.substring(1);
                }

                List<String> kvp = StringUtils.split(propDef, "=", "'\"", "'\"", true);

                String key = kvp.get(0);
                String value = kvp.size() > 1 ? kvp.get(1) : null;

                if (value != null && ((value.startsWith("\"") && value.endsWith("\"")) || (value.startsWith("'") && value.endsWith("'")))) {
                    value = value.substring(1, value.length() - 1);
                }

                if (value != null) {
                    if (PropertyDefinitions.HOST_PROPERTY_KEY.equalsIgnoreCase(key) || PropertyDefinitions.DBNAME_PROPERTY_KEY.equalsIgnoreCase(key)
                            || PropertyDefinitions.PORT_PROPERTY_KEY.equalsIgnoreCase(key) || PropertyDefinitions.PROTOCOL_PROPERTY_KEY.equalsIgnoreCase(key)
                            || PropertyDefinitions.PATH_PROPERTY_KEY.equalsIgnoreCase(key)) {
                        key = key.toUpperCase(Locale.ENGLISH);
                    } else if (PropertyDefinitions.PNAME_user.equalsIgnoreCase(key) || PropertyDefinitions.PNAME_password.equalsIgnoreCase(key)) {
                        key = key.toLowerCase(Locale.ENGLISH);
                    }

                    hostProps.setProperty(key, value);
                }
            }
        }

        return hostProps;
    }

    public static boolean isHostMaster(String host) {
        if (isHostPropertiesList(host)) {
            Properties hostSpecificProps = expandHostKeyValues(host);
            if (hostSpecificProps.containsKey("type") && "master".equalsIgnoreCase(hostSpecificProps.getProperty("type"))) {
                return true;
            }
        }
        return false;
    }

    public static String normalizeHost(String hostname) {
        if (hostname == null || StringUtils.isEmptyOrWhitespaceOnly(hostname)) {
            return "localhost";
        }

        return hostname;
    }

    public static int parsePortNumber(String portAsString) {
        try {
            return Integer.parseInt(portAsString);
        } catch (NumberFormatException nfe) {
            throw ExceptionFactory.createException(InvalidConnectionAttributeException.class,
                    Messages.getString("Connection.10", new Object[] { portAsString }), nfe);
        }
    }

    public HostInfo getHostInfo(PropertySet propertySet, String hostToConnectTo, int portToConnectTo, Properties mergedProps) {
        String newHost = "localhost";
        int newPort = 3306;

        String protocolString = mergedProps.getProperty(PropertyDefinitions.PROTOCOL_PROPERTY_KEY);

        if (protocolString != null) {
            // "new" style URL

            if ("tcp".equalsIgnoreCase(protocolString)) {
                newHost = ConnectionString.normalizeHost(mergedProps.getProperty(PropertyDefinitions.HOST_PROPERTY_KEY));
                newPort = ConnectionString.parsePortNumber(mergedProps.getProperty(PropertyDefinitions.PORT_PROPERTY_KEY, "3306"));
            } else if ("pipe".equalsIgnoreCase(protocolString)) {
                propertySet.getModifiableProperty(PropertyDefinitions.PNAME_socketFactory).setValue(NamedPipeSocketFactory.class.getName());

                String path = mergedProps.getProperty(PropertyDefinitions.PATH_PROPERTY_KEY);

                if (path != null) {
                    mergedProps.setProperty(NamedPipeSocketFactory.NAMED_PIPE_PROP_NAME, path);
                }
            } else {
                // normalize for all unknown protocols
                newHost = ConnectionString.normalizeHost(mergedProps.getProperty(PropertyDefinitions.HOST_PROPERTY_KEY));
                newPort = ConnectionString.parsePortNumber(mergedProps.getProperty(PropertyDefinitions.PORT_PROPERTY_KEY, "3306"));
            }
        } else {

            String hostPortPair;

            if (hostToConnectTo == null) {
                hostPortPair = newHost + ":" + portToConnectTo;
            } else {
                newHost = hostToConnectTo;

                if (hostToConnectTo.indexOf(":") == -1) {
                    hostPortPair = newHost + ":" + portToConnectTo;
                } else {
                    hostPortPair = newHost;
                }
            }

            String[] parsedHostPortPair = ConnectionString.parseHostPortPair(hostPortPair);
            newHost = parsedHostPortPair[PropertyDefinitions.HOST_NAME_INDEX];

            newHost = ConnectionString.normalizeHost(newHost);

            if (parsedHostPortPair[PropertyDefinitions.PORT_NUMBER_INDEX] != null) {
                newPort = ConnectionString.parsePortNumber(parsedHostPortPair[PropertyDefinitions.PORT_NUMBER_INDEX]);
            }
        }

        return new HostInfo(newHost, newPort);
    }

}
