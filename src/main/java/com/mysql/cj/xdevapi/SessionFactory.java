/*
  Copyright (c) 2015, 2017, Oracle and/or its affiliates. All rights reserved.

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

import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;

import com.mysql.cj.api.xdevapi.JsonValue;
import com.mysql.cj.api.xdevapi.Session;
import com.mysql.cj.core.conf.PropertyDefinitions;
import com.mysql.cj.core.conf.url.ConnectionUrl;
import com.mysql.cj.core.conf.url.HostInfo;
import com.mysql.cj.core.exceptions.CJCommunicationsException;
import com.mysql.cj.core.exceptions.ExceptionFactory;
import com.mysql.cj.core.exceptions.InvalidConnectionAttributeException;

/**
 * SessionFactory is used for creation of sessions.
 * 
 * <pre>
 * SessionFactory xFactory = new SessionFactory();
 * 
 * {@link Session} session1 = xFactory.getSession("<b>mysqlx:</b>//[user1[:pwd1]@]host1[:port1]/db");
 * {@link Session} session2 = xFactory.getSession("<b>mysqlx:</b>//host2[:port2]/db?user=user2&amp;password=pwd2");
 * </pre>
 *
 */
public class SessionFactory {
    public static final String SESSION_NAME = "sessionName";

    private static final Map<String, String> standardKeysMapping = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
    static {
        standardKeysMapping.put(PropertyDefinitions.HOST_PROPERTY_KEY, PropertyDefinitions.HOST_PROPERTY_KEY);
        standardKeysMapping.put(PropertyDefinitions.PORT_PROPERTY_KEY, PropertyDefinitions.PORT_PROPERTY_KEY);
        standardKeysMapping.put(PropertyDefinitions.DBNAME_PROPERTY_KEY, PropertyDefinitions.DBNAME_PROPERTY_KEY);
        standardKeysMapping.put(PropertyDefinitions.PNAME_schema, PropertyDefinitions.DBNAME_PROPERTY_KEY);
        standardKeysMapping.put(PropertyDefinitions.PNAME_user, PropertyDefinitions.PNAME_user);
        standardKeysMapping.put(PropertyDefinitions.PNAME_password, PropertyDefinitions.PNAME_password);
    }

    /**
     * Fix the case of the key names for the standard session components.
     * 
     * @param props
     *            the properties containing the session components' values.
     * @return a new {@link Properties} with the keys' case fixed as needed.
     */
    private Properties fixStandardKeys(Properties props) {
        Properties sessionProps = new Properties();

        for (String key : props.stringPropertyNames()) {
            String newKey = standardKeysMapping.get(key);
            if (newKey == null) {
                newKey = key;
            }
            sessionProps.setProperty(newKey, props.getProperty(key));
        }

        return sessionProps;
    }

    /**
     * Parses the connection string URL.
     * 
     * @param url
     *            the connection string URL.
     * @return a {@link ConnectionUrl} instance containing the URL components.
     */
    private ConnectionUrl parseUrl(String url) {
        ConnectionUrl connUrl = ConnectionUrl.getConnectionUrlInstance(url, null);
        if (connUrl.getType() != ConnectionUrl.Type.XDEVAPI_SESSION) {
            throw ExceptionFactory.createException(InvalidConnectionAttributeException.class, "Initialization via URL failed for \"" + url + "\"");
        }
        return connUrl;
    }

    /**
     * Gets the String representation of a {@link JsonValue}. {@link JsonString} is returned unescaped.
     * 
     * @param jsonValue
     *            the {@link JsonValue} to convert.
     * @return the String representation of the {@link JsonValue}.
     */
    private String jsonToString(JsonValue jsonValue) {
        if (jsonValue instanceof JsonString) {
            return ((JsonString) jsonValue).getString();
        }
        return jsonValue.toString();
    }

    /**
     * Creates a {@link Session} using the given set of properties, which are complemented or overriden by the second set of properties.
     * 
     * @param sessionProperties
     *            the main set of session properties.
     * @param complementProperties
     *            a second set of properties to add to or override the main ones.
     * @return a {@link Session} instance.
     */
    private Session getSession(Properties sessionProperties, Properties complementProperties) {
        Properties sessionProps = fixStandardKeys(sessionProperties);
        if (complementProperties != null && !complementProperties.isEmpty()) {
            for (String key : complementProperties.stringPropertyNames()) {
                if (!key.equals(SESSION_NAME)) {
                    String targetKey = standardKeysMapping.get(key);
                    if (targetKey == null) {
                        targetKey = key;
                    }
                    sessionProps.setProperty(targetKey, complementProperties.getProperty(key));
                }
            }
        }

        return new SessionImpl(sessionProps);
    }

    /**
     * Creates {@link Session} by given URL.
     * <p>
     * Note that if the URL contains the property {@code sessionName} then a {@link SessionConfig} for the given name will be retrieved and used to fill in any
     * blanks the URL may have.
     * 
     * @param url
     *            the session URL.
     * @return a {@link Session} instance.
     */
    public Session getSession(String url) {
        CJCommunicationsException latestException = null;
        ConnectionUrl connUrl = parseUrl(url);
        for (HostInfo hi : connUrl.getHostsList()) {
            try {
                return getSession(hi.exposeAsProperties());
            } catch (CJCommunicationsException e) {
                latestException = e;
            }
        }
        if (latestException != null) {
            throw latestException;
        }
        return null;
    }

    /**
     * Creates a {@link Session} using the information contained in the given properties.
     * <p>
     * The list of accepted session components includes:
     * <ul>
     * <li><strong>host</strong>: the hostname where the MySQL server is running;</li>
     * <li><strong>port</strong>: the port where the server is listening;</li>
     * <li><strong>user</strong>: the username to authenticate with server;</li>
     * <li><strong>password</strong>: the user's password;</li>
     * <li><strong>schema</strong>: the default schema to connect to;</li>
     * <li><strong>sessionName</strong>: a persistent {@link SessionConfig} name that will be used to fill in the session component blanks.</li>
     * </ul>
     * 
     * @param properties
     *            the {@link Properties} instance that contains the session components.
     * @return a {@link Session} instance.
     */
    public Session getSession(Properties properties) {
        if (properties.containsKey(SESSION_NAME)) {
            String sessionName = properties.getProperty(SESSION_NAME);
            SessionConfig sessionConfig = SessionConfigManager.get(sessionName);
            String url = sessionConfig.getUri();
            ConnectionUrl connUrl = parseUrl(url);

            // TODO: Remains to be decided the behavior for saved URLs with multiple hosts.
            return getSession(connUrl.getMainHost().exposeAsProperties(), properties);
        }

        return getSession(properties, null);
    }

    /**
     * Creates a {@link Session} using the information contained in the given map of properties.
     * <p>
     * The list of accepted session components includes:
     * <ul>
     * <li><strong>host</strong>: the hostname where the MySQL server is running;</li>
     * <li><strong>port</strong>: the port where the server is listening;</li>
     * <li><strong>user</strong>: the username to authenticate with server;</li>
     * <li><strong>password</strong>: the user's password;</li>
     * <li><strong>schema</strong>: the default schema to connect to;</li>
     * <li><strong>sessionName</strong>: a persistent {@link SessionConfig} name that will be used to fill in the session component blanks.</li>
     * </ul>
     * 
     * @param properties
     *            the {@link Map} instance that contains the session components.
     * @return a {@link Session} instance.
     */
    public Session getSession(Map<String, String> properties) {
        Properties sessionProps = new Properties();
        properties.forEach((k, v) -> sessionProps.setProperty(k, v == null ? "null" : v));

        return getSession(sessionProps);
    }

    /**
     * Creates a {@link Session} using the information contained in the given JSON structure.
     * <p>
     * The list of accepted session components includes:
     * <ul>
     * <li><strong>host</strong>: the hostname where the MySQL server is running;</li>
     * <li><strong>port</strong>: the port where the server is listening;</li>
     * <li><strong>user</strong>: the username to authenticate with server;</li>
     * <li><strong>password</strong>: the user's password;</li>
     * <li><strong>schema</strong>: the default schema to connect to;</li>
     * <li><strong>sessionName</strong>: a persistent {@link SessionConfig} name that will be used to fill in the session component blanks.</li>
     * </ul>
     * 
     * @param json
     *            the json structure that contains the session components.
     * @return a {@link Session} instance.
     */
    public Session getSession(DbDoc json) {
        Properties sessionProps = new Properties();
        json.forEach((k, v) -> {
            String vStr = jsonToString(v);
            sessionProps.setProperty(k, vStr == null ? "null" : vStr);
        });

        return getSession(sessionProps);
    }

    /**
     * Creates a {@link Session} for the given {@link SessionConfig}. Note that, by default, a {@link SessionConfig} does not contain password information,
     * unless the user is password-less. In that case, use the method {@link #getSession(SessionConfig, String)} to provide a password for the user.
     * 
     * @param sessionConfig
     *            the {@link SessionConfig} as obtained from the {@link SessionConfigManager} for a saved session.
     * @return a {@link Session} instance.
     */
    public Session getSession(SessionConfig sessionConfig) {
        String url = sessionConfig.getUri();
        ConnectionUrl connUrl = parseUrl(url);

        // TODO: Remains to be decided the behavior for saved URLs with multiple hosts.
        return getSession(connUrl.getMainHost().exposeAsProperties(), null);
    }

    /**
     * Creates a {@link Session} for the given {@link SessionConfig}.
     * 
     * @param sessionConfig
     *            the {@link SessionConfig} as obtained from the {@link SessionConfigManager} for a saved session.
     * @param password
     *            the password to use in the authentication process; the given password will be used even if the {@link SessionConfig} already contains one.
     * @return a {@link Session} instance.
     */
    public Session getSession(SessionConfig sessionConfig, String password) {
        String url = sessionConfig.getUri();
        ConnectionUrl connUrl = parseUrl(url);
        Properties overrideProps = new Properties();
        overrideProps.setProperty(PropertyDefinitions.PNAME_password, password != null ? password : "");

        // TODO: Remains to be decided the behavior for saved URLs with multiple hosts.
        return getSession(connUrl.getMainHost().exposeAsProperties(), overrideProps);
    }
}
