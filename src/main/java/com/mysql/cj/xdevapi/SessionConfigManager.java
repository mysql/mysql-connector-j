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

import java.io.IOException;
import java.io.StringReader;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.mysql.cj.api.conf.PropertyDefinition;
import com.mysql.cj.api.xdevapi.JsonValue;
import com.mysql.cj.api.xdevapi.PasswordHandler;
import com.mysql.cj.api.xdevapi.PersistenceHandler;
import com.mysql.cj.core.conf.PropertyDefinitions;
import com.mysql.cj.core.conf.url.ConnectionUrl;
import com.mysql.cj.core.exceptions.CJOperationNotSupportedException;
import com.mysql.cj.core.exceptions.ExceptionFactory;
import com.mysql.cj.core.util.StringUtils;

/**
 * An API to manage persistent session configurations.
 * 
 * <p>
 * A session configuration, {@link SessionConfig}, is composed of a structure (JSON, {@link Map}, {@link Properties} or other) that contains information for
 * both the X DevApi session and, optionally, client specific session data. The {@link SessionConfigManager} helps handle the persistent storage of such
 * session configuration structures, which can be assessed and saved under a unique name.
 * 
 * <p>
 * The different kinds of mappings representing a session configuration may contain a set of standard keys that constitute the session components. All remaining
 * keys are considered non-standard and are assumed to convey client session information, which the driver knows nothing about. The standard keys are:
 * 
 * <ul>
 * <li><strong>host</strong>: the hostname where the MySQL server is running;</li>
 * <li><strong>port</strong>: the port where the server is listening;</li>
 * <li><strong>user</strong>: the username to authenticate with server;</li>
 * <li><strong>password</strong>: the user's password;</li>
 * <li><strong>schema</strong>: the default schema to connect to;</li>
 * <li><strong>uri</strong>: the full URI containing all the session components;</li>
 * <li><strong>appdata</strong>: the client specific session data;</li>
 * <li><em>connection properties</em>: any of the acceptable connection properties.</li>
 * </ul>
 * 
 * <p>
 * Examples of session configuration structures:
 * 
 * <pre>
 *    { "session01": {"uri": "mysqlx://root@localhost:33060/db1"} } 
 *    { "session02": {"uri": "mysqlx://user:@mysqlserver:33060/db2", "appdata": {"key": "value", ...}} }
 *    { "session03": {"host": "localhost", "port": 33060, "user"="guest", "schema"="db3"} }
 * </pre>
 * 
 * <p>
 * Session configuration data can be given as a JSON structure, such as in the examples above, or given as a URI string, or through a {@link Map} of strings or
 * a {@link Properties} object containing the session components enumerated above.
 * 
 * <p>
 * <strong>Configurations persistence:</strong>
 * 
 * <p>
 * This class provides default session configuration persistence by saving and loading the session configuration data in two configurations files: a read-only
 * system-wide configuration file ("/etc/mysql/sessions.json" or "%PROGRAMDATA%\mysql\sessions.json") and a user-wide read-write configuration file
 * ("~/.mysql/sessions.json" or "%APPDATA%\mysql\sessions.json"). User-wide configurations take precedence over system-wide ones, overriding any duplicates.
 * 
 * <p>
 * A custom-made persistent handler can be used to support any other kinds of persistence. However, in both default and custom-made persistence, the session
 * configuration structures handled by this class will always have the passwords stripped out.
 * 
 * <p>
 * In order to support password persistence, a custom {@link PasswordHandler} must be implemented and registered in this {@link SessionConfigManager} as no
 * default implementation is provided by default.
 */
public final class SessionConfigManager {
    public enum Attr {
        URI("uri"), APPDATA("appdata"), USER("user"), PASSWORD("password"), HOST("host"), PORT("port"), SCHEMA("schema");

        private String key;

        private Attr(String key) {
            this.key = key;
        }

        public String getKey() {
            return this.key;
        }

        public static boolean contains(String key) {
            return Stream.of(Attr.values()).filter(a -> a.getKey().equals(key)).findFirst().isPresent();
        }
    }

    private static PersistenceHandler persistenceHandler = new DefaultPersistenceHandler();
    private static PasswordHandler passwordHandler = new PasswordHandler() {
    };

    /*
     * Prevents instantiation.
     */
    private SessionConfigManager() {
    }

    /**
     * Saves a session configuration structure for the given URI and application data. Regardless of the contents of the application data structure, it is all
     * considered client session data; as such, none of the included elements interfere with the session configuration components.
     * 
     * <p>
     * The session configuration components are extracted from the URI, then combined with the given application data and saved using the registered
     * {@link PersistenceHandler} and {@link PasswordHandler}.
     * 
     * <p>
     * Note that passwords are stripped out from the session configuration components if no {@link PasswordHandler} was previously registered in the
     * {@link SessionConfigManager}.
     * 
     * @param name
     *            the session configuration name.
     * @param uri
     *            the session URI.
     * @param appDataJson
     *            a JSON string containing the client session data.
     * @return a {@link SessionConfig} instance containing the session configuration data.
     */
    public static SessionConfig save(String name, String uri, String appDataJson) {
        try {
            return save(name, uri, JsonParser.parseDoc(new StringReader(appDataJson)));
        } catch (IOException e) {
            throw ExceptionFactory.createException("Failed to parse the given application data.");
        }
    }

    /**
     * Saves a session configuration structure for the given URI and application data. Regardless of the contents of the application data structure, it is all
     * considered client session data; as such, none of the included elements interfere with the session configuration components.
     * 
     * <p>
     * The session configuration components are extracted from the URI, then combined with the given application data and saved using the registered
     * {@link PersistenceHandler} and {@link PasswordHandler}.
     * 
     * <p>
     * Note that passwords are stripped out from the session configuration components if no {@link PasswordHandler} was previously registered in the
     * {@link SessionConfigManager}.
     * 
     * @param name
     *            the session configuration name.
     * @param uri
     *            the session URI.
     * @param appDataProps
     *            a {@link Properties} map containing the client session data.
     * @return a {@link SessionConfig} instance containing the session configuration data.
     */
    public static SessionConfig save(String name, String uri, Properties appDataProps) {
        DbDoc appData = new DbDoc();
        for (String key : appDataProps.stringPropertyNames()) {
            appData.put(key, new JsonString().setValue(appDataProps.getProperty(key)));
        }
        return save(name, uri, appData);
    }

    /**
     * Saves a session configuration structure for the given URI and application data. Regardless of the contents of the application data structure, it is all
     * considered client session data; as such, none of the included elements interfere with the session configuration components.
     * 
     * <p>
     * The session configuration components are extracted from the URI, then combined with the given application data and saved using the registered
     * {@link PersistenceHandler} and {@link PasswordHandler}.
     * 
     * <p>
     * Note that passwords are stripped out from the session configuration components if no {@link PasswordHandler} was previously registered in the
     * {@link SessionConfigManager}.
     * 
     * @param name
     *            the session configuration name.
     * @param uri
     *            the session URI.
     * @param appDataMap
     *            a {@link Map} of strings containing the client session data.
     * @return a {@link SessionConfig} instance containing the session configuration data.
     */
    public static SessionConfig save(String name, String uri, Map<String, String> appDataMap) {
        DbDoc appData = appDataMap.entrySet().stream()
                .collect(Collectors.toMap(Entry::getKey, e -> new JsonString().setValue(e.getValue()), (a, b) -> a, DbDoc::new));
        return save(name, uri, appData);
    }

    /**
     * Saves a session configuration structure for the given URI and application data. Regardless of the contents of the application data structure, it is all
     * considered client session data; as such, none of the included elements interfere with the session configuration components.
     * 
     * <p>
     * The session configuration components are extracted from the URI, then combined with the given application data and saved using the registered
     * {@link PersistenceHandler} and {@link PasswordHandler}.
     * 
     * <p>
     * Note that passwords are stripped out from the session configuration components if no {@link PasswordHandler} was previously registered in the
     * {@link SessionConfigManager}.
     * 
     * @param name
     *            the session configuration name.
     * @param uri
     *            the session URI.
     * @param appDataJson
     *            a JSON structure containing the client session data.
     * @return a {@link SessionConfig} instance containing the session configuration data.
     */
    public static SessionConfig save(String name, String uri, DbDoc appDataJson) {
        DbDoc configData = new DbDoc();
        configData.put(Attr.URI.getKey(), new JsonString().setValue(uri));
        configData.put(Attr.APPDATA.getKey(), appDataJson);
        return save(name, configData);
    }

    /**
     * Saves a session configuration structure built from the given URI or JSON string. If the given string starts with "mysqlx://" then it is considered a URI;
     * otherwise it is considered a JSON string and an attempt to parse it to a JSON structure will be made.
     * 
     * <p>
     * The session configuration components are extracted from either the URI or the JSON structure and then saved using the registered
     * {@link PersistenceHandler} and {@link PasswordHandler}.
     * 
     * <p>
     * If the argument is a JSON structure then it must contain either the element 'uri' or a set of session components ('host', 'port', ...), and optionally
     * an 'appdata'.
     * 
     * <p>
     * Note that passwords are stripped out from the session configuration components if no {@link PasswordHandler} was previously registered in the
     * {@link SessionConfigManager}.
     * 
     * @param name
     *            the session configuration name.
     * @param uriOrJson
     *            the session URI or JSON string.
     * @return a {@link SessionConfig} instance containing the session configuration data.
     */
    public static SessionConfig save(String name, String uriOrJson) {
        DbDoc configData;

        if (uriOrJson.trim().startsWith("mysqlx://")) { // Is this a URI?
            configData = new DbDoc();
            configData.put(Attr.URI.getKey(), new JsonString().setValue(uriOrJson));
        } else { // No? Then it must be a JSON doc.
            try {
                configData = JsonParser.parseDoc(new StringReader(uriOrJson));
            } catch (IOException e) {
                throw ExceptionFactory.createException("Failed to parse the given JSON structure.", e);
            }
        }

        return save(name, configData);
    }

    /**
     * Saves a session configuration structure built from the given properties set. The session configuration components are extracted from the properties and
     * then saved using the registered {@link PersistenceHandler} and {@link PasswordHandler}.
     * 
     * <p>
     * Only the acceptable properties (see in the main description above) are used for constructing a session configuration structure. Remaining properties are
     * considered client application data and will be stored under the 'appdata' attribute.
     * 
     * <p>
     * Note that passwords are stripped out from the session configuration components if no {@link PasswordHandler} was previously registered in the
     * {@link SessionConfigManager}.
     * 
     * @param name
     *            the session configuration name.
     * @param data
     *            the properties set containing both the session configuration components and client application data.
     * @return a {@link SessionConfig} instance containing the session configuration data.
     */
    public static SessionConfig save(String name, Properties data) {
        DbDoc configData = new DbDoc();
        DbDoc appData = new DbDoc();

        for (String key : data.stringPropertyNames()) {
            if (key.equals(Attr.APPDATA.getKey())) {
                try {
                    DbDoc tmpAppData = JsonParser.parseDoc(new StringReader(data.getProperty(key)));
                    appData.putAll(tmpAppData);
                } catch (IOException ex) {
                    throw ExceptionFactory.createException("Failed to parse the internal 'appdata' JSON structure.", ex);
                }
            } else if (isValidAttribute(key)) {
                configData.put(key, new JsonString().setValue(data.getProperty(key)));
            } else {
                appData.put(key, new JsonString().setValue(data.getProperty(key)));
            }
        }
        if (appData.size() > 0) {
            configData.put(Attr.APPDATA.getKey(), appData);
        }

        return save(name, configData);
    }

    /**
     * Saves a session configuration structure built from the given map of strings. The session configuration components are extracted from the map and then
     * saved using the registered {@link PersistenceHandler} and {@link PasswordHandler}.
     * 
     * <p>
     * Only the acceptable properties (see in the main description above) are used for constructing a session configuration structure. Remaining properties are
     * considered client application data and will be stored under the 'appdata' attribute.
     * 
     * <p>
     * Note that passwords are stripped out from the session configuration components if no {@link PasswordHandler} was previously registered in the
     * {@link SessionConfigManager}.
     * 
     * @param name
     *            the session configuration name.
     * @param data
     *            the map of strings containing both the session configuration components and client application data.
     * @return a {@link SessionConfig} instance containing the session configuration data.
     */
    public static SessionConfig save(String name, Map<String, String> data) {
        DbDoc configData = new DbDoc();
        DbDoc appData = new DbDoc();

        for (Entry<String, String> entry : data.entrySet()) {
            if (entry.getKey().equals(Attr.APPDATA.getKey())) {
                try {
                    DbDoc tmpAppData = JsonParser.parseDoc(new StringReader(entry.getValue()));
                    appData.putAll(tmpAppData);
                } catch (IOException ex) {
                    throw ExceptionFactory.createException("Failed to parse the internal 'appdata' JSON structure.", ex);
                }
            } else if (isValidAttribute(entry.getKey())) {
                configData.put(entry.getKey(), new JsonString().setValue(entry.getValue()));
            } else {
                appData.put(entry.getKey(), new JsonString().setValue(entry.getValue()));
            }
        }
        if (appData.size() > 0) {
            configData.put(Attr.APPDATA.getKey(), appData);
        }
        return save(name, configData);
    }

    /**
     * Saves a session configuration structure built from the given JSON structure. The session configuration components are extracted from the map and then
     * saved using the registered {@link PersistenceHandler} and {@link PasswordHandler}.
     * 
     * <p>
     * The given JSON structure must contain either a 'uri' or a set of session components ('host', 'port', ...), and optionally an
     * 'appdata'.
     * 
     * <p>
     * Note that passwords are stripped out from the session configuration components if no {@link PasswordHandler} was previously registered in the
     * {@link SessionConfigManager}.
     * 
     * @param name
     *            the session configuration name.
     * @param data
     *            the JSON structure containing both the session configuration components and client application data.
     * @return a {@link SessionConfig} instance containing the session configuration data.
     */
    public static SessionConfig save(String name, DbDoc data) {
        String user;
        boolean hasPassword;
        String password;
        String host;
        String port;
        String schema;
        Map<String, String> connProps;
        String uri;

        DbDoc configData = new DbDoc();

        if (data.containsKey(Attr.URI.getKey())) { // Is this a 'uri' + 'appdata' JSON doc?
            if (data.size() > 2 || data.size() == 2 && !data.containsKey(Attr.APPDATA.getKey())) {
                throw ExceptionFactory.createException(
                        "Invalid JSON document. Only the keys '" + Attr.URI.getKey() + "' and '" + Attr.APPDATA.getKey() + "' are allowed together.");
            }

            uri = jsonToString(data.get(Attr.URI.getKey()));

        } else { // No? Then it should be a JSON doc with URI components + 'appdata'.
            if (!data.keySet().stream().allMatch(SessionConfigManager::isValidAttribute)) {
                throw ExceptionFactory.createException("Invalid JSON document. Only the URI component keys '" + Attr.USER.getKey() + ", "
                        + Attr.PASSWORD.getKey() + ", " + Attr.HOST.getKey() + ", " + Attr.PORT.getKey() + " and " + Attr.SCHEMA.getKey()
                        + "', valid connection properties and '" + Attr.APPDATA.getKey() + "' are allowed together.");
            }

            user = data.containsKey(Attr.USER.getKey()) ? jsonToString(data.get(Attr.USER.getKey())) : "";
            hasPassword = data.containsKey(Attr.PASSWORD.getKey());
            password = hasPassword ? jsonToString(data.get(Attr.PASSWORD.getKey())) : null;
            host = data.containsKey(Attr.HOST.getKey()) ? jsonToString(data.get(Attr.HOST.getKey())) : "";
            port = data.containsKey(Attr.PORT.getKey()) ? jsonToString(data.get(Attr.PORT.getKey())) : "";
            schema = data.containsKey(Attr.SCHEMA.getKey()) ? jsonToString(data.get(Attr.SCHEMA.getKey())) : "";
            connProps = data.entrySet().stream().filter(e -> !Attr.contains(e.getKey()))
                    .collect(Collectors.toMap(Entry::getKey, e -> jsonToString(e.getValue())));

            uri = buildUri(user, hasPassword, password, host, port, schema, connProps);
        }

        // Let ConnectionUrl parse the uri and fill in defaults if needed.
        ConnectionUrl connUrl = ConnectionUrl.getConnectionUrlInstance(uri, null);

        user = connUrl.getMainHost().getUser();
        hasPassword = !connUrl.getMainHost().isPasswordless();
        password = hasPassword ? connUrl.getMainHost().getPassword() : null;
        host = connUrl.getMainHost().getHost();
        port = String.valueOf(connUrl.getMainHost().getPort());
        schema = connUrl.getMainHost().getDatabase();
        connProps = connUrl.getOriginalProperties().entrySet().stream().filter(e -> !Attr.contains(e.getKey()))
                .collect(Collectors.toMap(Entry::getKey, Entry::getValue));

        String rebuiltUri = buildUri(user, hasPassword, password, host, port, schema, connProps);

        String passwordlessUri = buildPasswordlessUri(user, hasPassword, host, port, schema, connProps);
        configData.put(Attr.URI.getKey(), new JsonString().setValue(passwordlessUri));

        DbDoc appData = data.containsKey(Attr.APPDATA.getKey()) ? ((DbDoc) data.get(Attr.APPDATA.getKey())) : null;
        if (appData != null && !appData.isEmpty()) {
            configData.put(Attr.APPDATA.getKey(), appData);
        }

        try {
            persistenceHandler.save(name, configData);

            if (hasPassword) {
                String key = user;
                String service = host + ":" + port;
                try {
                    passwordHandler.save(key, service, password);
                } catch (CJOperationNotSupportedException e) {
                    // It is ok not to have a default password handler implementation.
                }
            }
        } catch (Exception e) {
            throw ExceptionFactory.createException("Failed to save the session configuration '" + name + "'.", e);
        }

        return makeSessionConfig(name, rebuiltUri, appData);
    }

    /**
     * Saves a {@link SessionConfig} object using the registered {@link PersistenceHandler}.
     * 
     * <p>
     * Note that passwords are stripped out from the session configuration components if no {@link PasswordHandler} was previously registered in this
     * {@link SessionConfigManager}.
     * 
     * @param sessionConfig
     *            the {@link SessionConfig} object.
     * @return a {@link SessionConfig} instance containing the session configuration data.
     */
    public static SessionConfig save(SessionConfig sessionConfig) {
        DbDoc appData = sessionConfig.getAppData().entrySet().stream()
                .collect(Collectors.toMap(Entry::getKey, e -> new JsonString().setValue(e.getValue()), (a, b) -> a, DbDoc::new));
        return save(sessionConfig.getName(), sessionConfig.getUri(), appData);
    }

    /**
     * Loads the session configuration structure for the given name from the registered {@link PersistenceHandler}.
     * 
     * @param name
     *            the session configuration name.
     * @return a {@link SessionConfig} instance containing the session configuration data.
     */
    public static SessionConfig get(String name) {
        try {
            DbDoc configData = SessionConfigManager.persistenceHandler.load(name);

            if (!configData.containsKey(Attr.URI.getKey())) {
                ExceptionFactory.createException("URI missing from session configuration '" + name + "'.");
            }

            String uri = jsonToString(configData.get(Attr.URI.getKey()));
            DbDoc appData = (DbDoc) configData.get(Attr.APPDATA.getKey());

            // Let ConnectionUrl parse the uri and fill in defaults if needed.
            ConnectionUrl connUrl = ConnectionUrl.getConnectionUrlInstance(uri, null);

            String user = connUrl.getMainHost().getUser();
            boolean hasPassword = !connUrl.getMainHost().isPasswordless();
            String password = null; // Skip any password that my have come in the configuration data.
            String host = connUrl.getMainHost().getHost();
            String port = String.valueOf(connUrl.getMainHost().getPort());
            String schema = connUrl.getMainHost().getDatabase();
            Map<String, String> connProps = connUrl.getOriginalProperties().entrySet().stream().filter(e -> !Attr.contains(e.getKey()))
                    .collect(Collectors.toMap(Entry::getKey, Entry::getValue));

            String key = user;
            String service = host + ":" + port;
            try {
                password = passwordHandler.load(key, service);
            } catch (CJOperationNotSupportedException e) {
                // It is ok not to have a default password handler implementation.
            }
            hasPassword = hasPassword || password != null;

            String rebuiltUri = buildUri(user, hasPassword, password, host, port, schema, connProps);

            return makeSessionConfig(name, rebuiltUri, appData);
        } catch (Exception e) {
            throw ExceptionFactory.createException("Failed to get the session configuration '" + name + "'.", e);
        }
    }

    /**
     * Deletes the named session configuration structure from the registered {@link PersistenceHandler}.
     * 
     * @param name
     *            the session configuration name.
     * @return true if the named session configuration structure was deleted successfully, false otherwise.
     */
    public static boolean delete(String name) {
        try {
            return SessionConfigManager.persistenceHandler.delete(name);
        } catch (Exception e) {
            throw ExceptionFactory.createException("Failed to delete the session configuration '" + name + "'.", e);
        }
    }

    /**
     * Retrieves a list of session configuration names retrieved from the registered {@link PersistenceHandler}.
     * 
     * @return a list of session configuration names.
     */
    public static List<String> list() {
        try {
            return SessionConfigManager.persistenceHandler.list();
        } catch (Exception e) {
            throw ExceptionFactory.createException("Failed to get the session configurations list.", e);
        }
    }

    /**
     * Registers the {@link PersistenceHandler} to use in saving and loading persistent session configurations.
     * 
     * @param persistenceHandler
     *            the {@link PersistenceHandler} to use in the save and load operations. A {@code null} value sets a default {@link PersistenceHandler} with no
     *            operations implemented.
     */
    public static synchronized void setPersistenceHandler(PersistenceHandler persistenceHandler) {
        if (persistenceHandler == null) {
            SessionConfigManager.persistenceHandler = new PersistenceHandler() {
            };
        } else {
            SessionConfigManager.persistenceHandler = persistenceHandler;
        }
    }

    /**
     * Registers the {@link PasswordHandler} to use in saving and loading persistent passwords.
     * 
     * @param passwordHandler
     *            the {@link PasswordHandler} to use in the save and load operations. A {@code null} value sets a default {@link PasswordHandler} with no
     *            operations implemented.
     */
    public static synchronized void setPasswordHandler(PasswordHandler passwordHandler) {
        if (passwordHandler == null) {
            SessionConfigManager.passwordHandler = new PasswordHandler() {
            };
        } else {
            SessionConfigManager.passwordHandler = passwordHandler;
        }
    }

    /**
     * Checks if the given key is a valid session configuration attribute.
     * 
     * @param key
     *            the key to check.
     * @return
     *         true if the key is a valid session configuration attribute, false otherwise.
     */
    private static boolean isValidAttribute(String key) {
        PropertyDefinition<?> pDef;
        return Attr.contains(key)
                || (pDef = PropertyDefinitions.getPropertyDefinition(key)) != null && pDef.getCategory().equals(PropertyDefinitions.CATEGORY_XDEVAPI);
    }

    /**
     * Gets the String representation of a {@link JsonValue}. {@link JsonString} is returned un-escaped.
     * 
     * @param jsonValue
     *            the {@link JsonValue} to convert.
     * @return
     *         the String representation of the {@link JsonValue}.
     */
    private static String jsonToString(JsonValue jsonValue) {
        if (jsonValue instanceof JsonString) {
            return ((JsonString) jsonValue).getString();
        }
        return jsonValue.toString();
    }

    /**
     * Builds a URI string using the given components. In the resulting URI, the separator between the user and password (':') is omitted if this is a
     * password-less user; otherwise the separator is included and the password may or may not be exposed.
     * 
     * @param user
     *            the user.
     * @param hasPassword
     *            has this user a password?
     * @param password
     *            the password for this user. A {@code null} value omitts the password, but not the user/password separator.
     * @param host
     *            the host name.
     * @param port
     *            the TCP port.
     * @param schema
     *            the default schema.
     * @param connProps
     *            a {@link Map} containing the remaining session properties.
     * @return a string representing the URI composed by the given components.
     */
    private static String buildUri(String user, boolean hasPassword, String password, String host, String port, String schema, Map<String, String> connProps) {
        StringBuilder uri = new StringBuilder(ConnectionUrl.Type.XDEVAPI_SESSION.getProtocol());
        uri.append("//");
        if (!StringUtils.isEmptyOrWhitespaceOnly(user)) {
            uri.append(user);
            if (hasPassword) {
                uri.append(':');
                if (password != null) {
                    uri.append(password);
                }
            }
            uri.append('@');
        }
        if (!StringUtils.isEmptyOrWhitespaceOnly(host)) {
            uri.append(host);
        }
        if (!StringUtils.isEmptyOrWhitespaceOnly(port)) {
            uri.append(':');
            uri.append(port);
        }
        uri.append('/');
        if (!StringUtils.isEmptyOrWhitespaceOnly(schema)) {
            uri.append(schema);
        }
        if (!connProps.isEmpty()) {
            uri.append('?');
            String connPropsStr = connProps.entrySet().stream().map(e -> e.getKey() + "=" + e.getValue()).collect(Collectors.joining("&"));
            uri.append(connPropsStr);
        }
        return uri.toString();
    }

    /**
     * Builds a URI string using the given components. In the resulting URI, the separator between the user and password (':') is omitted if this is a
     * password-less user; otherwise the separator is included and the password is not exposed.
     * 
     * @param user
     *            the user.
     * @param hasPassword
     *            has this user a password?
     * @param host
     *            the host name.
     * @param port
     *            the TCP port.
     * @param schema
     *            the default schema.
     * @param connProps
     *            a {@link Map} containing the remaining session properties.
     * @return a string representing the URI composed by the given components.
     */
    private static String buildPasswordlessUri(String user, boolean hasPassword, String host, String port, String schema, Map<String, String> connProps) {
        return buildUri(user, hasPassword, null, host, port, schema, connProps);
    }

    /**
     * Creates a {@link SessionConfig} instance composed by the given elements.
     * 
     * @param name
     *            the session configuration name.
     * @param uri
     *            the session URI.
     * @param appData
     *            the client application data.
     * @return a {@link SessionConfig} object defined by the given name, URI and application data.
     */
    private static SessionConfig makeSessionConfig(String name, String uri, DbDoc appData) {
        SessionConfig sessionConfig = new SessionConfig(name, uri);
        if (appData != null && !appData.isEmpty()) {
            appData.entrySet().forEach(e -> sessionConfig.setAppData(e.getKey(), jsonToString(e.getValue())));
        }
        return sessionConfig;
    }
}
