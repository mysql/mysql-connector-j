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

import static com.mysql.cj.core.util.StringUtils.isNullOrEmpty;

import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.mysql.cj.api.conf.DatabaseUrlContainer;
import com.mysql.cj.core.Messages;
import com.mysql.cj.core.exceptions.ExceptionFactory;
import com.mysql.cj.core.exceptions.WrongArgumentException;
import com.mysql.cj.core.util.StringUtils;

/**
 * This class parses a connection string using the general URI structure defined in RFC 3986. Instead of using a URI instance to ensure the correct syntax of
 * the connection string, this implementation uses regular expressions which is faster but also less strict in terms of validations. This actually works better
 * because database URLs don't exactly stick to the RFC 3986 rules.
 * <p>
 * <i>scheme://authority/path?query#fragment</i>
 * <p>
 * This results in splitting the connection string URL and processing its internal parts:
 * <dl>
 * <dt>scheme</dt>
 * <dd>The protocol and subprotocol identification. Usually "jdbc:mysql:" or "mysqlx:".</dd>
 * <dt>authority</dt>
 * <dd>Contains information about the user credentials and/or the host and port information. Unlike its definition in the RFC 3986 specification, there can be
 * multiple authority sections separated by a single comma (,) in a connection string. It is also possible to use an alternative syntax for the user and/or host
 * identification, that also allows setting per host connection properties, in the form of
 * "[user[:password]@]address=(key1=value)[(key2=value)]...[,address=(key3=value)[(key4=value)]...]...".</dd>
 * <dt>path</dt>
 * <dd>Corresponds to the database identification.</dd>
 * <dt>query</dt>
 * <dd>The connection properties, written as "propertyName1[=[propertyValue1]][&propertyName2[=[propertyValue2]]]..."</dd>
 * <dt>fragment</dt>
 * <dd>The fragment section is ignored in Connector/J connection strings.</dd>
 * </dl>
 */
public class ConnectionUrlParser implements DatabaseUrlContainer {
    private static final String DUMMY_SCHEMA = "cj://";
    private static final String ALTERNATE_HOST_INFO_PREFIX = "ADDRESS=";
    private static final String USER_PASS_SEPARATOR = ":";
    private static final String USERINFO_HOST_SEPARATOR = "@";
    private static final String HOST_PORT_SEPARATOR = ":";
    private static final String HOSTS_SEPARATOR = ",";

    private static final Pattern CONNECTION_STRING_PTRN = Pattern
            .compile("(?<scheme>[\\w:]+)(?://(?<authority>[^/?#]*))?(?:/(?<path>[^?#]*))?(?:\\?(?<query>[^#]*))?(?<fragment>.*)");
    private static final Pattern ALTERNATE_HOST_INFO_PTRN = Pattern.compile("\\((?<key>\\w+)(?:=(?<value>[^)]*))?\\)");
    private static final Pattern PROPERTIES_PTRN = Pattern.compile("&*(?<key>\\w+)(?:=(?<value>[^&]*))?(?:&+|$)");
    private static final Pattern GENERIC_HOST_PORT_PTRN = Pattern.compile("(?<host>^.*):(?<port>\\d*)$");

    private final String baseConnectionString;
    private String scheme;
    private String authority;
    private String path;
    private String query;

    private List<HostInfo> parsedHosts = null;
    private Map<String, String> parsedProperties = null;

    /**
     * Static factory method for constructing instances of this class.
     * 
     * @param connString
     *            The connection string to parse.
     * @return an instance of {@link ConnectionUrlParser}
     */
    public static ConnectionUrlParser parseConnectionString(String connString) {
        if (connString == null) {
            throw ExceptionFactory.createException(WrongArgumentException.class, Messages.getString("ConnectionString.0"));
        }
        return new ConnectionUrlParser(connString);
    }

    /**
     * Constructs a connection string parser for the given connection string.
     * 
     * @param connString
     *            the connection string to parse
     */
    private ConnectionUrlParser(String connString) {
        this.baseConnectionString = connString;
        parseConnectionString();
    }

    /**
     * Splits the connection string in its main sections.
     */
    private void parseConnectionString() {
        String connString = this.baseConnectionString;
        Matcher matcher = CONNECTION_STRING_PTRN.matcher(connString);
        if (!matcher.matches()) {
            throw ExceptionFactory.createException(WrongArgumentException.class, Messages.getString("ConnectionString.1"));
        }
        this.scheme = decode(matcher.group("scheme"));
        this.authority = matcher.group("authority"); // Don't decode just yet.
        this.path = decode(matcher.group("path"));
        this.query = matcher.group("query"); // Don't decode just yet.
    }

    /**
     * Parses the authority section (user and/or host identification) of the connection string URI.
     */
    private void parseAuthoritySection() {
        if (isNullOrEmpty(this.authority)) {
            // Add an empty, default, host.
            this.parsedHosts.add(new HostInfo());
            return;
        }

        String[] authorityParts = this.authority.split(HOSTS_SEPARATOR, -1);
        for (String hi : authorityParts) {
            HostInfo hostInfo = parseHostInfo(hi);
            this.parsedHosts.add(hostInfo);
        }
    }

    /**
     * Parses the given host information which is formed by the parts [host][:port].
     * <p>
     * This method also handles the alternative host information syntax:
     * "[user[:password]@]address=(key1=value)[(key2=value)]...[,address=(key3=value)[(key4=value)]...]..."
     * 
     * @param hostInfo
     *            the string containing the host information
     * @return an instance of {@link HostInfo} containing the host and port information
     */
    private HostInfo parseHostInfo(String hostInfo) {
        if (isNullOrEmpty(hostInfo)) {
            // Return an empty, default, host.
            return new HostInfo();
        }

        String host = null;
        int port = -1;
        String user = null;
        String password = null;
        Pair<String, String> userInfo = null;

        /*
         * Start by using a java.net.URI instance to parse the host information. This helps dealing with the IPv6 syntax.
         */
        try {
            URI uri = URI.create(DUMMY_SCHEMA + hostInfo);
            if (uri.getHost() != null) {
                host = uri.getHost();
            }
            if (uri.getPort() != -1) {
                port = uri.getPort();
            }
            if (uri.getUserInfo() != null) {
                userInfo = parseUserInfo(uri.getUserInfo());
                user = userInfo.left;
                password = userInfo.right;
            }
        } catch (IllegalArgumentException e) {
            // The URI failed to parse the host information. Let's move on and try a different process.
        }
        if (host != null || port != -1 || userInfo != null) {
            // The host info parsing succeeded. Return now.
            return new HostInfo(this, host, port, user, password);
        }

        /*
         * The URI failed to parse the host information, let's check if it is written in the alternate syntax "address=(...)".
         */
        int p = StringUtils.indexOfIgnoreCase(hostInfo, ALTERNATE_HOST_INFO_PREFIX);
        if (p >= 0) {
            String[] altHostInfoParts = hostInfo.split("(?i)" + ALTERNATE_HOST_INFO_PREFIX, 2);
            // If there is a part left to 'address=' it must be a user info ending with '@'.
            if (!altHostInfoParts[0].isEmpty()) {
                if (!altHostInfoParts[0].endsWith(USERINFO_HOST_SEPARATOR)) {
                    throw ExceptionFactory.createException(WrongArgumentException.class,
                            Messages.getString("ConnectionString.2", new Object[] { USERINFO_HOST_SEPARATOR, ALTERNATE_HOST_INFO_PREFIX.toLowerCase() }));
                }
                userInfo = parseUserInfo(altHostInfoParts[0].substring(0, altHostInfoParts[0].length() - USERINFO_HOST_SEPARATOR.length()));
                user = userInfo.left;
                password = userInfo.right;
            }
            return new HostInfo(this, host, port, user, password, parseAlternateHostInfo(altHostInfoParts[1]));
        }

        /*
         * Alternate syntax also failed, let's wind up the corner cases the URI couldn't handle.
         */
        int s = 0;
        p = hostInfo.indexOf(USERINFO_HOST_SEPARATOR);
        if (p >= 0) {
            userInfo = parseUserInfo(hostInfo.substring(0, p));
            user = userInfo.left;
            password = userInfo.right;
            s = p + USERINFO_HOST_SEPARATOR.length();
        }
        p = hostInfo.indexOf(HOST_PORT_SEPARATOR, s);
        if (p >= 0) {
            String[] hostPortParts = hostInfo.substring(s).split(HOST_PORT_SEPARATOR, 2);
            host = hostPortParts.length > 0 && !hostPortParts[0].isEmpty() ? decode(hostPortParts[0]) : null;
            port = hostPortParts.length > 1 && !hostPortParts[1].isEmpty() ? Integer.parseInt(decode(hostPortParts[1])) : -1;
        } else {
            String hostPart = decode(hostInfo.substring(s));
            host = !hostPart.isEmpty() ? hostPart : null;
        }
        return new HostInfo(this, host, port, user, password);
    }

    /**
     * Parses the given user information which is formed by the parts [user][:password].
     * 
     * @param userInfo
     *            the string containing the user information
     * @return a {@link Pair} containing the user and password information
     */
    private static Pair<String, String> parseUserInfo(String userInfo) {
        String[] userInfoParts = userInfo.split(USER_PASS_SEPARATOR, 2);
        String userName = userInfoParts.length > 0 && !userInfoParts[0].isEmpty() ? decode(userInfoParts[0]) : null;
        String password = userInfoParts.length > 1 && !userInfoParts[1].isEmpty() ? decode(userInfoParts[1]) : null;
        return new Pair<String, String>(userName, password);
    }

    /**
     * Parses a host:port pair and returns the two elements in a {@link Pair}
     * 
     * @param hostPortPair
     *            the host:pair to parse
     * @return a {@link Pair} containing the host and port information or null if it didn't match the pattern
     */
    public static Pair<String, String> parseHostPortPair(String hostPortPair) {
        Matcher matcher = GENERIC_HOST_PORT_PTRN.matcher(hostPortPair);
        if (matcher.matches()) {
            return new Pair<String, String>(matcher.group("host"), matcher.group("port"));
        }
        throw ExceptionFactory.createException(WrongArgumentException.class, Messages.getString("ConnectionString.3", new Object[] { hostPortPair }));
    }

    /**
     * Parses the alternate host information syntax ("address=...")
     * 
     * @param altHostInfo
     *            the string containing the host information
     * @return a map containing all the key/value pairs extracted from the given host information
     */
    private Map<String, String> parseAlternateHostInfo(String altHostInfo) {
        return processKeyValuePattern(ALTERNATE_HOST_INFO_PTRN, altHostInfo);
    }

    /**
     * Parses the connection properties section and stores the extracted key/value pairs into a local map.
     */
    private void parseQuerySection() {
        if (isNullOrEmpty(this.query)) {
            this.parsedProperties = new HashMap<>();
            return;
        }
        this.parsedProperties = processKeyValuePattern(PROPERTIES_PTRN, this.query);
    }

    /**
     * Takes a two-matching-groups (respectively named "key" and "value") pattern which is successively tested against the given string and produces a key/value
     * map with the matched values. The given pattern must ensure that there are no leftovers between successive tests, i.e., the end of the previous match must
     * coincide with the beginning of the next.
     * 
     * @param pattern
     *            the regular expression pattern to match against to
     * @param input
     *            the input string
     * @return a key/value map containing the matched values
     */
    private Map<String, String> processKeyValuePattern(Pattern pattern, String input) {
        Matcher matcher = pattern.matcher(input);
        int p = 0;
        Map<String, String> kvMap = new HashMap<>();
        while (matcher.find()) {
            if (matcher.start() != p) {
                throw ExceptionFactory.createException(WrongArgumentException.class,
                        Messages.getString("ConnectionString.4", new Object[] { input.substring(p) }));
            }
            p = matcher.end();
            String key = decode(matcher.group("key")).trim();
            String value = decode(matcher.group("value"));
            kvMap.put(key, value);
        }
        if (p != input.length()) {
            throw ExceptionFactory.createException(WrongArgumentException.class, Messages.getString("ConnectionString.4", new Object[] { input.substring(p) }));
        }
        return kvMap;
    }

    /**
     * URL-decode the given string.
     * 
     * @param text
     *            the string to decode
     * @return
     *         the decoded string
     */
    private static String decode(String text) {
        if (isNullOrEmpty(text)) {
            return text;
        }
        try {
            return URLDecoder.decode(text, StandardCharsets.UTF_8.name());
        } catch (UnsupportedEncodingException e) {
            // Won't happen.
        }
        return "";
    }

    /**
     * Returns the original database URL that produced this connection string parser.
     * 
     * @return the original database URL
     */
    @Override
    public String getDatabaseUrl() {
        return this.baseConnectionString;
    }

    /**
     * Returns the scheme section.
     * 
     * @return the scheme section
     */
    public String getScheme() {
        return this.scheme;
    }

    /**
     * Returns the authority section.
     * 
     * @return the authority section
     */
    public String getAuthority() {
        return this.authority;
    }

    /**
     * Returns the path section.
     * 
     * @return the path section
     */
    public String getPath() {
        return this.path;
    }

    /**
     * Returns the query section.
     * 
     * @return the query section
     */
    public String getQuery() {
        return this.query;
    }

    /**
     * Returns the hosts information.
     * 
     * @return the hosts information
     */
    public List<HostInfo> getHosts() {
        if (this.parsedHosts == null) {
            this.parsedHosts = new ArrayList<>();
            parseAuthoritySection();
        }
        return this.parsedHosts;
    }

    /**
     * Returns the properties map contained in this connection string.
     * 
     * @return the properties map
     */
    public Map<String, String> getProperties() {
        if (this.parsedProperties == null) {
            parseQuerySection();
        }
        return Collections.unmodifiableMap(this.parsedProperties);
    }

    /**
     * Returns a string representation of this object.
     * 
     * @return a string representation of this object
     */
    @Override
    public String toString() {
        StringBuilder asStr = new StringBuilder(super.toString());
        asStr.append(String.format(" :: {scheme: \"%s\", authority: \"%s\", path: \"%s\", query: \"%s\", parsedHosts: %s, parsedProperties: %s}", this.scheme,
                this.authority, this.path, this.query, this.parsedHosts, this.parsedProperties));
        return asStr.toString();
    }

    /**
     * This class is a simple container for two elements.
     */
    public static class Pair<T, U> {
        public final T left;
        public final U right;

        public Pair(T left, U right) {
            this.left = left;
            this.right = right;
        }
    }
}
