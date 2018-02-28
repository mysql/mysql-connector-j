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
import static com.mysql.cj.util.StringUtils.safeTrim;

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

import com.mysql.cj.Messages;
import com.mysql.cj.exceptions.ExceptionFactory;
import com.mysql.cj.exceptions.WrongArgumentException;
import com.mysql.cj.util.StringUtils;

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
 * <dd>The connection properties, written as "propertyName1[=[propertyValue1]][&amp;propertyName2[=[propertyValue2]]]..."</dd>
 * <dt>fragment</dt>
 * <dd>The fragment section is ignored in Connector/J connection strings.</dd>
 * </dl>
 */
public class ConnectionUrlParser implements DatabaseUrlContainer {
    private static final String DUMMY_SCHEMA = "cj://";
    private static final String USER_PASS_SEPARATOR = ":";
    private static final String USER_HOST_SEPARATOR = "@";
    private static final String HOSTS_SEPARATOR = ",";
    private static final String KEY_VALUE_HOST_INFO_OPENING_MARKER = "(";
    private static final String KEY_VALUE_HOST_INFO_CLOSING_MARKER = ")";
    private static final String HOSTS_LIST_OPENING_MARKERS = "[(";
    private static final String HOSTS_LIST_CLOSING_MARKERS = "])";
    private static final String ADDRESS_EQUALS_HOST_INFO_PREFIX = "ADDRESS=";

    private static final Pattern CONNECTION_STRING_PTRN = Pattern.compile("(?<scheme>[\\w:%]+)\\s*" // scheme: required; alphanumeric, colon or percent
            + "(?://(?<authority>[^/?#]*))?\\s*" // authority: optional; starts with "//" followed by any char except "/", "?" and "#"
            + "(?:/(?!\\s*/)(?<path>[^?#]*))?" // path: optional; starts with "/" but not followed by "/", and then followed by by any char except "?" and "#"
            + "(?:\\?(?!\\s*\\?)(?<query>[^#]*))?" // query: optional; starts with "?" but not followed by "?", and then followed by by any char except "#"
            + "(?:\\s*#(?<fragment>.*))?"); // fragment: optional; starts with "#", and then followed by anything
    private static final Pattern HOST_LIST_PTRN = Pattern.compile("^\\[(?<hosts>.*)\\]$");
    private static final Pattern GENERIC_HOST_PTRN = Pattern.compile("^(?<host>.*?)(?::(?<port>[^:]*))?$");
    private static final Pattern KEY_VALUE_HOST_PTRN = Pattern.compile("[,\\s]*(?<key>[\\w\\.\\-\\s%]*)(?:=(?<value>[^,=]*))?");
    private static final Pattern ADDRESS_EQUALS_HOST_PTRN = Pattern.compile("\\s*\\(\\s*(?<key>[\\w\\.\\-%]+)?\\s*(?:=(?<value>[^)]*))?\\)\\s*");
    private static final Pattern PROPERTIES_PTRN = Pattern.compile("[&\\s]*(?<key>[\\w\\.\\-\\s%]*)(?:=(?<value>[^&=]*))?");

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
        this.path = matcher.group("path") == null ? null : decode(matcher.group("path")).trim();
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

        List<String> authoritySegments = StringUtils.split(this.authority, HOSTS_SEPARATOR, HOSTS_LIST_OPENING_MARKERS, HOSTS_LIST_CLOSING_MARKERS, true,
                StringUtils.SEARCH_MODE__MRK_WS);
        for (String hi : authoritySegments) {
            parseAuthoritySegment(hi);
        }
    }

    /**
     * Parses the given sub authority segment, which can take one of the following syntaxes:
     * <ul>
     * <li>_user_:_password_@_host_:_port_
     * <li>_user_:_password_@(key1=value1,key2=value2,...)
     * <li>_user_:_password_@address=(key1=value1)(key2=value2)...
     * <li>_user_:_password_@[_any_of_the_above_1_,_any_of_the_above_2_,...]
     * </ul>
     * Most of the above placeholders can be omitted, representing a null, empty, or default value.
     * The placeholder _host_, can be a host name, IPv4 or IPv6. This parser doesn't check IP syntax. IPv6 addresses are enclosed by square brackets ([::1]).
     * The placeholder _any_of_the_above_?_ can be any of the above except for the user information part (_user_:_password_@).
     * When the symbol ":" is not used, it means an null/empty password or a default (-1) port, respectively.
     * When the symbol "@" is not used, it means that the authority part doesn't contain user information (depending on the scheme type can still be provided
     * via key=value pairs).
     * 
     * @param authSegment
     *            the string containing the authority segment
     */
    private void parseAuthoritySegment(String authSegment) {
        /*
         * Start by splitting the user and host information parts from the authority segment and process the user information, if any.
         */
        Pair<String, String> userHostInfoSplit = splitByUserInfoAndHostInfo(authSegment);
        String userInfo = safeTrim(userHostInfoSplit.left);
        String user = null;
        String password = null;
        if (!isNullOrEmpty(userInfo)) {
            Pair<String, String> userInfoPair = parseUserInfo(userInfo);
            user = decode(safeTrim(userInfoPair.left));
            password = decode(safeTrim(userInfoPair.right));
        }
        String hostInfo = safeTrim(userHostInfoSplit.right);

        /*
         * Handle an authority part without host information.
         */
        HostInfo hi = buildHostInfoForEmptyHost(user, password, hostInfo);
        if (hi != null) {
            this.parsedHosts.add(hi);
            return;
        }

        /*
         * Try using a java.net.URI instance to parse the host information. This helps dealing with the IPv6 syntax.
         */
        hi = buildHostInfoResortingToUriParser(user, password, authSegment);
        if (hi != null) {
            this.parsedHosts.add(hi);
            return;
        }

        /*
         * Using a URI didn't work, now check if the host part is composed by a sub list of hosts and process them, one by one if so.
         */
        List<HostInfo> hiList = buildHostInfoResortingToSubHostsListParser(user, password, hostInfo);
        if (hiList != null) {
            this.parsedHosts.addAll(hiList);
            return;
        }

        /*
         * The hosts list syntax didn't work, now check if the host information is written in the alternate syntax "(Key1=value1,key2=value2)".
         */
        hi = buildHostInfoResortingToKeyValueSyntaxParser(user, password, hostInfo);
        if (hi != null) {
            this.parsedHosts.add(hi);
            return;
        }

        /*
         * Key/value syntax didn't work either, now check if the host information is written in the alternate syntax "address=(...)".
         * This parser needs to run after the key/value one because a key named "address" could invalidate it.
         */
        hi = buildHostInfoResortingToAddressEqualsSyntaxParser(user, password, hostInfo);
        if (hi != null) {
            this.parsedHosts.add(hi);
            return;
        }

        /*
         * Alternate syntax also failed, let's wind up the corner cases the URI couldn't handle.
         */
        hi = buildHostInfoResortingToGenericSyntaxParser(user, password, hostInfo);
        if (hi != null) {
            this.parsedHosts.add(hi);
            return;
        }

        /*
         * Failed parsing the authority segment.
         */
        throw ExceptionFactory.createException(WrongArgumentException.class, Messages.getString("ConnectionString.2", new Object[] { authSegment }));
    }

    /**
     * Builds an {@link HostInfo} instance for empty host authority segments.
     * 
     * @param user
     *            the user to include in the final {@link HostInfo}
     * @param password
     *            the password to include in the final {@link HostInfo}
     * @param hostInfo
     *            the string containing the host information part
     * @return the {@link HostInfo} instance containing the parsed information or <code>null</code> if the host part is not empty
     */
    private HostInfo buildHostInfoForEmptyHost(String user, String password, String hostInfo) {
        if (isNullOrEmpty(hostInfo)) {
            if (isNullOrEmpty(user) && isNullOrEmpty(password)) {
                return new HostInfo();
            }
            return new HostInfo(this, null, -1, user, password);
        }
        return null;
    }

    /**
     * Parses the host information resorting to a URI object. This process handles most single-host well formed addresses.
     * 
     * @param user
     *            the user to include in the final {@link HostInfo}
     * @param password
     *            the password to include in the final {@link HostInfo}
     * @param hostInfo
     *            the string containing the host information part
     * 
     * @return the {@link HostInfo} instance containing the parsed information or <code>null</code> if unable to parse the host information
     */
    private HostInfo buildHostInfoResortingToUriParser(String user, String password, String hostInfo) {
        String host = null;
        int port = -1;

        try {
            URI uri = URI.create(DUMMY_SCHEMA + hostInfo);
            if (uri.getHost() != null) {
                host = decode(uri.getHost());
            }
            if (uri.getPort() != -1) {
                port = uri.getPort();
            }
            if (uri.getUserInfo() != null) {
                // Can't have another one. The user information should have been handled already.
                return null;
            }
        } catch (IllegalArgumentException e) {
            // The URI failed to parse the host information.
            return null;
        }
        if (host != null || port != -1) {
            // The host info parsing succeeded.
            return new HostInfo(this, host, port, user, password);
        }
        return null;
    }

    /**
     * Parses the host information using the alternate sub hosts lists syntax "[host1, host2, ...]".
     * 
     * @param user
     *            the user to include in all the resulting {@link HostInfo}
     * @param password
     *            the password to include in all the resulting {@link HostInfo}
     * @param hostInfo
     *            the string containing the host information part
     * @return a list with all {@link HostInfo} instances containing the parsed information or <code>null</code> if unable to parse the host information
     */
    private List<HostInfo> buildHostInfoResortingToSubHostsListParser(String user, String password, String hostInfo) {
        Matcher matcher = HOST_LIST_PTRN.matcher(hostInfo);
        if (matcher.matches()) {
            String hosts = matcher.group("hosts");
            List<String> hostsList = StringUtils.split(hosts, HOSTS_SEPARATOR, HOSTS_LIST_OPENING_MARKERS, HOSTS_LIST_CLOSING_MARKERS, true,
                    StringUtils.SEARCH_MODE__MRK_WS);
            // One single element could, in fact, be an IPv6 stripped from its delimiters.
            boolean maybeIPv6 = hostsList.size() == 1 && hostsList.get(0).matches("(?i)^[\\dabcdef:]+$");
            List<HostInfo> hostInfoList = new ArrayList<>();
            for (String h : hostsList) {
                HostInfo hi;
                if ((hi = buildHostInfoForEmptyHost(user, password, h)) != null) {
                    hostInfoList.add(hi);
                } else if ((hi = buildHostInfoResortingToUriParser(user, password, h)) != null
                        || (maybeIPv6 && (hi = buildHostInfoResortingToUriParser(user, password, "[" + h + "]")) != null)) {
                    hostInfoList.add(hi);
                } else if ((hi = buildHostInfoResortingToKeyValueSyntaxParser(user, password, h)) != null) {
                    hostInfoList.add(hi);
                } else if ((hi = buildHostInfoResortingToAddressEqualsSyntaxParser(user, password, h)) != null) {
                    hostInfoList.add(hi);
                } else if ((hi = buildHostInfoResortingToGenericSyntaxParser(user, password, h)) != null) {
                    hostInfoList.add(hi);
                } else {
                    return null;
                }
            }
            return hostInfoList;
        }
        return null;
    }

    /**
     * Parses the host information using the alternate syntax "(key1=value1, key2=value2, ...)".
     * 
     * @param user
     *            the user to include in the resulting {@link HostInfo}
     * @param password
     *            the password to include in the resulting {@link HostInfo}
     * @param hostInfo
     *            the string containing the host information part
     * @return the {@link HostInfo} instance containing the parsed information or <code>null</code> if unable to parse the host information
     */
    private HostInfo buildHostInfoResortingToKeyValueSyntaxParser(String user, String password, String hostInfo) {
        if (!hostInfo.startsWith(KEY_VALUE_HOST_INFO_OPENING_MARKER) || !hostInfo.endsWith(KEY_VALUE_HOST_INFO_CLOSING_MARKER)) {
            // This pattern won't work.
            return null;
        }
        hostInfo = hostInfo.substring(KEY_VALUE_HOST_INFO_OPENING_MARKER.length(), hostInfo.length() - KEY_VALUE_HOST_INFO_CLOSING_MARKER.length());
        return new HostInfo(this, null, -1, user, password, processKeyValuePattern(KEY_VALUE_HOST_PTRN, hostInfo));
    }

    /**
     * Parses the host information using the alternate syntax "address=(key1=value1)(key2=value2)...".
     * 
     * @param user
     *            the user to include in the resulting {@link HostInfo}
     * @param password
     *            the password to include in the resulting {@link HostInfo}
     * @param hostInfo
     *            the string containing the host information part
     * @return the {@link HostInfo} instance containing the parsed information or <code>null</code> if unable to parse the host information
     */
    private HostInfo buildHostInfoResortingToAddressEqualsSyntaxParser(String user, String password, String hostInfo) {
        int p = StringUtils.indexOfIgnoreCase(hostInfo, ADDRESS_EQUALS_HOST_INFO_PREFIX);
        if (p != 0) {
            // This pattern won't work.
            return null;
        }
        hostInfo = hostInfo.substring(p + ADDRESS_EQUALS_HOST_INFO_PREFIX.length()).trim();
        return new HostInfo(this, null, -1, user, password, processKeyValuePattern(ADDRESS_EQUALS_HOST_PTRN, hostInfo));
    }

    /**
     * Parses the host information using the generic syntax "host:port".
     * 
     * @param user
     *            the user to include in the resulting {@link HostInfo}
     * @param password
     *            the password to include in the resulting {@link HostInfo}
     * @param hostInfo
     *            the string containing the host information part
     * @return the {@link HostInfo} instance containing the parsed information or <code>null</code> if unable to parse the host information
     */
    private HostInfo buildHostInfoResortingToGenericSyntaxParser(String user, String password, String hostInfo) {
        if (splitByUserInfoAndHostInfo(hostInfo).left != null) {
            // This host information is invalid if contains another user information part.
            return null;
        }
        Pair<String, Integer> hostPortPair = parseHostPortPair(hostInfo);
        String host = decode(safeTrim(hostPortPair.left));
        Integer port = hostPortPair.right;
        return new HostInfo(this, isNullOrEmpty(host) ? null : host, port, user, password);
    }

    /**
     * Splits the given authority segment in the user information part and the host part.
     * 
     * @param authSegment
     *            the string containing the authority segment, i.e., the user and host information parts
     * @return
     *         a {@link Pair} containing the user information in the left side and the host information in the right
     */
    private Pair<String, String> splitByUserInfoAndHostInfo(String authSegment) {
        String userInfoPart = null;
        String hostInfoPart = authSegment;
        int p = authSegment.indexOf(USER_HOST_SEPARATOR);
        if (p >= 0) {
            userInfoPart = authSegment.substring(0, p);
            hostInfoPart = authSegment.substring(p + USER_HOST_SEPARATOR.length());
        }
        return new Pair<>(userInfoPart, hostInfoPart);
    }

    /**
     * Parses the given user information which is formed by the parts [user][:password].
     * 
     * @param userInfo
     *            the string containing the user information
     * @return a {@link Pair} containing the user and password information or null if the user information can't be parsed
     */
    public static Pair<String, String> parseUserInfo(String userInfo) {
        if (isNullOrEmpty(userInfo)) {
            return null;
        }
        String[] userInfoParts = userInfo.split(USER_PASS_SEPARATOR, 2);
        String userName = userInfoParts[0];
        String password = userInfoParts.length > 1 ? userInfoParts[1] : null;
        return new Pair<>(userName, password);
    }

    /**
     * Parses a host:port pair and returns the two elements in a {@link Pair}
     * 
     * @param hostInfo
     *            the host:pair to parse
     * @return a {@link Pair} containing the host and port information or null if the host information can't be parsed
     */
    public static Pair<String, Integer> parseHostPortPair(String hostInfo) {
        if (isNullOrEmpty(hostInfo)) {
            return null;
        }
        Matcher matcher = GENERIC_HOST_PTRN.matcher(hostInfo);
        if (matcher.matches()) {
            String host = matcher.group("host");
            String portAsString = decode(safeTrim(matcher.group("port")));
            Integer portAsInteger = -1;
            if (!isNullOrEmpty(portAsString)) {
                try {
                    portAsInteger = Integer.parseInt(portAsString);
                } catch (NumberFormatException e) {
                    throw ExceptionFactory.createException(WrongArgumentException.class, Messages.getString("ConnectionString.3", new Object[] { hostInfo }),
                            e);
                }
            }
            return new Pair<>(host, portAsInteger);
        }
        throw ExceptionFactory.createException(WrongArgumentException.class, Messages.getString("ConnectionString.3", new Object[] { hostInfo }));
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
            String key = decode(safeTrim(matcher.group("key")));
            String value = decode(safeTrim(matcher.group("value")));
            if (!isNullOrEmpty(key)) {
                kvMap.put(key, value);
            } else if (!isNullOrEmpty(value)) {
                throw ExceptionFactory.createException(WrongArgumentException.class,
                        Messages.getString("ConnectionString.4", new Object[] { input.substring(p) }));
            }
            p = matcher.end();
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

        @Override
        public String toString() {
            StringBuilder asStr = new StringBuilder(super.toString());
            asStr.append(String.format(" :: { left: %s, right: %s }", this.left, this.right));
            return asStr.toString();
        }
    }
}
