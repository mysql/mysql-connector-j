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

package com.mysql.cj;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.Properties;

import org.junit.Ignore;
import org.junit.Test;

import com.mysql.cj.conf.ConnectionPropertiesTransform;
import com.mysql.cj.conf.ConnectionUrl;
import com.mysql.cj.conf.ConnectionUrlParser;
import com.mysql.cj.conf.HostInfo;
import com.mysql.cj.conf.PropertyDefinitions;
import com.mysql.cj.conf.PropertyDefinitions.PropertyKey;
import com.mysql.cj.exceptions.WrongArgumentException;

public class ConnectionUrlTest {
    /**
     * Internal class for generating hundreds of thousands of connection strings.
     */
    private static class ConnectionStringGenerator implements Iterator<String>, Iterable<String> {
        enum UrlMode {
            SINGLE_HOST(1), OUTER_HOSTS_LIST(2), INNER_HOSTS_LIST(2);

            private int hostsCount;

            UrlMode(int hostsCount) {
                this.hostsCount = hostsCount;
            }

            int getHostsCount() {
                return this.hostsCount;
            }
        }

        private static final String[] PROTOCOL = new String[] { "jdbc:mysql:", "mysqlx:" };
        private static final String[] USER = new String[] { "", "@", "johndoe@", "johndoe:@", "johndoe:secret@", ":secret@", ":@" };
        private static final String[] STD_HOST = new String[] { "", "myhost", "192.168.0.1", "[1000:abcd::1]" };
        private static final String[] STD_PORT = new String[] { "", ":", ":1234" };
        private static final String[] KEY_VALUE_HOST = new String[] { "", "()", "(host=[::1],port=1234,prio=1)",
                "(protocol=tcp,host=myhost,port=1234,key=value%28%29)", "(address=myhost:1234,prio=2)" };
        private static final String[] ADDRESS_EQUALS_HOST = new String[] { "address=", "address=()", "address=(flag)",
                "address=(protocol=tcp)(host=myhost)(port=1234)", "address=(protocol=tcp)(host=myhost)(port=1234)(key=value%28%29)" };
        private static final String[] HOST; // Initialized below.
        private static final String[] DB = new String[] { "", "/", "/mysql" };
        private static final String[] PARAMS = new String[] { "", "?", "?key=value&flag", "?key=value%26&flag&26", "?file=%2Fpath%2Fto%2Ffile&flag&key=value",
                "?file=(/path/to/file)&flag&key=value" };

        static {
            int i = 0;
            String[] hosts = new String[STD_HOST.length * STD_PORT.length + KEY_VALUE_HOST.length + ADDRESS_EQUALS_HOST.length];
            for (String h : STD_HOST) {
                for (String p : STD_PORT) {
                    hosts[i++] = h + p;
                }
            }
            for (String h : KEY_VALUE_HOST) {
                hosts[i++] = h;
            }
            for (String h : ADDRESS_EQUALS_HOST) {
                hosts[i++] = h;
            }
            HOST = hosts;
        }

        UrlMode urlMode;
        private int numberOfHosts;
        private int[] current;
        private int[] next;
        private int[] ceiling;
        boolean hasNext = true;

        /**
         * Create an instance of {@link ConnectionStringGenerator} and initializes internal data for the iterator.
         * 
         * @param numberOfHosts
         */
        public ConnectionStringGenerator(UrlMode urlMode) {
            this.urlMode = urlMode;
            this.numberOfHosts = this.urlMode.getHostsCount();

            int counterLen = 0;
            switch (this.urlMode) {
                case SINGLE_HOST:
                    counterLen = 5; // protocol + user + host + db + params
                    break;
                case OUTER_HOSTS_LIST:
                    counterLen = 3 + 2 * this.numberOfHosts; // protocol + (user + host) * num_of_hosts + db + params
                    break;
                case INNER_HOSTS_LIST:
                    counterLen = 4 + this.numberOfHosts; // protocol + user + host * num_of_hosts + db + params
                    break;
            }
            this.current = new int[counterLen];
            this.next = new int[counterLen];
            this.ceiling = new int[counterLen];

            int counterIndex = 0;
            this.ceiling[counterIndex++] = PROTOCOL.length;
            switch (this.urlMode) {
                case SINGLE_HOST:
                    this.ceiling[counterIndex++] = USER.length;
                    this.ceiling[counterIndex++] = HOST.length;
                    break;
                case OUTER_HOSTS_LIST:
                    for (int i = 0; i < this.numberOfHosts; i++) {
                        this.ceiling[counterIndex++] = USER.length;
                        this.ceiling[counterIndex++] = HOST.length;
                    }
                    break;
                case INNER_HOSTS_LIST:
                    this.ceiling[counterIndex++] = USER.length;
                    for (int i = 0; i < this.numberOfHosts; i++) {
                        this.ceiling[counterIndex++] = HOST.length;
                    }
                    break;
            }
            this.ceiling[counterIndex++] = DB.length;
            this.ceiling[counterIndex++] = PARAMS.length;
        }

        /**
         * Increments the counter recursively for each connection string part.
         * 
         * @param i
         *            the part where to increment the counter
         * @return false if the counter reaches the end, true otherwise
         */
        private boolean incrementCounter(int i) {
            if (i >= this.next.length) {
                return false;
            }
            this.current[i] = this.next[i];
            this.next[i] = (this.next[i] + 1) % this.ceiling[i];
            if (this.next[i] == 0) {
                return incrementCounter(i + 1);
            }
            return true;
        }

        /**
         * Builds a connection string with the parts corresponding to the current counter position.
         * 
         * @return the connection string built from the current counter position
         */
        private String buildConnectionString() {
            StringBuilder sb = new StringBuilder();
            int counterIndex = 0;
            sb.append(PROTOCOL[this.current[counterIndex++]]).append("//");
            if (this.urlMode == UrlMode.SINGLE_HOST || this.urlMode == UrlMode.OUTER_HOSTS_LIST) {
                for (int i = 0; i < this.numberOfHosts; i++) {
                    if (i != 0) {
                        sb.append(",");
                    }
                    sb.append(USER[this.current[counterIndex++]]);
                    sb.append(HOST[this.current[counterIndex++]]);
                }
            } else if (this.urlMode == UrlMode.INNER_HOSTS_LIST) {
                sb.append(USER[this.current[counterIndex++]]).append("[");
                for (int i = 0; i < this.numberOfHosts; i++) {
                    if (i != 0) {
                        sb.append(",");
                    }
                    sb.append(HOST[this.current[counterIndex++]]);
                }
                sb.append("]");

            }
            sb.append(DB[this.current[counterIndex++]]);
            sb.append(PARAMS[this.current[counterIndex++]]);
            return sb.toString();
        }

        @Override
        public boolean hasNext() {
            return this.hasNext;
        }

        @Override
        public String next() {
            if (!this.hasNext) {
                throw new NoSuchElementException();
            }
            this.hasNext = incrementCounter(0);
            return buildConnectionString();
        }

        /**
         * Returns the protocol part (scheme) for the current position.
         * 
         * @return the protocol part
         */
        public String getProtocol() {
            int counterIndex = 0; // protocol
            return PROTOCOL[this.current[counterIndex]];
        }

        /**
         * Returns the user info part for the current position and the given host.
         * 
         * @param fromHostIndex
         *            the host from where to get user info
         * @return the user info part
         */
        public String getUserInfo(int fromHostIndex) {
            if (fromHostIndex < 0 || fromHostIndex >= this.numberOfHosts) {
                throw new IllegalArgumentException("Argument \"fromHostIndex\" out of bounds.");
            }

            int counterIndex = 1; // user (single host or inner hosts list)
            if (this.urlMode == UrlMode.OUTER_HOSTS_LIST) {
                counterIndex += fromHostIndex * 2; // increments of two per additional host
            }
            fromHostIndex--;
            return USER[this.current[counterIndex]];
        }

        /**
         * Returns the host info part for the current position and the given host.
         * 
         * @param fromHostIndex
         *            the host from where to get host info
         * @return the host info part
         */
        public String getHostInfo(int fromHostIndex) {
            if (fromHostIndex < 0 || fromHostIndex >= this.numberOfHosts) {
                throw new IllegalArgumentException("Argument \"fromHostIndex\" out of bounds.");
            }

            int counterIndex = 2; // host (single host)
            if (this.urlMode == UrlMode.INNER_HOSTS_LIST) {
                counterIndex += fromHostIndex; // increments of one per additional host
            } else if (this.urlMode == UrlMode.OUTER_HOSTS_LIST) {
                counterIndex += fromHostIndex * 2; // increments of two per additional host
            }
            fromHostIndex--;
            return HOST[this.current[counterIndex]];
        }

        /**
         * Returns the database part for the current position.
         * 
         * @return the database part
         */
        public String getDatabase() {
            int counterIndex = 3; // db (single host)
            if (this.urlMode == UrlMode.INNER_HOSTS_LIST) {
                counterIndex += this.numberOfHosts - 1; // increments of one per additional host
            } else if (this.urlMode == UrlMode.OUTER_HOSTS_LIST) {
                counterIndex += (this.numberOfHosts - 1) * 2; // increments of two per additional host
            }
            return DB[this.current[counterIndex]];
        }

        /**
         * Returns the connection parameters part for the current position.
         * 
         * @return the connection parameter part
         */
        public String getParams() {
            int counterIndex = 4; // params (single host)
            if (this.urlMode == UrlMode.INNER_HOSTS_LIST) {
                counterIndex += this.numberOfHosts - 1; // increments of one per additional host
            } else if (this.urlMode == UrlMode.OUTER_HOSTS_LIST) {
                counterIndex += (this.numberOfHosts - 1) * 2; // increments of two per additional host
            }
            return PARAMS[this.current[counterIndex]];
        }

        /**
         * Checks if current host info contains the given key & value parameter.
         * 
         * @param hostIndex
         *            the host from where the given information will be checked against
         * @param key
         *            the key to check
         * @param value
         *            the value to check
         * @return true if the key/value pair exists, false otherwise
         */
        public boolean hasHostParam(int hostIndex, String key, String value) {
            StringBuilder sbKv = new StringBuilder(key);
            if (value != null) {
                sbKv.append("=").append(value);
            }
            return getHostInfo(hostIndex).contains(sbKv.toString()) || decode(getHostInfo(hostIndex)).contains(sbKv.toString());
        }

        /**
         * Returns the number of host specific parameters existing in the current position and the given host.
         * 
         * @param hostIndex
         *            the host from where to get the count
         * @return the number of host specific parameters
         */
        public int getHostParamsCount(int hostIndex) {
            String hi = getHostInfo(hostIndex);
            if (hi.startsWith("(") && hi.lastIndexOf(")") != 1) {
                return hi.length() - hi.replace(",", "").length() + 1;
            } else if (hi.startsWith("address=") && hi.length() > 10) { // len("address=()") == 10.
                return hi.length() - hi.replace(")(", "|").length() + 1;
            }
            return 0;
        }

        /**
         * Checks if the current connection properties contain the given key & value.
         * 
         * @param key
         *            the key to check
         * @param value
         *            the value to check
         * @return true if the key/value pair exists, false otherwise
         */
        public boolean hasParam(String key, String value) {
            StringBuilder sbKv = new StringBuilder(key);
            if (value != null) {
                sbKv.append("=").append(value);
            }
            return getParams().indexOf(sbKv.toString()) != -1 || decode(getParams()).indexOf(sbKv.toString()) != -1;
        }

        /**
         * Returns the number of connection parameters existing the the current position.
         * 
         * @return the number of connection parameters
         */
        public int getParamsCount() {
            String params = getParams();
            if (params.startsWith("?")) {
                params = params.substring(1);
            }
            return params.isEmpty() ? 0 : params.split("&").length;
        }

        /**
         * Utility method to URL decode the given string.
         * 
         * @param text
         *            the text to decode
         * @return the decoded text
         */
        private String decode(String text) {
            if (text == null || text.isEmpty()) {
                return text;
            }
            try {
                return URLDecoder.decode(text, StandardCharsets.UTF_8.name());
            } catch (UnsupportedEncodingException e) {
                // Won't happen.
            }
            return "";
        }

        @Override
        public Iterator<String> iterator() {
            return this;
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder("{counter: ");
            sb.append(Arrays.toString(this.next));
            sb.append(", connectionString: \"").append(buildConnectionString()).append("\"}");
            return sb.toString();
        }
    }

    /**
     * Checks if the values returned from {@link ConnectionUrl.Type#fromValue(String, int)} are correct.
     */
    @Test
    public void testTypeEnumCorrectValues() {
        assertEquals(ConnectionUrl.Type.SINGLE_CONNECTION, ConnectionUrl.Type.fromValue("jdbc:mysql:", 1));
        assertEquals(ConnectionUrl.Type.FAILOVER_CONNECTION, ConnectionUrl.Type.fromValue("jdbc:mysql:", 2));
        assertEquals(ConnectionUrl.Type.FAILOVER_CONNECTION, ConnectionUrl.Type.fromValue("jdbc:mysql:", 3));
        assertEquals(ConnectionUrl.Type.LOADBALANCE_CONNECTION, ConnectionUrl.Type.fromValue("jdbc:mysql:loadbalance:", 1));
        assertEquals(ConnectionUrl.Type.LOADBALANCE_CONNECTION, ConnectionUrl.Type.fromValue("jdbc:mysql:loadbalance:", 2));
        assertEquals(ConnectionUrl.Type.REPLICATION_CONNECTION, ConnectionUrl.Type.fromValue("jdbc:mysql:replication:", 1));
        assertEquals(ConnectionUrl.Type.REPLICATION_CONNECTION, ConnectionUrl.Type.fromValue("jdbc:mysql:replication:", 2));
        assertEquals(ConnectionUrl.Type.XDEVAPI_SESSION, ConnectionUrl.Type.fromValue("mysqlx:", 1));
    }

    /**
     * Checks the expected exception from an incorrect usage of {@link ConnectionUrl.Type#fromValue(String, int)}.
     */
    @Ignore // No longer applies.
    @Test(expected = WrongArgumentException.class)
    public void testTypeEnumWrongMysqlxValue() {
        ConnectionUrl.Type.fromValue("mysqlx:", 2);
    }

    /**
     * Tests the {@link ConnectionUrlParser} with close to one million of different connection string variations.
     */
    @Test
    public void testConnectionStringParser() {
        for (ConnectionStringGenerator.UrlMode urlMode : ConnectionStringGenerator.UrlMode.values()) {
            ConnectionStringGenerator csg = new ConnectionStringGenerator(urlMode);
            for (String cs : csg) {
                ConnectionUrlParser cup = ConnectionUrlParser.parseConnectionString(cs);
                String expected;
                String actual;
                // Protocol:
                assertEquals(cs, csg.getProtocol(), cup.getScheme());
                // User & Host:
                assertEquals(cs, urlMode.getHostsCount(), cup.getHosts().size());
                for (int hostIndex = 0; hostIndex < urlMode.getHostsCount(); hostIndex++) {
                    HostInfo hi = cup.getHosts().get(hostIndex);
                    // User(n):
                    expected = testCSParserTrimTail(testCSParserTrimHead(csg.getUserInfo(hostIndex), ":"), "@", ":");
                    actual = new StringBuilder(hi.getUser() == null ? "" : hi.getUser()).append(":").append(hi.getPassword() == null ? "" : hi.getPassword())
                            .toString();
                    actual = testCSParserTrimTail(testCSParserTrimHead(actual, ":"), ":");
                    assertEquals(cs, expected, actual);
                    if (csg.getHostInfo(hostIndex).startsWith("address=") || csg.getHostInfo(hostIndex).startsWith("(")) {
                        // Host props(n):
                        assertEquals(cs, csg.getHostParamsCount(hostIndex), hi.getHostProperties().size());
                        for (Entry<String, String> kv : hi.getHostProperties().entrySet()) {
                            assertTrue(cs, csg.hasHostParam(hostIndex, kv.getKey(), kv.getValue()));
                        }
                    } else {
                        // Host(n)
                        expected = testCSParserTrimTail(testCSParserTrimHead(csg.getHostInfo(hostIndex), ":"), ":");
                        actual = new StringBuilder(hi.getHost() == null ? "" : hi.getHost()).append(":").append(hi.getPort() == -1 ? "" : hi.getPort())
                                .toString();
                        actual = testCSParserTrimTail(testCSParserTrimHead(actual, ":"), ":");
                        assertEquals(cs, expected, actual);
                    }
                }
                // Database:
                expected = testCSParserTrimHead(csg.getDatabase(), "/");
                actual = cup.getPath() == null ? "" : testCSParserTrimHead(cup.getPath(), "/");
                assertEquals(cs, expected, actual);
                // Connection arguments:
                assertEquals(cs, csg.getParamsCount(), cup.getProperties().size());
                for (Entry<String, String> kv : cup.getProperties().entrySet()) {
                    assertTrue(cs, csg.hasParam(kv.getKey(), kv.getValue()));
                }
            }
        }
    }

    private String testCSParserTrimHead(String text, String... charsFromHead) {
        for (String c : charsFromHead) {
            if (text.startsWith(c)) {
                text = text.substring(c.length());
            }
        }
        return text;
    }

    private String testCSParserTrimTail(String text, String... charsFromTail) {
        for (String c : charsFromTail) {
            if (text.endsWith(c)) {
                text = text.substring(0, text.length() - c.length());
            }
        }
        return text;
    }

    /**
     * Tests the {@link ConnectionUrl#acceptsUrl(String)} method for supported and non-supported protocols.
     */
    @Test
    public void testConnectionStringAcceptsUrl() {
        // Supported URLs:
        assertTrue(ConnectionUrl.acceptsUrl("jdbc:mysql:"));
        assertTrue(ConnectionUrl.acceptsUrl("jdbc:mysql://somehost:1234/db?key=value"));
        assertTrue(ConnectionUrl.acceptsUrl("jdbc:mysql://127.0.0.1:1234/db?key=value"));
        assertTrue(ConnectionUrl.acceptsUrl("jdbc:mysql:loadbalance:"));
        assertTrue(ConnectionUrl.acceptsUrl("jdbc:mysql:loadbalance://somehost:1234/db?key=value"));
        assertTrue(ConnectionUrl.acceptsUrl("jdbc:mysql:replication:"));
        assertTrue(ConnectionUrl.acceptsUrl("jdbc:mysql:replication://somehost:1234/db?key=value"));
        assertTrue(ConnectionUrl.acceptsUrl("jdbc:mysql:"));
        assertTrue(ConnectionUrl.acceptsUrl("mysqlx://somehost:1234/db?key=value"));
        assertTrue(ConnectionUrl.acceptsUrl("mysqlx://127.0.0.1:1234/db?key=value"));
        assertTrue(ConnectionUrl.acceptsUrl("mysqlx://[::1]:1234/db?key=value"));
        assertTrue(ConnectionUrl.acceptsUrl("mysqlx://[fe80::250:56ff:fec0:8]:1234/db?key=value"));
        assertTrue(ConnectionUrl.acceptsUrl("mysqlx://johndoe:secret@[::1]:1234/db?key=value"));
        assertTrue(ConnectionUrl.acceptsUrl("mysqlx://johndoe:secret@[[::1]:1234]/db?key=value"));
        assertTrue(ConnectionUrl.acceptsUrl("mysqlx://johndoe:secret@[[::1]:1234,(address=[abcd:1000::f09a]:4321,priority=100)]/db?key=value"));

        // Non-supported URLs:
        assertFalse(ConnectionUrl.acceptsUrl(""));
        assertFalse(ConnectionUrl.acceptsUrl("//somehost:1234/db?key=value"));
        assertFalse(ConnectionUrl.acceptsUrl("mysql:"));
        assertFalse(ConnectionUrl.acceptsUrl("mysql://somehost:1234/db?key=value"));
        assertFalse(ConnectionUrl.acceptsUrl("mysql:jdbc:"));
        assertFalse(ConnectionUrl.acceptsUrl("mysql:jdbc://somehost:1234/db?key=value"));
        assertFalse(ConnectionUrl.acceptsUrl("loadbalance:"));
        assertFalse(ConnectionUrl.acceptsUrl("loadbalance://somehost:1234/db?key=value"));
        assertFalse(ConnectionUrl.acceptsUrl("replication:"));
        assertFalse(ConnectionUrl.acceptsUrl("replication://somehost:1234/db?key=value"));
        assertFalse(ConnectionUrl.acceptsUrl("jdbc:"));
        assertFalse(ConnectionUrl.acceptsUrl("jdbc://somehost:1234/db?key=value"));
        assertFalse(ConnectionUrl.acceptsUrl("jdbc: mysql://somehost:1234/db?key=value"));
        assertFalse(ConnectionUrl.acceptsUrl("jdbc:loadbalance:"));
        assertFalse(ConnectionUrl.acceptsUrl("jdbc:loadbalance://somehost:1234/db?key=value"));
        assertFalse(ConnectionUrl.acceptsUrl("jdbc:replication:"));
        assertFalse(ConnectionUrl.acceptsUrl("jdbc:replication://somehost:1234/db?key=value"));
        assertFalse(ConnectionUrl.acceptsUrl("jdbc:mysql:unknown:"));
        assertFalse(ConnectionUrl.acceptsUrl("jdbc:mysql:unknown://somehost:1234/db?key=value"));
        assertFalse(ConnectionUrl.acceptsUrl("mysql-x:"));
        assertFalse(ConnectionUrl.acceptsUrl("mysql-x://somehost:1234/db?key=value"));
    }

    /**
     * Tests the {@link ConnectionUrl} with close to one million of different connection string variations.
     */
    @Test
    public void testConnectionUrl() {
        Properties props = new Properties();
        props.setProperty("propKey", "propValue");

        for (ConnectionStringGenerator.UrlMode urlMode : ConnectionStringGenerator.UrlMode.values()) {
            ConnectionStringGenerator csg = new ConnectionStringGenerator(urlMode);
            for (String cs : csg) {
                try {
                    ConnectionUrl.getConnectionUrlInstance(cs, props);
                } catch (WrongArgumentException e) {
                    // X plugin connections ("mysqlx:") don't allow different credentials in different hosts and the generator doesn't account for that.
                    assertEquals(cs, ConnectionUrl.Type.XDEVAPI_SESSION.getScheme(), csg.getProtocol());
                    boolean first = true;
                    boolean ok = false;
                    String lastUi = "";
                    for (int hostIndex = 0; hostIndex < urlMode.getHostsCount() && !ok; hostIndex++) {
                        if (first) {
                            first = false;
                            lastUi = csg.getUserInfo(hostIndex);
                        } else if (!lastUi.equals(csg.getUserInfo(hostIndex))) {
                            ok = true;
                        }
                    }
                    if (!ok) {
                        fail(cs + ": unexpected " + e.getClass().getName() + " thrown with message: " + e.getMessage());
                    }
                }
            }
        }
    }

    /**
     * Tests the {@link ConnectionUrlParser} and {@link ConnectionUrl} with non standard, but accepted, connection strings.
     * 
     * @throws Exception
     */
    @Test
    public void testNonStandardConnectionUrl() throws Exception {
        String url;
        ConnectionUrlParser cup;

        /*
         * Single host syntax with spaces.
         */
        url = "jdbc:mysql: // johndoe : secret @ [abcd:9876::1:1234] : 1234 / db ? key1 = value1 & key2 = value2 # ignore";
        cup = ConnectionUrlParser.parseConnectionString(url);
        // schema & db
        assertEquals("jdbc:mysql:", cup.getScheme());
        assertEquals("db", cup.getPath());
        // single host: user, password, host name, port and host properties
        assertEquals(1, cup.getHosts().size());
        assertEquals("johndoe", cup.getHosts().get(0).getUser());
        assertEquals("secret", cup.getHosts().get(0).getPassword());
        assertEquals("[abcd:9876::1:1234]", cup.getHosts().get(0).getHost());
        assertEquals(1234, cup.getHosts().get(0).getPort());
        assertEquals(0, cup.getHosts().get(0).getHostProperties().size());
        // properties
        assertEquals(2, cup.getProperties().size());
        assertTrue(cup.getProperties().containsKey("key1"));
        assertEquals("value1", cup.getProperties().get("key1"));
        assertTrue(cup.getProperties().containsKey("key2"));
        assertEquals("value2", cup.getProperties().get("key2"));
        ConnectionUrl.getConnectionUrlInstance(url, null);

        /*
         * Single host address-equals syntax with spaces.
         */
        url = "jdbc:mysql: // johndoe : secret @ address= ( host = [abcd:9876::1:1234] ) ( port = 1234 ) / db ? key1 = value1 & key2 = value2 # ignore";
        cup = ConnectionUrlParser.parseConnectionString(url);
        // schema & db
        assertEquals("jdbc:mysql:", cup.getScheme());
        assertEquals("db", cup.getPath());
        // single host: user, password, host name, port and host properties
        assertEquals(1, cup.getHosts().size());
        assertEquals("johndoe", cup.getHosts().get(0).getUser());
        assertEquals("secret", cup.getHosts().get(0).getPassword());
        assertNull(cup.getHosts().get(0).getHost());
        assertEquals(-1, cup.getHosts().get(0).getPort());
        assertEquals(2, cup.getHosts().get(0).getHostProperties().size());
        assertTrue(cup.getHosts().get(0).getHostProperties().containsKey("host"));
        assertEquals("[abcd:9876::1:1234]", cup.getHosts().get(0).getHostProperties().get("host"));
        assertTrue(cup.getHosts().get(0).getHostProperties().containsKey("port"));
        assertEquals("1234", cup.getHosts().get(0).getHostProperties().get("port"));
        // properties
        assertEquals(2, cup.getProperties().size());
        assertTrue(cup.getProperties().containsKey("key1"));
        assertEquals("value1", cup.getProperties().get("key1"));
        assertTrue(cup.getProperties().containsKey("key2"));
        assertEquals("value2", cup.getProperties().get("key2"));
        ConnectionUrl.getConnectionUrlInstance(url, null);

        /*
         * Single host key/value syntax with spaces.
         */
        url = "jdbc:mysql: // johndoe : secret @ ( host = [abcd:9876::1:1234] , port = 1234 ) / db ? key1 = value1 & key2 = value2 # ignore";
        cup = ConnectionUrlParser.parseConnectionString(url);
        // schema & db
        assertEquals("jdbc:mysql:", cup.getScheme());
        assertEquals("db", cup.getPath());
        // single host: user, password, host name, port and host properties
        assertEquals(1, cup.getHosts().size());
        assertEquals("johndoe", cup.getHosts().get(0).getUser());
        assertEquals("secret", cup.getHosts().get(0).getPassword());
        assertNull(cup.getHosts().get(0).getHost());
        assertEquals(-1, cup.getHosts().get(0).getPort());
        assertEquals(2, cup.getHosts().get(0).getHostProperties().size());
        assertTrue(cup.getHosts().get(0).getHostProperties().containsKey("host"));
        assertEquals("[abcd:9876::1:1234]", cup.getHosts().get(0).getHostProperties().get("host"));
        assertTrue(cup.getHosts().get(0).getHostProperties().containsKey("port"));
        assertEquals("1234", cup.getHosts().get(0).getHostProperties().get("port"));
        // properties
        assertEquals(2, cup.getProperties().size());
        assertTrue(cup.getProperties().containsKey("key1"));
        assertEquals("value1", cup.getProperties().get("key1"));
        assertTrue(cup.getProperties().containsKey("key2"));
        assertEquals("value2", cup.getProperties().get("key2"));
        ConnectionUrl.getConnectionUrlInstance(url, null);

        /*
         * Hosts list syntax with spaces combining the above single host syntaxes.
         */
        url = "jdbc:mysql: // johndoe : secret @ [abcd:9876::1:1000] : 1111 , janedoe : secret @ address= ( host = [abcd:9876::1:2000] ) ( port = 2222 ) , "
                + "jabdoe : secret @ ( host = [abcd:9876::1:3000] , port = 3333 ) / db ? key1 = value1 & key2 = value2 # ignore";
        cup = ConnectionUrlParser.parseConnectionString(url);
        // schema & db
        assertEquals("jdbc:mysql:", cup.getScheme());
        assertEquals("db", cup.getPath());
        assertEquals(3, cup.getHosts().size());
        // first host: user, password, host name, port and host properties
        assertEquals("johndoe", cup.getHosts().get(0).getUser());
        assertEquals("secret", cup.getHosts().get(0).getPassword());
        assertEquals("[abcd:9876::1:1000]", cup.getHosts().get(0).getHost());
        assertEquals(1111, cup.getHosts().get(0).getPort());
        assertEquals(0, cup.getHosts().get(0).getHostProperties().size());
        // second host: user, password, host name, port and host properties
        assertEquals("janedoe", cup.getHosts().get(1).getUser());
        assertEquals("secret", cup.getHosts().get(1).getPassword());
        assertNull(cup.getHosts().get(1).getHost());
        assertEquals(-1, cup.getHosts().get(1).getPort());
        assertEquals(2, cup.getHosts().get(1).getHostProperties().size());
        assertTrue(cup.getHosts().get(1).getHostProperties().containsKey("host"));
        assertEquals("[abcd:9876::1:2000]", cup.getHosts().get(1).getHostProperties().get("host"));
        assertTrue(cup.getHosts().get(1).getHostProperties().containsKey("port"));
        assertEquals("2222", cup.getHosts().get(1).getHostProperties().get("port"));
        // third host: user, password, host name, port and host properties
        assertEquals("jabdoe", cup.getHosts().get(2).getUser());
        assertEquals("secret", cup.getHosts().get(2).getPassword());
        assertNull(cup.getHosts().get(2).getHost());
        assertEquals(-1, cup.getHosts().get(2).getPort());
        assertEquals(2, cup.getHosts().get(2).getHostProperties().size());
        assertTrue(cup.getHosts().get(2).getHostProperties().containsKey("host"));
        assertEquals("[abcd:9876::1:3000]", cup.getHosts().get(2).getHostProperties().get("host"));
        assertTrue(cup.getHosts().get(2).getHostProperties().containsKey("port"));
        assertEquals("3333", cup.getHosts().get(2).getHostProperties().get("port"));
        // properties
        assertEquals(2, cup.getProperties().size());
        assertTrue(cup.getProperties().containsKey("key1"));
        assertEquals("value1", cup.getProperties().get("key1"));
        assertTrue(cup.getProperties().containsKey("key2"));
        assertEquals("value2", cup.getProperties().get("key2"));
        ConnectionUrl.getConnectionUrlInstance(url, null);

        /*
         * Hosts sub list syntax with spaces combining the above single host syntaxes.
         */
        url = "jdbc:mysql: // johndoe : secret @ "
                + "[ [abcd:9876::1:1000] : 1111 , address= ( host = [abcd:9876::1:2000] ) ( port = 2222 ) , ( host = [abcd:9876::1:3000] , port = 3333 ) ] "
                + "/ db ? key1 = value1 & key2 = value2 # ignore";
        cup = ConnectionUrlParser.parseConnectionString(url);
        // schema & db
        assertEquals("jdbc:mysql:", cup.getScheme());
        assertEquals("db", cup.getPath());
        assertEquals(3, cup.getHosts().size());
        // first host: user, password, host name, port and host properties
        assertEquals("johndoe", cup.getHosts().get(0).getUser());
        assertEquals("secret", cup.getHosts().get(0).getPassword());
        assertEquals("[abcd:9876::1:1000]", cup.getHosts().get(0).getHost());
        assertEquals(1111, cup.getHosts().get(0).getPort());
        assertEquals(0, cup.getHosts().get(0).getHostProperties().size());
        // second host: user, password, host name, port and host properties
        assertEquals("johndoe", cup.getHosts().get(1).getUser());
        assertEquals("secret", cup.getHosts().get(1).getPassword());
        assertNull(cup.getHosts().get(1).getHost());
        assertEquals(-1, cup.getHosts().get(1).getPort());
        assertEquals(2, cup.getHosts().get(1).getHostProperties().size());
        assertTrue(cup.getHosts().get(1).getHostProperties().containsKey("host"));
        assertEquals("[abcd:9876::1:2000]", cup.getHosts().get(1).getHostProperties().get("host"));
        assertTrue(cup.getHosts().get(1).getHostProperties().containsKey("port"));
        assertEquals("2222", cup.getHosts().get(1).getHostProperties().get("port"));
        // third host: user, password, host name, port and host properties
        assertEquals("johndoe", cup.getHosts().get(2).getUser());
        assertEquals("secret", cup.getHosts().get(2).getPassword());
        assertNull(cup.getHosts().get(2).getHost());
        assertEquals(-1, cup.getHosts().get(2).getPort());
        assertEquals(2, cup.getHosts().get(2).getHostProperties().size());
        assertTrue(cup.getHosts().get(2).getHostProperties().containsKey("host"));
        assertEquals("[abcd:9876::1:3000]", cup.getHosts().get(2).getHostProperties().get("host"));
        assertTrue(cup.getHosts().get(2).getHostProperties().containsKey("port"));
        assertEquals("3333", cup.getHosts().get(2).getHostProperties().get("port"));
        // properties
        assertEquals(2, cup.getProperties().size());
        assertTrue(cup.getProperties().containsKey("key1"));
        assertEquals("value1", cup.getProperties().get("key1"));
        assertTrue(cup.getProperties().containsKey("key2"));
        assertEquals("value2", cup.getProperties().get("key2"));
        ConnectionUrl.getConnectionUrlInstance(url, null);
    }

    /**
     * Tests the {@link ConnectionUrl} with a few wrong connection strings.
     */
    @Test
    public void testConnectionUrlWithWrongConnectionString() {
        List<String> connStr = new ArrayList<>();
        connStr.add("jdbc:mysql://johndoe:secret@janedoe:secret@myhost:1234/db?key=value");
        connStr.add("jdbc:mysql://johndoe:secret@@myhost:1234/db?key=value");
        connStr.add("jdbc:mysql://johndoe:secret@myhost:abcd/db?key=value");
        connStr.add("jdbc:mysql://johndoe:secret@myhost:1234//db?key=value");
        connStr.add("jdbc:mysql://johndoe:secret@myhost:1234/db??key=value");
        connStr.add("jdbc:mysql://johndoe:secret@myhost:1234/db?key==value");
        connStr.add("jdbc:mysql://johndoe:secret@myhost:1234/db?key=value1=value2");
        connStr.add("jdbc:mysql://johndoe:secret@myhost:1234/db?=value");

        for (String cs : connStr) {
            try {
                System.out.println(ConnectionUrl.getConnectionUrlInstance(cs, null));
                fail(cs + ": expected to throw a " + WrongArgumentException.class.getName());
            } catch (Exception e) {
                assertTrue(cs + ": expected to throw a " + WrongArgumentException.class.getName(), WrongArgumentException.class.isAssignableFrom(e.getClass()));
            }
        }
    }

    /**
     * Tests the connection strings internal cache.
     */
    @Test
    public void testConnectionStringCache() {
        Properties props1 = new Properties();
        props1.setProperty("propKey", "propValue");
        Properties props2 = new Properties(props1);

        ConnectionUrl cu1 = ConnectionUrl.getConnectionUrlInstance("jdbc:mysql://localhost:3306/?param=value", null);
        ConnectionUrl cu2 = ConnectionUrl.getConnectionUrlInstance("jdbc:mysql://localhost:3306/?param=value", props1);
        ConnectionUrl cu3 = ConnectionUrl.getConnectionUrlInstance("jdbc:mysql://localhost:3306/?param=value", props1);
        ConnectionUrl cu4 = ConnectionUrl.getConnectionUrlInstance("jdbc:mysql://localhost:3306/?param=value", props2);
        ConnectionUrl cu5 = ConnectionUrl.getConnectionUrlInstance("jdbc:mysql://localhost:3306/?param=value&flag", props1);

        assertNotSame(cu1, cu2);
        assertSame(cu2, cu3);
        assertSame(cu3, cu4);
        assertNotSame(cu4, cu5);
        assertNotSame(cu5, cu1);
    }

    /**
     * Tests default values.
     */
    @Test
    public void testDefaultValues() {
        Map<String, Integer> connStr = new HashMap<>();
        connStr.put("jdbc:mysql:", 3306);
        connStr.put("jdbc:mysql://,", 3306);
        connStr.put("jdbc:mysql:loadbalance://,", 3306);
        connStr.put("jdbc:mysql:replication://,", 3306);
        connStr.put("mysqlx:", 33060);

        for (String cs : connStr.keySet()) {
            ConnectionUrl connUrl = ConnectionUrl.getConnectionUrlInstance(cs, null);
            for (HostInfo hi : connUrl.getHostsList()) {
                assertEquals(cs + "#databaseUrl", cs, hi.getDatabaseUrl());
                assertEquals(cs + "#host", "localhost", hi.getHost());
                assertEquals(cs + "#port", connStr.get(cs).intValue(), hi.getPort());
                assertEquals(cs + "#hostPortPair", "localhost:" + connStr.get(cs), hi.getHostPortPair());
                assertEquals(cs + "#user", "", hi.getUser());
                assertEquals(cs + "#password", "", hi.getPassword());
                assertEquals(cs + "#database", "", hi.getDatabase());
            }
        }
    }

    /**
     * Tests loading properties from config files.
     */
    @Test
    public void testLoadingPropertiesFromConfigFiles() {
        Properties propsFromFile = ConnectionUrl.getPropertiesFromConfigFiles("fullDebug");

        List<String> connStr = new ArrayList<>();
        connStr.add("jdbc:mysql://johndoe:secret@mysql:1234/sakila");
        connStr.add("jdbc:mysql://johndoe:secret@mysql:1234/sakila?useConfigs=fullDebug");
        connStr.add("jdbc:mysql://johndoe:secret@address=(host=mysql)(port=1234)(useConfigs=fullDebug)/sakila");

        for (String cs : connStr) {
            Properties props = new Properties();
            if (cs.indexOf(PropertyDefinitions.PNAME_useConfigs) == -1) {
                // Send "useConfigs" through external properties.
                props.setProperty(PropertyDefinitions.PNAME_useConfigs, "fullDebug");
            }
            ConnectionUrl connUrl = ConnectionUrl.getConnectionUrlInstance(cs, props);

            if (cs.indexOf("address=") == -1) {
                // Properties from file must be found simultaneously in per connection properties and per host.
                Properties asProps = connUrl.getConnectionArgumentsAsProperties();
                for (String key : propsFromFile.stringPropertyNames()) {
                    assertEquals(cs + "#" + key, propsFromFile.getProperty(key), asProps.getProperty(key));
                }
                Map<String, String> asMap = connUrl.getOriginalProperties();
                for (String key : propsFromFile.stringPropertyNames()) {
                    assertEquals(cs + "#" + key, propsFromFile.getProperty(key), asMap.get(key));
                }
            }
            Properties hostProps = connUrl.getMainHost().exposeAsProperties();
            for (String key : propsFromFile.stringPropertyNames()) {
                assertEquals(cs + "#" + key, propsFromFile.getProperty(key), hostProps.getProperty(key));
            }
        }
    }

    /**
     * Tests the usage of a properties transformer.
     */
    @Test
    public void testPropertiesTransformer() {
        String propsTransClassName = ConnectionPropertiesTest.class.getName();
        List<String> connStr = new ArrayList<>();
        connStr.add("jdbc:mysql://johndoe:secret@mysql:1234/sakila");
        connStr.add("jdbc:mysql://johndoe:secret@mysql:1234/sakila?stars=*");
        connStr.add("jdbc:mysql://johndoe:secret@address=(host=mysql)(port=1234)/sakila");
        connStr.add("jdbc:mysql://johndoe:secret@address=(host=mysql)(port=1234)(stars=*)/sakila");
        connStr.add("jdbc:mysql://johndoe:secret@address=(host=mysql)(port=1234)/sakila?stars=*");
        connStr.add("jdbc:mysql://johndoe:secret@mysql:1234/sakila?propertiesTransform=" + propsTransClassName);
        connStr.add("jdbc:mysql://johndoe:secret@mysql:1234/sakila?stars=*&propertiesTransform=" + propsTransClassName);
        connStr.add("jdbc:mysql://johndoe:secret@address=(host=mysql)(port=1234)(propertiesTransform=" + propsTransClassName + ")/sakila");
        connStr.add("jdbc:mysql://johndoe:secret@address=(host=mysql)(port=1234)(propertiesTransform=" + propsTransClassName + ")(stars=*)/sakila");
        connStr.add("jdbc:mysql://johndoe:secret@address=(host=mysql)(port=1234)(propertiesTransform=" + propsTransClassName + ")/sakila?stars=*");

        for (String cs : connStr) {
            Properties props = new Properties();
            if (cs.indexOf(PropertyDefinitions.PNAME_propertiesTransform) == -1) {
                // Send "propertiesTransform" parameter through external properties.
                props.setProperty(PropertyDefinitions.PNAME_propertiesTransform, propsTransClassName);
            }
            if (cs.indexOf("stars") == -1) {
                // Send "stars" parameter through external properties.
                props.setProperty("stars", "*");

            }
            ConnectionUrl connUrl = ConnectionUrl.getConnectionUrlInstance(cs, props);

            // "propertiesTransform" doesn't apply when set through host internal properties.
            boolean transforms = cs.indexOf("(propertiesTransform") == -1;

            assertEquals(cs + "#hostProps", transforms ? "**" : "*", connUrl.getMainHost().getProperty("stars"));
            if (cs.indexOf("(stars") == -1) {
                assertEquals(cs + "#originalProps", "*", connUrl.getOriginalProperties().get("stars"));
                assertEquals(cs + "#connProps", transforms ? "**" : "*", connUrl.getConnectionArgumentsAsProperties().getProperty("stars"));
            } else {
                assertNull(cs + "#originalProps", connUrl.getOriginalProperties().get("stars"));
                assertNull(cs + "#connProps", connUrl.getConnectionArgumentsAsProperties().getProperty("stars"));
            }
        }
    }

    public static class ConnectionPropertiesTest implements ConnectionPropertiesTransform {
        public Properties transformProperties(Properties props) {
            if (props.containsKey("stars")) {
                props.setProperty("stars", props.getProperty("stars") + props.getProperty("stars"));
            }
            return props;
        }
    }

    /**
     * Tests specifics for the X Plugin connection strings.
     */
    @Test
    public void testMysqlxConnectionUrl() {
        ConnectionUrl connUrl;
        int hostIdx;

        // Hosts sub list with "address" splitting (host3:3333) and priority value.
        connUrl = ConnectionUrl.getConnectionUrlInstance("mysqlx://johndoe:secret@[host1:1111,address=(host=host2)(port=2222)(priority=99),"
                + "(address=host3:3333,priority=98)]/db?address=host4:4444&priority=100", null);
        hostIdx = 1;
        for (HostInfo hi : connUrl.getHostsList()) {
            String testCase = "Host " + hostIdx + ":";
            assertEquals(testCase, "johndoe", hi.getUser());
            assertEquals(testCase, "secret", hi.getPassword());
            assertEquals(testCase, "host" + hostIdx, hi.getHost());
            assertEquals(testCase, 1111 * hostIdx, hi.getPort());
            assertEquals(testCase, "db", hi.getDatabase());
            assertTrue(testCase, hi.getHostProperties().containsKey(PropertyKey.PRIORITY.getKeyName()));
            assertEquals(testCase, Integer.toString(101 - hostIdx), hi.getHostProperties().get(PropertyKey.PRIORITY.getKeyName()));
            hostIdx++;
        }

        // Hosts sub list with "address" splitting (host3:3333) and without priority value.
        connUrl = ConnectionUrl.getConnectionUrlInstance(
                "mysqlx://johndoe:secret@[host1:1111,address=(host=host2)(port=2222)," + "(address=host3:3333)]/db?address=host4:4444", null);
        hostIdx = 1;
        for (HostInfo hi : connUrl.getHostsList()) {
            String testCase = "Host " + hostIdx + ":";
            assertEquals(testCase, "johndoe", hi.getUser());
            assertEquals(testCase, "secret", hi.getPassword());
            assertEquals(testCase, "host" + hostIdx, hi.getHost());
            assertEquals(testCase, 1111 * hostIdx, hi.getPort());
            assertEquals(testCase, "db", hi.getDatabase());
            assertFalse(testCase, hi.getHostProperties().containsKey(PropertyKey.PRIORITY.getKeyName()));
            hostIdx++;
        }

        // Hosts list with "address" splitting (host3:3333) and priority value.
        connUrl = ConnectionUrl.getConnectionUrlInstance("mysqlx://johndoe:secret@host1:1111,johndoe:secret@address=(host=host2)(port=2222)(priority=99),"
                + "johndoe:secret@(address=host3:3333,priority=98)/db?address=host4:4444&priority=100", null);
        hostIdx = 1;
        for (HostInfo hi : connUrl.getHostsList()) {
            String testCase = "Host " + hostIdx + ":";
            assertEquals(testCase, "johndoe", hi.getUser());
            assertEquals(testCase, "secret", hi.getPassword());
            assertEquals(testCase, "host" + hostIdx, hi.getHost());
            assertEquals(testCase, 1111 * hostIdx, hi.getPort());
            assertEquals(testCase, "db", hi.getDatabase());
            assertTrue(testCase, hi.getHostProperties().containsKey(PropertyKey.PRIORITY.getKeyName()));
            assertEquals(testCase, Integer.toString(101 - hostIdx), hi.getHostProperties().get(PropertyKey.PRIORITY.getKeyName()));
            hostIdx++;
        }

        // Hosts list with "address" splitting (host3:3333) and without priority value.
        connUrl = ConnectionUrl.getConnectionUrlInstance("mysqlx://johndoe:secret@host1:1111,johndoe:secret@address=(host=host2)(port=2222),"
                + "johndoe:secret@(address=host3:3333)/db?address=host4:4444", null);
        hostIdx = 1;
        for (HostInfo hi : connUrl.getHostsList()) {
            String testCase = "Host " + hostIdx + ":";
            assertEquals(testCase, "johndoe", hi.getUser());
            assertEquals(testCase, "secret", hi.getPassword());
            assertEquals(testCase, "host" + hostIdx, hi.getHost());
            assertEquals(testCase, 1111 * hostIdx, hi.getPort());
            assertEquals(testCase, "db", hi.getDatabase());
            assertFalse(testCase, hi.getHostProperties().containsKey(PropertyKey.PRIORITY.getKeyName()));
            hostIdx++;
        }

        List<String> connStr;

        // Error for distinct credentials.
        connStr = new ArrayList<>();
        connStr.add("mysqlx://johndoe:secret@host1:1111,janedoe:secret@host2:2222/db");
        connStr.add("mysqlx://johndoe:secret@host1:1111,johndoe:public@host2:2222/db");
        connStr.add("mysqlx://johndoe:secret@host1:1111,address=(host=host2)(port=2222)(user=janedoe)(password=secret)/db");
        connStr.add("mysqlx://johndoe:secret@host1:1111,address=(host=host2)(port=2222)(user=johndoe)(password=public)/db");
        connStr.add("mysqlx://johndoe:secret@host1:1111,(host=host2,port=2222,user=janedoe,password=secret)/db");
        connStr.add("mysqlx://johndoe:secret@host1:1111,(host=host2,port=2222,user=johndoe,password=public)/db");
        for (String cs : connStr) {
            try {
                connUrl = ConnectionUrl.getConnectionUrlInstance(cs, null);
                System.out.println(connUrl);
                fail(cs + ": expected to throw a " + WrongArgumentException.class.getName());
            } catch (Exception e) {
                assertTrue(cs + ": expected to throw a " + WrongArgumentException.class.getName(), WrongArgumentException.class.isAssignableFrom(e.getClass()));
                assertEquals(cs, Messages.getString("ConnectionString.14", new Object[] { ConnectionUrl.Type.XDEVAPI_SESSION.getScheme() }), e.getMessage());
            }
        }

        // Error for missing priority value.
        connStr = new ArrayList<>();
        connStr.add("mysqlx://johndoe:secret@[(address=host1:1111,priority=1),host2:2222]/db");
        connStr.add("mysqlx://johndoe:secret@[(address=host1:1111,priority=1),(address=host2:2222)]/db");
        connStr.add("mysqlx://johndoe:secret@[(address=host1:1111,priority=1),address(host=host2)(port=2222)]/db");
        connStr.add("mysqlx://johndoe:secret@[host1:1111,address=(host=host2)(port=2222)(priority=2)]/db");
        connStr.add("mysqlx://johndoe:secret@[(address=host1:1111),address=(host=host2)(port=2222)(priority=2)]/db");
        connStr.add("mysqlx://johndoe:secret@[address=(host=host1)(port=1111),address=(host=host2)(port=2222)(priority=2)]/db");
        for (String cs : connStr) {
            try {
                connUrl = ConnectionUrl.getConnectionUrlInstance(cs, null);
                System.out.println(connUrl);
                fail(cs + ": expected to throw a " + WrongArgumentException.class.getName());
            } catch (Exception e) {
                assertTrue(cs + ": expected to throw a " + WrongArgumentException.class.getName(), WrongArgumentException.class.isAssignableFrom(e.getClass()));
                assertEquals(cs, Messages.getString("ConnectionString.15", new Object[] { ConnectionUrl.Type.XDEVAPI_SESSION.getScheme() }), e.getMessage());
            }
        }

        // Error for wrong priority value.
        connStr = new ArrayList<>();
        connStr.add("mysqlx://(address=host1:1111,priority=-1)/db");
        connStr.add("mysqlx://(address=host1:1111,priority=101)/db");
        for (String cs : connStr) {
            try {
                connUrl = ConnectionUrl.getConnectionUrlInstance(cs, null);
                System.out.println(connUrl);
                fail(cs + ": expected to throw a " + WrongArgumentException.class.getName());
            } catch (Exception e) {
                assertTrue(cs + ": expected to throw a " + WrongArgumentException.class.getName(), WrongArgumentException.class.isAssignableFrom(e.getClass()));
                assertEquals(cs, Messages.getString("ConnectionString.16", new Object[] { ConnectionUrl.Type.XDEVAPI_SESSION.getScheme() }), e.getMessage());
            }
        }

        // Sorting hosts by default priority.
        connUrl = ConnectionUrl.getConnectionUrlInstance("mysqlx://johndoe:secret@[host2,host3,host1,host5,host4]/db", null);
        assertEquals("host2", connUrl.getMainHost().getHost());
        assertEquals("host2", connUrl.getHostsList().get(0).getHost());
        assertEquals("host3", connUrl.getHostsList().get(1).getHost());
        assertEquals("host1", connUrl.getHostsList().get(2).getHost());
        assertEquals("host5", connUrl.getHostsList().get(3).getHost());
        assertEquals("host4", connUrl.getHostsList().get(4).getHost());

        // Sorting hosts by defined priority.
        connUrl = ConnectionUrl
                .getConnectionUrlInstance("mysqlx://johndoe:secret@[(address=host1,priority=50),(address=host2,priority=100),(address=host3,priority=75),"
                        + "(address=host4,priority=0),(address=host5,priority=25)]/db", null);
        assertEquals("host2", connUrl.getMainHost().getHost());
        assertEquals("host2", connUrl.getHostsList().get(0).getHost());
        assertEquals("host3", connUrl.getHostsList().get(1).getHost());
        assertEquals("host1", connUrl.getHostsList().get(2).getHost());
        assertEquals("host5", connUrl.getHostsList().get(3).getHost());
        assertEquals("host4", connUrl.getHostsList().get(4).getHost());
    }
}
