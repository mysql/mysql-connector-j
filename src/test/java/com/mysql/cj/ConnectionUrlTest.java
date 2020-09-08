/*
 * Copyright (c) 2016, 2020, Oracle and/or its affiliates.
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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

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
import java.util.concurrent.Callable;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.junit.jupiter.api.Test;

import com.mysql.cj.conf.ConnectionPropertiesTransform;
import com.mysql.cj.conf.ConnectionUrl;
import com.mysql.cj.conf.ConnectionUrl.Type;
import com.mysql.cj.conf.ConnectionUrlParser;
import com.mysql.cj.conf.HostInfo;
import com.mysql.cj.conf.HostsListView;
import com.mysql.cj.conf.PropertyDefinitions.ZeroDatetimeBehavior;
import com.mysql.cj.conf.PropertyKey;
import com.mysql.cj.conf.url.FailoverConnectionUrl;
import com.mysql.cj.conf.url.FailoverDnsSrvConnectionUrl;
import com.mysql.cj.conf.url.LoadBalanceConnectionUrl;
import com.mysql.cj.conf.url.LoadBalanceDnsSrvConnectionUrl;
import com.mysql.cj.conf.url.ReplicationConnectionUrl;
import com.mysql.cj.conf.url.ReplicationDnsSrvConnectionUrl;
import com.mysql.cj.conf.url.SingleConnectionUrl;
import com.mysql.cj.conf.url.XDevApiConnectionUrl;
import com.mysql.cj.conf.url.XDevApiDnsSrvConnectionUrl;
import com.mysql.cj.exceptions.CJException;
import com.mysql.cj.exceptions.InvalidConnectionAttributeException;
import com.mysql.cj.exceptions.WrongArgumentException;

public class ConnectionUrlTest {
    protected static <EX extends Throwable> EX assertThrows(Class<EX> throwable, Callable<?> testRoutine) {
        return assertThrows("", throwable, null, testRoutine);
    }

    protected static <EX extends Throwable> EX assertThrows(String message, Class<EX> throwable, Callable<?> testRoutine) {
        return assertThrows(message, throwable, null, testRoutine);
    }

    protected static <EX extends Throwable> EX assertThrows(Class<EX> throwable, String msgMatchesRegex, Callable<?> testRoutine) {
        return assertThrows("", throwable, msgMatchesRegex, testRoutine);
    }

    protected static <EX extends Throwable> EX assertThrows(String message, Class<EX> throwable, String msgMatchesRegex, Callable<?> testRoutine) {
        if (message.length() > 0) {
            message += " ";
        }
        try {
            testRoutine.call();
        } catch (Throwable t) {
            if (!throwable.isAssignableFrom(t.getClass())) {
                fail(message + "expected exception of type '" + throwable.getName() + "' but instead a exception of type '" + t.getClass().getName()
                        + "' was thrown.");
            }

            if (msgMatchesRegex != null && !t.getMessage().matches(msgMatchesRegex)) {
                fail(message + "the error message «" + t.getMessage() + "» was expected to match «" + msgMatchesRegex + "».");
            }

            return throwable.cast(t);
        }
        fail(message + "expected exception of type '" + throwable.getName() + "'.");

        // never reaches here
        return null;
    }

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
        private static final String[] STD_HOST = new String[] { "", "myhost", "192.168.0.1", "[1000:abcd::1]",
                "verylonghostname01234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789" };
        private static final String[] STD_PORT = new String[] { "", ":", ":1234" };
        private static final String[] KEY_VALUE_HOST = new String[] { "", "()", "(host=[::1],port=1234,prio=1)",
                "(protocol=tcp,host=myhost,port=1234,key=value%28%29)", "(address=myhost:1234,prio=2)",
                "(protocol=tcp,host=verylonghostname01234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789,port=1234,key=value%28%29)",
                "(address=verylonghostname01234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789:1234,prio=2)" };
        private static final String[] ADDRESS_EQUALS_HOST = new String[] { "address=", "address=()", "address=(flag)",
                "address=(protocol=tcp)(host=myhost)(port=1234)", "address=(protocol=tcp)(host=myhost)(port=1234)(key=value%28%29)",
                "address=(protocol=tcp)(host=verylonghostname01234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789)(port=1234)",
                "address=(protocol=tcp)(host=verylonghostname01234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789)(port=1234)(key=value%28%29)" };
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
         * @param urlMode
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
     * Checks if the values returned from {@link Type#fromValue(String, int)} are correct.
     */
    @Test
    public void testTypeEnumCorrectValues() {
        // Standard schemes:
        assertEquals(ConnectionUrl.Type.SINGLE_CONNECTION, ConnectionUrl.Type.fromValue("jdbc:mysql:", 1));
        assertEquals(ConnectionUrl.Type.FAILOVER_CONNECTION, ConnectionUrl.Type.fromValue("jdbc:mysql:", 2));
        assertEquals(ConnectionUrl.Type.FAILOVER_CONNECTION, ConnectionUrl.Type.fromValue("jdbc:mysql:", 3));
        assertEquals(ConnectionUrl.Type.LOADBALANCE_CONNECTION, ConnectionUrl.Type.fromValue("jdbc:mysql:loadbalance:", 1));
        assertEquals(ConnectionUrl.Type.LOADBALANCE_CONNECTION, ConnectionUrl.Type.fromValue("jdbc:mysql:loadbalance:", 2));
        assertEquals(ConnectionUrl.Type.REPLICATION_CONNECTION, ConnectionUrl.Type.fromValue("jdbc:mysql:replication:", 1));
        assertEquals(ConnectionUrl.Type.REPLICATION_CONNECTION, ConnectionUrl.Type.fromValue("jdbc:mysql:replication:", 2));
        assertEquals(ConnectionUrl.Type.XDEVAPI_SESSION, ConnectionUrl.Type.fromValue("mysqlx:", 1));
        // DNS SRV schemes:
        assertEquals(ConnectionUrl.Type.FAILOVER_DNS_SRV_CONNECTION, ConnectionUrl.Type.fromValue("jdbc:mysql+srv:", 1));
        assertEquals(ConnectionUrl.Type.LOADBALANCE_DNS_SRV_CONNECTION, ConnectionUrl.Type.fromValue("jdbc:mysql+srv:loadbalance:", 1));
        assertEquals(ConnectionUrl.Type.REPLICATION_DNS_SRV_CONNECTION, ConnectionUrl.Type.fromValue("jdbc:mysql+srv:replication:", 1));
        assertEquals(ConnectionUrl.Type.REPLICATION_DNS_SRV_CONNECTION, ConnectionUrl.Type.fromValue("jdbc:mysql+srv:replication:", 2));
        assertEquals(ConnectionUrl.Type.XDEVAPI_DNS_SRV_SESSION, ConnectionUrl.Type.fromValue("mysqlx+srv:", 1));
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
                assertEquals(csg.getProtocol(), cup.getScheme(), cs);
                // User & Host:
                assertEquals(urlMode.getHostsCount(), cup.getHosts().size(), cs);
                for (int hostIndex = 0; hostIndex < urlMode.getHostsCount(); hostIndex++) {
                    HostInfo hi = cup.getHosts().get(hostIndex);
                    // User(n):
                    expected = testCSParserTrimTail(testCSParserTrimHead(csg.getUserInfo(hostIndex), ":"), "@", ":");
                    actual = new StringBuilder(hi.getUser() == null ? "" : hi.getUser()).append(":").append(hi.getPassword() == null ? "" : hi.getPassword())
                            .toString();
                    actual = testCSParserTrimTail(testCSParserTrimHead(actual, ":"), ":");
                    assertEquals(expected, actual, cs);
                    if (csg.getHostInfo(hostIndex).startsWith("address=") || csg.getHostInfo(hostIndex).startsWith("(")) {
                        // Host props(n):
                        assertEquals(csg.getHostParamsCount(hostIndex), hi.getHostProperties().size(), cs);
                        for (Entry<String, String> kv : hi.getHostProperties().entrySet()) {
                            assertTrue(csg.hasHostParam(hostIndex, kv.getKey(), kv.getValue()), cs);
                        }
                    } else {
                        // Host(n)
                        expected = testCSParserTrimTail(testCSParserTrimHead(csg.getHostInfo(hostIndex), ":"), ":");
                        actual = new StringBuilder(hi.getHost() == null ? "" : hi.getHost()).append(":").append(hi.getPort() == -1 ? "" : hi.getPort())
                                .toString();
                        actual = testCSParserTrimTail(testCSParserTrimHead(actual, ":"), ":");
                        assertEquals(expected, actual, cs);
                    }
                }
                // Database:
                expected = testCSParserTrimHead(csg.getDatabase(), "/");
                actual = cup.getPath() == null ? "" : testCSParserTrimHead(cup.getPath(), "/");
                assertEquals(expected, actual, cs);
                // Connection arguments:
                assertEquals(csg.getParamsCount(), cup.getProperties().size(), cs);
                for (Entry<String, String> kv : cup.getProperties().entrySet()) {
                    assertTrue(csg.hasParam(kv.getKey(), kv.getValue()), cs);
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
        assertTrue(ConnectionUrl.acceptsUrl(
                "jdbc:mysql://verylonghostname01234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789:1234/db?key=value"));
        assertTrue(ConnectionUrl.acceptsUrl("jdbc:mysql://127.0.0.1:1234/db?key=value"));
        assertTrue(ConnectionUrl.acceptsUrl("jdbc:mysql:loadbalance:"));
        assertTrue(ConnectionUrl.acceptsUrl("jdbc:mysql:loadbalance://somehost:1234/db?key=value"));
        assertTrue(ConnectionUrl.acceptsUrl(
                "jdbc:mysql:loadbalance://verylonghostname01234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789:1234/db?key=value"));
        assertTrue(ConnectionUrl.acceptsUrl("jdbc:mysql:replication:"));
        assertTrue(ConnectionUrl.acceptsUrl("jdbc:mysql:replication://somehost:1234/db?key=value"));
        assertTrue(ConnectionUrl.acceptsUrl(
                "jdbc:mysql:replication://verylonghostname01234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789:1234/db?key=value"));
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

        // Supported DNS-SRV variants:
        assertTrue(ConnectionUrl.acceptsUrl("jdbc:mysql+srv:"));
        assertTrue(ConnectionUrl.acceptsUrl("jdbc:mysql+srv://somehost:1234/db?key=value"));
        assertTrue(ConnectionUrl.acceptsUrl(
                "jdbc:mysql+srv://verylonghostname01234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789:1234/db?key=value"));
        assertTrue(ConnectionUrl.acceptsUrl("jdbc:mysql+srv://127.0.0.1:1234/db?key=value"));
        assertTrue(ConnectionUrl.acceptsUrl("jdbc:mysql+srv:loadbalance:"));
        assertTrue(ConnectionUrl.acceptsUrl("jdbc:mysql+srv:loadbalance://somehost:1234/db?key=value"));
        assertTrue(ConnectionUrl.acceptsUrl(
                "jdbc:mysql:loadbalance://verylonghostname01234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789:1234/db?key=value"));
        assertTrue(ConnectionUrl.acceptsUrl("jdbc:mysql+srv:replication:"));
        assertTrue(ConnectionUrl.acceptsUrl("jdbc:mysql+srv:replication://somehost:1234/db?key=value"));
        assertTrue(ConnectionUrl.acceptsUrl(
                "jdbc:mysql+srv:replication://verylonghostname01234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789:1234/db?key=value"));
        assertTrue(ConnectionUrl.acceptsUrl("jdbc:mysql+srv:"));
        assertTrue(ConnectionUrl.acceptsUrl("mysqlx+srv://somehost:1234/db?key=value"));
        assertTrue(ConnectionUrl.acceptsUrl("mysqlx+srv://127.0.0.1:1234/db?key=value"));
        assertTrue(ConnectionUrl.acceptsUrl("mysqlx+srv://[::1]:1234/db?key=value"));
        assertTrue(ConnectionUrl.acceptsUrl("mysqlx+srv://[fe80::250:56ff:fec0:8]:1234/db?key=value"));
        assertTrue(ConnectionUrl.acceptsUrl("mysqlx+srv://johndoe:secret@[::1]:1234/db?key=value"));
        assertTrue(ConnectionUrl.acceptsUrl("mysqlx+srv://johndoe:secret@[[::1]:1234]/db?key=value"));
        assertTrue(ConnectionUrl.acceptsUrl("mysqlx+srv://johndoe:secret@[[::1]:1234,(address=[abcd:1000::f09a]:4321,priority=100)]/db?key=value"));
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
                    assertEquals(ConnectionUrl.Type.XDEVAPI_SESSION.getScheme(), csg.getProtocol(), cs);
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
        connStr.add("jdbc:mysql://johndoe:secret@myhost:1234/db?=value");

        for (String cs : connStr) {
            try {
                System.out.println(ConnectionUrl.getConnectionUrlInstance(cs, null));
                fail(cs + ": expected to throw a " + WrongArgumentException.class.getName());
            } catch (Exception e) {
                assertTrue(WrongArgumentException.class.isAssignableFrom(e.getClass()), cs + ": expected to throw a " + WrongArgumentException.class.getName());
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
                assertEquals(cs, hi.getDatabaseUrl(), cs + "#databaseUrl");
                assertEquals("localhost", hi.getHost(), cs + "#host");
                assertEquals(connStr.get(cs).intValue(), hi.getPort(), cs + "#port");
                assertEquals("localhost:" + connStr.get(cs), hi.getHostPortPair(), cs + "#hostPortPair");
                assertEquals("", hi.getUser(), cs + "#user");
                assertEquals("", hi.getPassword(), cs + "#password");
                assertEquals("", hi.getDatabase(), cs + "#database");
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
            if (cs.indexOf(PropertyKey.useConfigs.getKeyName()) == -1) {
                // Send "useConfigs" through external properties.
                props.setProperty(PropertyKey.useConfigs.getKeyName(), "fullDebug");
            }
            ConnectionUrl connUrl = ConnectionUrl.getConnectionUrlInstance(cs, props);

            if (cs.indexOf("address=") == -1) {
                // Properties from file must be found simultaneously in per connection properties and per host.
                Properties asProps = connUrl.getConnectionArgumentsAsProperties();
                for (String key : propsFromFile.stringPropertyNames()) {
                    assertEquals(propsFromFile.getProperty(key), asProps.getProperty(key), cs + "#" + key);
                }
                Map<String, String> asMap = connUrl.getOriginalProperties();
                for (String key : propsFromFile.stringPropertyNames()) {
                    assertEquals(propsFromFile.getProperty(key), asMap.get(key), cs + "#" + key);
                }
            }
            Properties hostProps = connUrl.getMainHost().exposeAsProperties();
            for (String key : propsFromFile.stringPropertyNames()) {
                assertEquals(propsFromFile.getProperty(key), hostProps.getProperty(key), cs + "#" + key);
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
            if (cs.indexOf(PropertyKey.propertiesTransform.getKeyName()) == -1) {
                // Send "propertiesTransform" parameter through external properties.
                props.setProperty(PropertyKey.propertiesTransform.getKeyName(), propsTransClassName);
            }
            if (cs.indexOf("stars") == -1) {
                // Send "stars" parameter through external properties.
                props.setProperty("stars", "*");

            }
            ConnectionUrl connUrl = ConnectionUrl.getConnectionUrlInstance(cs, props);

            // "propertiesTransform" doesn't apply when set through host internal properties.
            boolean transforms = cs.indexOf("(propertiesTransform") == -1;

            assertEquals(transforms ? "**" : "*", connUrl.getMainHost().getProperty("stars"), cs + "#hostProps");
            if (cs.indexOf("(stars") == -1) {
                assertEquals("*", connUrl.getOriginalProperties().get("stars"), cs + "#originalProps");
                assertEquals(transforms ? "**" : "*", connUrl.getConnectionArgumentsAsProperties().getProperty("stars"), cs + "#connProps");
            } else {
                assertNull(connUrl.getOriginalProperties().get("stars"), cs + "#originalProps");
                assertNull(connUrl.getConnectionArgumentsAsProperties().getProperty("stars"), cs + "#connProps");
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

        String[] hostNames = new String[] { "host",
                "verylonghostname0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789"
                        + "0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789" };

        for (String hostName : hostNames) {
            // Hosts sub list with "address" splitting (host3:3333) and priority value.
            connUrl = ConnectionUrl.getConnectionUrlInstance("mysqlx://johndoe:secret@[" + hostName + "1:1111,address=(host=" + hostName
                    + "2)(port=2222)(priority=99)," + "(address=" + hostName + "3:3333,priority=98)]/db?address=host4:4444&priority=100", null);
            hostIdx = 1;
            for (HostInfo hi : connUrl.getHostsList()) {
                String testCase = "Host " + hostIdx + ":";
                assertEquals("johndoe", hi.getUser(), testCase);
                assertEquals("secret", hi.getPassword(), testCase);
                assertEquals(hostName + hostIdx, hi.getHost(), testCase);
                assertEquals(1111 * hostIdx, hi.getPort(), testCase);
                assertEquals("db", hi.getDatabase(), testCase);
                assertTrue(hi.getHostProperties().containsKey(PropertyKey.PRIORITY.getKeyName()), testCase);
                assertEquals(Integer.toString(101 - hostIdx), hi.getHostProperties().get(PropertyKey.PRIORITY.getKeyName()), testCase);
                hostIdx++;
            }

            // Hosts sub list with "address" splitting (host3:3333) and without priority value (randomly sorted).
            connUrl = ConnectionUrl.getConnectionUrlInstance("mysqlx://johndoe:secret@[" + hostName + "1:1111,address=(host=" + hostName + "2)(port=2222),"
                    + "(address=" + hostName + "3:3333)]/db?address=host4:4444", null);
            String hosts = IntStream.range(1, 4).mapToObj(i -> hostName + i).collect(Collectors.joining("|"));
            String ports = IntStream.range(1, 4).mapToObj(i -> 1111 * i).map(i -> i.toString()).collect(Collectors.joining("|"));
            hostIdx = 1;
            for (HostInfo hi : connUrl.getHostsList()) {
                String testCase = "Host " + hostIdx + ":";
                assertEquals("johndoe", hi.getUser(), testCase);
                assertEquals("secret", hi.getPassword(), testCase);
                hosts = hosts.replace(hi.getHost(), "");
                ports = ports.replace(Integer.toString(hi.getPort()), "");
                assertEquals("db", hi.getDatabase(), testCase);
                assertFalse(hi.getHostProperties().containsKey(PropertyKey.PRIORITY.getKeyName()), testCase);
                hostIdx++;
            }
            assertEquals("||", hosts);
            assertEquals("||", ports);

            // Hosts list with "address" splitting (host3:3333) and priority value.
            connUrl = ConnectionUrl.getConnectionUrlInstance("mysqlx://johndoe:secret@" + hostName + "1:1111,johndoe:secret@address=(host=" + hostName
                    + "2)(port=2222)(priority=99)," + "johndoe:secret@(address=" + hostName + "3:3333,priority=98)/db?address=host4:4444&priority=100", null);
            hostIdx = 1;
            for (HostInfo hi : connUrl.getHostsList()) {
                String testCase = "Host " + hostIdx + ":";
                assertEquals("johndoe", hi.getUser(), testCase);
                assertEquals("secret", hi.getPassword(), testCase);
                assertEquals(hostName + hostIdx, hi.getHost(), testCase);
                assertEquals(1111 * hostIdx, hi.getPort(), testCase);
                assertEquals("db", hi.getDatabase(), testCase);
                assertTrue(hi.getHostProperties().containsKey(PropertyKey.PRIORITY.getKeyName()), testCase);
                assertEquals(Integer.toString(101 - hostIdx), hi.getHostProperties().get(PropertyKey.PRIORITY.getKeyName()), testCase);
                hostIdx++;
            }

            // Hosts list with "address" splitting (host3:3333) and without priority value (randomly sorted).
            connUrl = ConnectionUrl.getConnectionUrlInstance("mysqlx://johndoe:secret@" + hostName + "1:1111,johndoe:secret@address=(host=" + hostName
                    + "2)(port=2222)," + "johndoe:secret@(address=" + hostName + "3:3333)/db?address=host4:4444", null);
            hosts = IntStream.range(1, 4).mapToObj(i -> hostName + i).collect(Collectors.joining("|"));
            ports = IntStream.range(1, 4).mapToObj(i -> 1111 * i).map(i -> i.toString()).collect(Collectors.joining("|"));
            hostIdx = 1;
            for (HostInfo hi : connUrl.getHostsList()) {
                String testCase = "Host " + hostIdx + ":";
                assertEquals("johndoe", hi.getUser(), testCase);
                assertEquals("secret", hi.getPassword(), testCase);
                hosts = hosts.replace(hi.getHost(), "");
                ports = ports.replace(Integer.toString(hi.getPort()), "");
                assertEquals("db", hi.getDatabase(), testCase);
                assertFalse(hi.getHostProperties().containsKey(PropertyKey.PRIORITY.getKeyName()), testCase);
                hostIdx++;
            }
            assertEquals("||", hosts);
            assertEquals("||", ports);

            List<String> connStr;

            // Error for distinct credentials.
            connStr = new ArrayList<>();
            connStr.add("mysqlx://johndoe:secret@" + hostName + "1:1111,janedoe:secret@" + hostName + "2:2222/db");
            connStr.add("mysqlx://johndoe:secret@" + hostName + "1:1111,johndoe:public@" + hostName + "2:2222/db");
            connStr.add("mysqlx://johndoe:secret@" + hostName + "1:1111,address=(host=" + hostName + "2)(port=2222)(user=janedoe)(password=secret)/db");
            connStr.add("mysqlx://johndoe:secret@" + hostName + "1:1111,address=(host=" + hostName + "2)(port=2222)(user=johndoe)(password=public)/db");
            connStr.add("mysqlx://johndoe:secret@" + hostName + "1:1111,(host=" + hostName + "2,port=2222,user=janedoe,password=secret)/db");
            connStr.add("mysqlx://johndoe:secret@" + hostName + "1:1111,(host=" + hostName + "2,port=2222,user=johndoe,password=public)/db");
            for (String cs : connStr) {
                try {
                    connUrl = ConnectionUrl.getConnectionUrlInstance(cs, null);
                    System.out.println(connUrl);
                    fail(cs + ": expected to throw a " + WrongArgumentException.class.getName());
                } catch (Exception e) {
                    assertTrue(WrongArgumentException.class.isAssignableFrom(e.getClass()),
                            cs + ": expected to throw a " + WrongArgumentException.class.getName());
                    assertEquals(Messages.getString("ConnectionString.14", new Object[] { ConnectionUrl.Type.XDEVAPI_SESSION.getScheme() }), e.getMessage(),
                            cs);
                }
            }

            // Error for missing priority value.
            connStr = new ArrayList<>();
            connStr.add("mysqlx://johndoe:secret@[(address=" + hostName + "1:1111,priority=1)," + hostName + "2:2222]/db");
            connStr.add("mysqlx://johndoe:secret@[(address=" + hostName + "1:1111,priority=1),(address=" + hostName + "2:2222)]/db");
            connStr.add("mysqlx://johndoe:secret@[(address=" + hostName + "1:1111,priority=1),address(host=" + hostName + "2)(port=2222)]/db");
            connStr.add("mysqlx://johndoe:secret@[" + hostName + "1:1111,address=(host=" + hostName + "2)(port=2222)(priority=2)]/db");
            connStr.add("mysqlx://johndoe:secret@[(address=" + hostName + "1:1111),address=(host=" + hostName + "2)(port=2222)(priority=2)]/db");
            connStr.add("mysqlx://johndoe:secret@[address=(host=" + hostName + "1)(port=1111),address=(host=" + hostName + "2)(port=2222)(priority=2)]/db");
            for (String cs : connStr) {
                try {
                    connUrl = ConnectionUrl.getConnectionUrlInstance(cs, null);
                    System.out.println(connUrl);
                    fail(cs + ": expected to throw a " + WrongArgumentException.class.getName());
                } catch (Exception e) {
                    assertTrue(WrongArgumentException.class.isAssignableFrom(e.getClass()),
                            cs + ": expected to throw a " + WrongArgumentException.class.getName());
                    assertEquals(Messages.getString("ConnectionString.15", new Object[] { ConnectionUrl.Type.XDEVAPI_SESSION.getScheme() }), e.getMessage(),
                            cs);
                }
            }

            // Error for wrong priority value.
            connStr = new ArrayList<>();
            connStr.add("mysqlx://(address=" + hostName + "1:1111,priority=-1)/db");
            connStr.add("mysqlx://(address=" + hostName + "1:1111,priority=101)/db");
            for (String cs : connStr) {
                try {
                    connUrl = ConnectionUrl.getConnectionUrlInstance(cs, null);
                    System.out.println(connUrl);
                    fail(cs + ": expected to throw a " + WrongArgumentException.class.getName());
                } catch (Exception e) {
                    assertTrue(WrongArgumentException.class.isAssignableFrom(e.getClass()),
                            cs + ": expected to throw a " + WrongArgumentException.class.getName());
                    assertEquals(Messages.getString("ConnectionString.16", new Object[] { ConnectionUrl.Type.XDEVAPI_SESSION.getScheme() }), e.getMessage(),
                            cs);
                }
            }

            // Sorting hosts by default priority (random).
            connUrl = ConnectionUrl.getConnectionUrlInstance(
                    "mysqlx://johndoe:secret@[" + hostName + "2," + hostName + "3," + hostName + "1," + hostName + "5," + hostName + "4]/db", null);
            List<HostInfo> hostsList = connUrl.getHostsList();
            assertEquals(connUrl.getMainHost().getHost(), hostsList.get(0).getHost());
            hosts = IntStream.range(1, 6).mapToObj(i -> hostName + i).collect(Collectors.joining("|"));
            for (HostInfo hi : connUrl.getHostsList()) {
                hosts = hosts.replace(hi.getHost(), "");
            }
            assertEquals("||||", hosts);

            // Sorting hosts by defined priority.
            connUrl = ConnectionUrl.getConnectionUrlInstance(
                    "mysqlx://johndoe:secret@[(address=" + hostName + "1,priority=50),(address=" + hostName + "2,priority=100),(address=" + hostName
                            + "3,priority=75)," + "(address=" + hostName + "4,priority=0),(address=" + hostName + "5,priority=25)]/db",
                    null);
            hostsList = connUrl.getHostsList();
            assertEquals(hostName + "2", connUrl.getMainHost().getHost());
            assertEquals(hostName + "2", hostsList.get(0).getHost());
            assertEquals(hostName + "3", hostsList.get(1).getHost());
            assertEquals(hostName + "1", hostsList.get(2).getHost());
            assertEquals(hostName + "5", hostsList.get(3).getHost());
            assertEquals(hostName + "4", hostsList.get(4).getHost());

            // Sorting hosts by defined priority, with duplicates.
            connUrl = ConnectionUrl.getConnectionUrlInstance("mysqlx://johndoe:secret@[(address=" + hostName + "1,priority=50),(address=" + hostName
                    + "2,priority=100),(address=" + hostName + "3,priority=10)," + "(address=" + hostName + "4,priority=50),(address=" + hostName
                    + "5,priority=50),(address=" + hostName + "6,priority=75)]/db", null);
            hostsList = connUrl.getHostsList();
            assertEquals(hostName + "2", connUrl.getMainHost().getHost());
            assertEquals(hostName + "2", hostsList.get(0).getHost());
            assertEquals(hostName + "6", hostsList.get(1).getHost());
            hosts = IntStream.of(1, 4, 5).mapToObj(i -> hostName + i).collect(Collectors.joining("|"));
            hosts = hosts.replace(hostsList.get(2).getHost(), "");
            hosts = hosts.replace(hostsList.get(3).getHost(), "");
            hosts = hosts.replace(hostsList.get(4).getHost(), "");
            assertEquals("||", hosts);
            assertEquals(hostName + "3", hostsList.get(5).getHost());
        }
    }

    @Test
    public void testReplaceLegacyPropertyValues() throws Exception {
        /* Test zeroDateTimeBehavior convertToNull-> CONVERT_TO_NULL replacement (BUG#91421) */
        List<String> connStr = new ArrayList<>();
        connStr.add("jdbc:mysql://somehost:1234/db");
        connStr.add("jdbc:mysql://somehost:1234/db?key=value&zeroDateTimeBehavior=convertToNull");
        connStr.add("jdbc:mysql://127.0.0.1:1234/db");
        connStr.add("jdbc:mysql://127.0.0.1:1234/db?key=value&zeroDateTimeBehavior=convertToNull");
        connStr.add("jdbc:mysql://(port=3306,user=root,password=pwd,zeroDateTimeBehavior=convertToNull)/test");
        connStr.add("jdbc:mysql://address=(port=3306)(user=root)(password=pwd)(zeroDateTimeBehavior=convertToNull)/test");

        Properties props = new Properties();
        props.setProperty(PropertyKey.zeroDateTimeBehavior.getKeyName(), "convertToNull");

        for (String cs : connStr) {
            ConnectionUrl connUrl = ConnectionUrl.getConnectionUrlInstance(cs, props);
            assertEquals(ZeroDatetimeBehavior.CONVERT_TO_NULL.name(), connUrl.getMainHost().getProperty(PropertyKey.zeroDateTimeBehavior.getKeyName()));
        }
    }

    /**
     * Tests jdbc:mysql+srv: connection strings.
     */
    @Test
    public void testMysqlFailoverDnsSrvConnectionUrl() {
        Properties props;
        props = new Properties();
        props.setProperty(PropertyKey.dnsSrv.getKeyName(), "true");

        // Host missing.
        assertThrows(InvalidConnectionAttributeException.class, "A host name is required for DNS SRV lookup enabled connections\\.",
                () -> ConnectionUrl.getConnectionUrlInstance("jdbc:mysql+srv:", null));
        assertThrows(InvalidConnectionAttributeException.class, "A host name is required for DNS SRV lookup enabled connections\\.",
                () -> ConnectionUrl.getConnectionUrlInstance("jdbc:mysql:?dnsSrv=true", null));
        assertThrows(InvalidConnectionAttributeException.class, "A host name is required for DNS SRV lookup enabled connections\\.",
                () -> ConnectionUrl.getConnectionUrlInstance("jdbc:mysql:", props));

        // More than one host.
        assertThrows(InvalidConnectionAttributeException.class, "Specifying multiple host names with DNS SRV lookup is not allowed\\.",
                () -> ConnectionUrl.getConnectionUrlInstance("jdbc:mysql+srv://hostname1,hostname2", null));
        assertThrows(InvalidConnectionAttributeException.class, "Specifying multiple host names with DNS SRV lookup is not allowed\\.",
                () -> ConnectionUrl.getConnectionUrlInstance("jdbc:mysql://hostname1,hostname2?dnsSrv=true", null));
        assertThrows(InvalidConnectionAttributeException.class, "Specifying multiple host names with DNS SRV lookup is not allowed\\.",
                () -> ConnectionUrl.getConnectionUrlInstance("jdbc:mysql://hostname1,hostname2", props));

        // Port specified.
        props.setProperty(PropertyKey.PORT.getKeyName(), "12345");
        assertThrows(InvalidConnectionAttributeException.class, "Specifying a port number with DNS SRV lookup is not allowed\\.",
                () -> ConnectionUrl.getConnectionUrlInstance("jdbc:mysql+srv://hostname:12345", null));
        assertThrows(InvalidConnectionAttributeException.class, "Specifying a port number with DNS SRV lookup is not allowed\\.",
                () -> ConnectionUrl.getConnectionUrlInstance("jdbc:mysql://hostname?dnsSrv=true&port=12345", null));
        assertThrows(InvalidConnectionAttributeException.class, "Specifying a port number with DNS SRV lookup is not allowed\\.",
                () -> ConnectionUrl.getConnectionUrlInstance("jdbc:mysql://hostname", props));
        props.remove(PropertyKey.PORT.getKeyName());

        // Conflicting options.
        props.setProperty(PropertyKey.dnsSrv.getKeyName(), "false");
        assertThrows(InvalidConnectionAttributeException.class, "'dnsSrv' cannot be set to false with DNS SRV lookup enabled\\.",
                () -> ConnectionUrl.getConnectionUrlInstance("jdbc:mysql+srv://hostname?dnsSrv=false", null));
        assertThrows(InvalidConnectionAttributeException.class, "'dnsSrv' cannot be set to false with DNS SRV lookup enabled\\.",
                () -> ConnectionUrl.getConnectionUrlInstance("jdbc:mysql+srv://hostname", props));
        props.setProperty(PropertyKey.dnsSrv.getKeyName(), "true");

        // Setting protocol=PIPE not allowed.
        props.setProperty(PropertyKey.PROTOCOL.getKeyName(), "pipe");
        assertThrows(InvalidConnectionAttributeException.class, "Using named pipes with DNS SRV lookup is not allowed\\.",
                () -> ConnectionUrl.getConnectionUrlInstance("jdbc:mysql+srv://hostname?protocol=pipe", null));
        assertThrows(InvalidConnectionAttributeException.class, "Using named pipes with DNS SRV lookup is not allowed\\.",
                () -> ConnectionUrl.getConnectionUrlInstance("jdbc:mysql://hostname?dnsSrv=true&protocol=pipe", null));
        assertThrows(InvalidConnectionAttributeException.class, "Using named pipes with DNS SRV lookup is not allowed\\.",
                () -> ConnectionUrl.getConnectionUrlInstance("jdbc:mysql://hostname", props));
        props.remove(PropertyKey.PROTOCOL.getKeyName());

        // Resolving hosts fails.
        assertThrows(CJException.class, "Unable to locate any hosts for hostname\\.",
                () -> ConnectionUrl.getConnectionUrlInstance("jdbc:mysql+srv://hostname", null).getHostsList());

        // Correct ConnectionUrl instances - jdbc:mysql+srv:.
        assertEquals(FailoverDnsSrvConnectionUrl.class, ConnectionUrl.getConnectionUrlInstance("jdbc:mysql+srv://hostname", null).getClass());
        assertEquals(FailoverDnsSrvConnectionUrl.class, ConnectionUrl.getConnectionUrlInstance("jdbc:mysql+srv://hostname?dnsSrv=true", null).getClass());
        assertEquals(FailoverDnsSrvConnectionUrl.class, ConnectionUrl.getConnectionUrlInstance("jdbc:mysql+srv://hostname?dnsSrv=true", props).getClass());
        assertEquals(FailoverDnsSrvConnectionUrl.class, ConnectionUrl.getConnectionUrlInstance("jdbc:mysql+srv://hostname?dnsSrv=false", props).getClass());
        assertEquals(FailoverDnsSrvConnectionUrl.class, ConnectionUrl.getConnectionUrlInstance("jdbc:mysql://hostname?dnsSrv=true", null).getClass());
        assertEquals(FailoverDnsSrvConnectionUrl.class, ConnectionUrl.getConnectionUrlInstance("jdbc:mysql://hostname", props).getClass());
        assertEquals(FailoverDnsSrvConnectionUrl.class, ConnectionUrl.getConnectionUrlInstance("jdbc:mysql://hostname?dnsSrv=true", props).getClass());
        assertEquals(FailoverDnsSrvConnectionUrl.class, ConnectionUrl.getConnectionUrlInstance("jdbc:mysql://hostname?dnsSrv=false", props).getClass());

        // Correct ConnectionUrl instances - jdbc:mysql:.
        props.setProperty(PropertyKey.dnsSrv.getKeyName(), "false");
        assertEquals(SingleConnectionUrl.class, ConnectionUrl.getConnectionUrlInstance("jdbc:mysql://hostname?dnsSrv=false", null).getClass());
        assertEquals(SingleConnectionUrl.class, ConnectionUrl.getConnectionUrlInstance("jdbc:mysql://hostname", props).getClass());
        assertEquals(SingleConnectionUrl.class, ConnectionUrl.getConnectionUrlInstance("jdbc:mysql://hostname?dnsArv=false", props).getClass());
        assertEquals(SingleConnectionUrl.class, ConnectionUrl.getConnectionUrlInstance("jdbc:mysql://hostname?dnsSrv=true", props).getClass());
        assertEquals(FailoverConnectionUrl.class, ConnectionUrl.getConnectionUrlInstance("jdbc:mysql://hostname1,hostname2?dnsSrv=false", null).getClass());
        assertEquals(FailoverConnectionUrl.class, ConnectionUrl.getConnectionUrlInstance("jdbc:mysql://hostname1,hostname2", props).getClass());
        assertEquals(FailoverConnectionUrl.class, ConnectionUrl.getConnectionUrlInstance("jdbc:mysql://hostname1,hostname2?dnsArv=false", props).getClass());
        assertEquals(FailoverConnectionUrl.class, ConnectionUrl.getConnectionUrlInstance("jdbc:mysql://hostname1,hostname2?dnsSrv=true", props).getClass());
    }

    /**
     * Tests jdbc:mysql+srv:loadbalance: connection strings.
     */
    @Test
    public void testMysqlLoadBalanceDnsSrvConnectionUrl() {
        Properties props;
        props = new Properties();
        props.setProperty(PropertyKey.dnsSrv.getKeyName(), "true");

        // Host missing.
        assertThrows(InvalidConnectionAttributeException.class, "A host name is required for DNS SRV lookup enabled connections\\.",
                () -> ConnectionUrl.getConnectionUrlInstance("jdbc:mysql+srv:loadbalance:", null));
        assertThrows(InvalidConnectionAttributeException.class, "A host name is required for DNS SRV lookup enabled connections\\.",
                () -> ConnectionUrl.getConnectionUrlInstance("jdbc:mysql:loadbalance:?dnsSrv=true", null));
        assertThrows(InvalidConnectionAttributeException.class, "A host name is required for DNS SRV lookup enabled connections\\.",
                () -> ConnectionUrl.getConnectionUrlInstance("jdbc:mysql:loadbalance:", props));

        // More than one host.
        assertThrows(InvalidConnectionAttributeException.class, "Specifying multiple host names with DNS SRV lookup is not allowed\\.",
                () -> ConnectionUrl.getConnectionUrlInstance("jdbc:mysql+srv:loadbalance://hostname1,hostname2", null));
        assertThrows(InvalidConnectionAttributeException.class, "Specifying multiple host names with DNS SRV lookup is not allowed\\.",
                () -> ConnectionUrl.getConnectionUrlInstance("jdbc:mysql:loadbalance://hostname1,hostname2?dnsSrv=true", null));
        assertThrows(InvalidConnectionAttributeException.class, "Specifying multiple host names with DNS SRV lookup is not allowed\\.",
                () -> ConnectionUrl.getConnectionUrlInstance("jdbc:mysql:loadbalance://hostname1,hostname2", props));

        // Port specified.
        props.setProperty(PropertyKey.PORT.getKeyName(), "12345");
        assertThrows(InvalidConnectionAttributeException.class, "Specifying a port number with DNS SRV lookup is not allowed\\.",
                () -> ConnectionUrl.getConnectionUrlInstance("jdbc:mysql+srv:loadbalance://hostname:12345", null));
        assertThrows(InvalidConnectionAttributeException.class, "Specifying a port number with DNS SRV lookup is not allowed\\.",
                () -> ConnectionUrl.getConnectionUrlInstance("jdbc:mysql:loadbalance://hostname?dnsSrv=true&port=12345", null));
        assertThrows(InvalidConnectionAttributeException.class, "Specifying a port number with DNS SRV lookup is not allowed\\.",
                () -> ConnectionUrl.getConnectionUrlInstance("jdbc:mysql:loadbalance://hostname", props));
        props.remove(PropertyKey.PORT.getKeyName());

        // Conflicting options.
        props.setProperty(PropertyKey.dnsSrv.getKeyName(), "false");
        assertThrows(InvalidConnectionAttributeException.class, "'dnsSrv' cannot be set to false with DNS SRV lookup enabled\\.",
                () -> ConnectionUrl.getConnectionUrlInstance("jdbc:mysql+srv:loadbalance://hostname?dnsSrv=false", null));
        assertThrows(InvalidConnectionAttributeException.class, "'dnsSrv' cannot be set to false with DNS SRV lookup enabled\\.",
                () -> ConnectionUrl.getConnectionUrlInstance("jdbc:mysql+srv:loadbalance://hostname", props));
        props.setProperty(PropertyKey.dnsSrv.getKeyName(), "true");

        // Setting protocol=PIPE not allowed.
        props.setProperty(PropertyKey.PROTOCOL.getKeyName(), "pipe");
        assertThrows(InvalidConnectionAttributeException.class, "Using named pipes with DNS SRV lookup is not allowed\\.",
                () -> ConnectionUrl.getConnectionUrlInstance("jdbc:mysql+srv:loadbalance://hostname?protocol=pipe", null));
        assertThrows(InvalidConnectionAttributeException.class, "Using named pipes with DNS SRV lookup is not allowed\\.",
                () -> ConnectionUrl.getConnectionUrlInstance("jdbc:mysql:loadbalance://hostname?dnsSrv=true&protocol=pipe", null));
        assertThrows(InvalidConnectionAttributeException.class, "Using named pipes with DNS SRV lookup is not allowed\\.",
                () -> ConnectionUrl.getConnectionUrlInstance("jdbc:mysql:loadbalance://hostname", props));
        props.remove(PropertyKey.PROTOCOL.getKeyName());

        // Setting loadBalanceConnectionGroup not allowed.
        props.setProperty(PropertyKey.loadBalanceConnectionGroup.getKeyName(), "lbgrp");
        assertThrows(InvalidConnectionAttributeException.class,
                "The option 'loadBalanceConnectionGroup' cannot be set\\. Live management of connections is not supported with DNS SRV lookup\\.",
                () -> ConnectionUrl.getConnectionUrlInstance("jdbc:mysql+srv:loadbalance://hostname?loadBalanceConnectionGroup=lbgrp", null));
        assertThrows(InvalidConnectionAttributeException.class,
                "The option 'loadBalanceConnectionGroup' cannot be set\\. Live management of connections is not supported with DNS SRV lookup\\.",
                () -> ConnectionUrl.getConnectionUrlInstance("jdbc:mysql:loadbalance://hostname?dnsSrv=true&loadBalanceConnectionGroup=lbgrp", null));
        assertThrows(InvalidConnectionAttributeException.class,
                "The option 'loadBalanceConnectionGroup' cannot be set\\. Live management of connections is not supported with DNS SRV lookup\\.",
                () -> ConnectionUrl.getConnectionUrlInstance("jdbc:mysql:loadbalance://hostname", props));
        props.remove(PropertyKey.loadBalanceConnectionGroup.getKeyName());

        // Resolving hosts fails.
        assertThrows(CJException.class, "Unable to locate any hosts for hostname\\.",
                () -> ConnectionUrl.getConnectionUrlInstance("jdbc:mysql+srv:loadbalance://hostname", null).getHostsList());

        // Correct ConnectionUrl instances - jdbc:mysql+srv:loadbalance:.
        assertEquals(LoadBalanceDnsSrvConnectionUrl.class, ConnectionUrl.getConnectionUrlInstance("jdbc:mysql+srv:loadbalance://hostname", null).getClass());
        assertEquals(LoadBalanceDnsSrvConnectionUrl.class,
                ConnectionUrl.getConnectionUrlInstance("jdbc:mysql+srv:loadbalance://hostname?dnsSrv=true", null).getClass());
        assertEquals(LoadBalanceDnsSrvConnectionUrl.class,
                ConnectionUrl.getConnectionUrlInstance("jdbc:mysql+srv:loadbalance://hostname?dnsSrv=true", props).getClass());
        assertEquals(LoadBalanceDnsSrvConnectionUrl.class,
                ConnectionUrl.getConnectionUrlInstance("jdbc:mysql+srv:loadbalance://hostname?dnsSrv=false", props).getClass());
        assertEquals(LoadBalanceDnsSrvConnectionUrl.class,
                ConnectionUrl.getConnectionUrlInstance("jdbc:mysql:loadbalance://hostname?dnsSrv=true", null).getClass());
        assertEquals(LoadBalanceDnsSrvConnectionUrl.class, ConnectionUrl.getConnectionUrlInstance("jdbc:mysql:loadbalance://hostname", props).getClass());
        assertEquals(LoadBalanceDnsSrvConnectionUrl.class,
                ConnectionUrl.getConnectionUrlInstance("jdbc:mysql:loadbalance://hostname?dnsSrv=true", props).getClass());
        assertEquals(LoadBalanceDnsSrvConnectionUrl.class,
                ConnectionUrl.getConnectionUrlInstance("jdbc:mysql:loadbalance://hostname?dnsSrv=false", props).getClass());

        // Correct ConnectionUrl instances - jdbc:mysql:loadbalance:.
        props.setProperty(PropertyKey.dnsSrv.getKeyName(), "false");
        assertEquals(LoadBalanceConnectionUrl.class, ConnectionUrl.getConnectionUrlInstance("jdbc:mysql:loadbalance://hostname?dnsSrv=false", null).getClass());
        assertEquals(LoadBalanceConnectionUrl.class, ConnectionUrl.getConnectionUrlInstance("jdbc:mysql:loadbalance://hostname", props).getClass());
        assertEquals(LoadBalanceConnectionUrl.class,
                ConnectionUrl.getConnectionUrlInstance("jdbc:mysql:loadbalance://hostname?dnsArv=false", props).getClass());
        assertEquals(LoadBalanceConnectionUrl.class, ConnectionUrl.getConnectionUrlInstance("jdbc:mysql:loadbalance://hostname?dnsSrv=true", props).getClass());
        assertEquals(LoadBalanceConnectionUrl.class,
                ConnectionUrl.getConnectionUrlInstance("jdbc:mysql:loadbalance://hostname1,hostname2?dnsSrv=false", null).getClass());
        assertEquals(LoadBalanceConnectionUrl.class, ConnectionUrl.getConnectionUrlInstance("jdbc:mysql:loadbalance://hostname1,hostname2", props).getClass());
        assertEquals(LoadBalanceConnectionUrl.class,
                ConnectionUrl.getConnectionUrlInstance("jdbc:mysql:loadbalance://hostname1,hostname2?dnsArv=false", props).getClass());
        assertEquals(LoadBalanceConnectionUrl.class,
                ConnectionUrl.getConnectionUrlInstance("jdbc:mysql:loadbalance://hostname1,hostname2?dnsSrv=true", props).getClass());
    }

    /**
     * Tests jdbc:mysql+srv:replication: connection strings with deprecated types.
     */
    @Test
    public void testDeprecatedMysqlLoadReplicationDnsSrvConnectionUrl() {
        Properties props = new Properties();
        props.setProperty(PropertyKey.dnsSrv.getKeyName(), "true");

        // More than one host of the same type.
        assertThrows(InvalidConnectionAttributeException.class, "Specifying multiple host names for the same type with DNS SRV lookup is not allowed\\.",
                () -> ConnectionUrl.getConnectionUrlInstance(
                        "jdbc:mysql+srv:replication://(host=hostname1,type=master),(host=hostname2,type=master),(host=hostname3,type=slave)", null));
        assertThrows(InvalidConnectionAttributeException.class, "Specifying multiple host names for the same type with DNS SRV lookup is not allowed\\.",
                () -> ConnectionUrl.getConnectionUrlInstance(
                        "jdbc:mysql:replication://(host=hostname1,type=master),(host=hostname2,type=master),(host=hostname3,type=slave)?dnsSrv=true", null));
        assertThrows(InvalidConnectionAttributeException.class, "Specifying multiple host names for the same type with DNS SRV lookup is not allowed\\.",
                () -> ConnectionUrl.getConnectionUrlInstance(
                        "jdbc:mysql:replication://(host=hostname1,type=master),(host=hostname2,type=master),(host=hostname3,type=slave)", props));
        assertThrows(InvalidConnectionAttributeException.class, "Specifying multiple host names for the same type with DNS SRV lookup is not allowed\\.",
                () -> ConnectionUrl.getConnectionUrlInstance(
                        "jdbc:mysql+srv:replication://(host=hostname1,type=master),(host=hostname2,type=slave),(host=hostname3,type=slave)", null));
        assertThrows(InvalidConnectionAttributeException.class, "Specifying multiple host names for the same type with DNS SRV lookup is not allowed\\.",
                () -> ConnectionUrl.getConnectionUrlInstance(
                        "jdbc:mysql:replication://(host=hostname1,type=master),(host=hostname2,type=slave),(host=hostname3,type=slave)?dnsSrv=true", null));
        assertThrows(InvalidConnectionAttributeException.class, "Specifying multiple host names for the same type with DNS SRV lookup is not allowed\\.",
                () -> ConnectionUrl.getConnectionUrlInstance(
                        "jdbc:mysql:replication://(host=hostname1,type=master),(host=hostname2,type=slave),(host=hostname3,type=slave)", props));

        // Correct ConnectionUrl instances - jdbc:mysql:replication:.
        props.setProperty(PropertyKey.dnsSrv.getKeyName(), "false");
        assertEquals(ReplicationConnectionUrl.class, ConnectionUrl.getConnectionUrlInstance(
                "jdbc:mysql:replication://(host=hostname1,type=master)(host=hostname2,type=master)(host=hostname2,type=slave)(host=hostname4,type=slave)?dnsSrv=false",
                null).getClass());
        assertEquals(ReplicationConnectionUrl.class, ConnectionUrl.getConnectionUrlInstance(
                "jdbc:mysql:replication://(host=hostname1,type=master)(host=hostname2,type=master)(host=hostname2,type=slave)(host=hostname4,type=slave)",
                props).getClass());
        assertEquals(ReplicationConnectionUrl.class, ConnectionUrl.getConnectionUrlInstance(
                "jdbc:mysql:replication://(host=hostname1,type=master)(host=hostname2,type=master)(host=hostname2,type=slave)(host=hostname4,type=slave)?dnsArv=false",
                props).getClass());
        assertEquals(ReplicationConnectionUrl.class, ConnectionUrl.getConnectionUrlInstance(
                "jdbc:mysql:replication://(host=hostname1,type=master)(host=hostname2,type=master)(host=hostname2,type=slave)(host=hostname4,type=slave)?dnsSrv=true",
                props).getClass());
    }

    /**
     * Tests jdbc:mysql+srv:replication: connection strings.
     */
    @Test
    public void testMysqlLoadReplicationDnsSrvConnectionUrl() {
        Properties props = new Properties();
        props.setProperty(PropertyKey.dnsSrv.getKeyName(), "true");

        // Hosts missing.
        assertThrows(InvalidConnectionAttributeException.class,
                "Exactly two host names of different types are required for DNS SRV lookup enabled replication connections\\.",
                () -> ConnectionUrl.getConnectionUrlInstance("jdbc:mysql+srv:replication:", null));
        assertThrows(InvalidConnectionAttributeException.class,
                "Exactly two host names of different types are required for DNS SRV lookup enabled replication connections\\.",
                () -> ConnectionUrl.getConnectionUrlInstance("jdbc:mysql:replication:?dnsSrv=true", null));
        assertThrows(InvalidConnectionAttributeException.class,
                "Exactly two host names of different types are required for DNS SRV lookup enabled replication connections\\.",
                () -> ConnectionUrl.getConnectionUrlInstance("jdbc:mysql:replication:", props));

        // More than one host of the same type.
        assertThrows(InvalidConnectionAttributeException.class, "Specifying multiple host names for the same type with DNS SRV lookup is not allowed\\.",
                () -> ConnectionUrl.getConnectionUrlInstance("jdbc:mysql+srv:replication://hostname1,hostname2,hostname3", null));
        assertThrows(InvalidConnectionAttributeException.class, "Specifying multiple host names for the same type with DNS SRV lookup is not allowed\\.",
                () -> ConnectionUrl.getConnectionUrlInstance("jdbc:mysql:replication://hostname1,hostname2,hostname3?dnsSrv=true", null));
        assertThrows(InvalidConnectionAttributeException.class, "Specifying multiple host names for the same type with DNS SRV lookup is not allowed\\.",
                () -> ConnectionUrl.getConnectionUrlInstance("jdbc:mysql:replication://hostname1,hostname2,hostname3", props));
        assertThrows(InvalidConnectionAttributeException.class, "Specifying multiple host names for the same type with DNS SRV lookup is not allowed\\.",
                () -> ConnectionUrl.getConnectionUrlInstance(
                        "jdbc:mysql+srv:replication://(host=hostname1,type=source),(host=hostname2,type=source),(host=hostname3,type=replica)", null));
        assertThrows(InvalidConnectionAttributeException.class, "Specifying multiple host names for the same type with DNS SRV lookup is not allowed\\.",
                () -> ConnectionUrl.getConnectionUrlInstance(
                        "jdbc:mysql:replication://(host=hostname1,type=source),(host=hostname2,type=source),(host=hostname3,type=replica)?dnsSrv=true", null));
        assertThrows(InvalidConnectionAttributeException.class, "Specifying multiple host names for the same type with DNS SRV lookup is not allowed\\.",
                () -> ConnectionUrl.getConnectionUrlInstance(
                        "jdbc:mysql:replication://(host=hostname1,type=source),(host=hostname2,type=source),(host=hostname3,type=replica)", props));
        assertThrows(InvalidConnectionAttributeException.class, "Specifying multiple host names for the same type with DNS SRV lookup is not allowed\\.",
                () -> ConnectionUrl.getConnectionUrlInstance(
                        "jdbc:mysql+srv:replication://(host=hostname1,type=source),(host=hostname2,type=replica),(host=hostname3,type=replica)", null));
        assertThrows(InvalidConnectionAttributeException.class, "Specifying multiple host names for the same type with DNS SRV lookup is not allowed\\.",
                () -> ConnectionUrl.getConnectionUrlInstance(
                        "jdbc:mysql:replication://(host=hostname1,type=source),(host=hostname2,type=replica),(host=hostname3,type=replica)?dnsSrv=true", null));
        assertThrows(InvalidConnectionAttributeException.class, "Specifying multiple host names for the same type with DNS SRV lookup is not allowed\\.",
                () -> ConnectionUrl.getConnectionUrlInstance(
                        "jdbc:mysql:replication://(host=hostname1,type=source),(host=hostname2,type=replica),(host=hostname3,type=replica)", props));

        // Port specified.
        props.setProperty(PropertyKey.PORT.getKeyName(), "12345");
        assertThrows(InvalidConnectionAttributeException.class, "Specifying a port number with DNS SRV lookup is not allowed\\.",
                () -> ConnectionUrl.getConnectionUrlInstance("jdbc:mysql+srv:replication://hostname1:12345,hostname2", null));
        assertThrows(InvalidConnectionAttributeException.class, "Specifying a port number with DNS SRV lookup is not allowed\\.",
                () -> ConnectionUrl.getConnectionUrlInstance("jdbc:mysql+srv:replication://hostname1,hostname2:12345", null));
        assertThrows(InvalidConnectionAttributeException.class, "Specifying a port number with DNS SRV lookup is not allowed\\.",
                () -> ConnectionUrl.getConnectionUrlInstance("jdbc:mysql:replication://hostname1,hostname2?dnsSrv=true&port=12345", null));
        assertThrows(InvalidConnectionAttributeException.class, "Specifying a port number with DNS SRV lookup is not allowed\\.",
                () -> ConnectionUrl.getConnectionUrlInstance("jdbc:mysql:replication://hostname1,hostname2", props));
        props.remove(PropertyKey.PORT.getKeyName());

        // Conflicting options.
        props.setProperty(PropertyKey.dnsSrv.getKeyName(), "false");
        assertThrows(InvalidConnectionAttributeException.class, "'dnsSrv' cannot be set to false with DNS SRV lookup enabled\\.",
                () -> ConnectionUrl.getConnectionUrlInstance("jdbc:mysql+srv:replication://hostname1,hostname2?dnsSrv=false", null));
        assertThrows(InvalidConnectionAttributeException.class, "'dnsSrv' cannot be set to false with DNS SRV lookup enabled\\.",
                () -> ConnectionUrl.getConnectionUrlInstance("jdbc:mysql+srv:replication://hostname1,hostname2", props));
        props.setProperty(PropertyKey.dnsSrv.getKeyName(), "true");

        // Setting protocol=PIPE not allowed.
        props.setProperty(PropertyKey.PROTOCOL.getKeyName(), "pipe");
        assertThrows(InvalidConnectionAttributeException.class, "Using named pipes with DNS SRV lookup is not allowed\\.",
                () -> ConnectionUrl.getConnectionUrlInstance("jdbc:mysql+srv:replication://hostname1,hostname2?protocol=pipe", null));
        assertThrows(InvalidConnectionAttributeException.class, "Using named pipes with DNS SRV lookup is not allowed\\.",
                () -> ConnectionUrl.getConnectionUrlInstance("jdbc:mysql:replication://hostname1,hostname2?dnsSrv=true&protocol=pipe", null));
        assertThrows(InvalidConnectionAttributeException.class, "Using named pipes with DNS SRV lookup is not allowed\\.",
                () -> ConnectionUrl.getConnectionUrlInstance("jdbc:mysql:replication://hostname1,hostname2", props));
        props.remove(PropertyKey.PROTOCOL.getKeyName());

        // Setting replicationConnectionGroup not allowed.
        props.setProperty(PropertyKey.replicationConnectionGroup.getKeyName(), "lbgrp");
        assertThrows(InvalidConnectionAttributeException.class,
                "The option 'replicationConnectionGroup' cannot be set\\. Live management of connections is not supported with DNS SRV lookup\\.",
                () -> ConnectionUrl.getConnectionUrlInstance("jdbc:mysql+srv:replication://hostname1,hostname2?replicationConnectionGroup=lbgrp", null));
        assertThrows(InvalidConnectionAttributeException.class,
                "The option 'replicationConnectionGroup' cannot be set\\. Live management of connections is not supported with DNS SRV lookup\\.",
                () -> ConnectionUrl.getConnectionUrlInstance("jdbc:mysql:replication://hostname1,hostname2?dnsSrv=true&replicationConnectionGroup=lbgrp",
                        null));
        assertThrows(InvalidConnectionAttributeException.class,
                "The option 'replicationConnectionGroup' cannot be set\\. Live management of connections is not supported with DNS SRV lookup\\.",
                () -> ConnectionUrl.getConnectionUrlInstance("jdbc:mysql:replication://hostname1,hostname2", props));
        props.remove(PropertyKey.replicationConnectionGroup.getKeyName());

        // Resolving hosts fails.
        assertThrows(CJException.class, "Unable to locate any hosts for hostname1\\.",
                () -> ConnectionUrl.getConnectionUrlInstance("jdbc:mysql+srv:replication://hostname1,hostname2", null).getHostsList(HostsListView.SOURCES));
        assertThrows(CJException.class, "Unable to locate any hosts for hostname2\\.",
                () -> ConnectionUrl.getConnectionUrlInstance("jdbc:mysql+srv:replication://hostname1,hostname2", null).getHostsList(HostsListView.REPLICAS));

        // Correct ConnectionUrl instances - jdbc:mysql+srv:replication:.
        assertEquals(ReplicationDnsSrvConnectionUrl.class,
                ConnectionUrl.getConnectionUrlInstance("jdbc:mysql+srv:replication://hostname1,hostname2", null).getClass());
        assertEquals(ReplicationDnsSrvConnectionUrl.class,
                ConnectionUrl.getConnectionUrlInstance("jdbc:mysql+srv:replication://hostname1,hostname2?dnsSrv=true", null).getClass());
        assertEquals(ReplicationDnsSrvConnectionUrl.class,
                ConnectionUrl.getConnectionUrlInstance("jdbc:mysql+srv:replication://hostname1,hostname2?dnsSrv=true", props).getClass());
        assertEquals(ReplicationDnsSrvConnectionUrl.class,
                ConnectionUrl.getConnectionUrlInstance("jdbc:mysql+srv:replication://hostname1,hostname2?dnsSrv=false", props).getClass());
        assertEquals(ReplicationDnsSrvConnectionUrl.class,
                ConnectionUrl.getConnectionUrlInstance("jdbc:mysql:replication://hostname1,hostname2?dnsSrv=true", null).getClass());
        assertEquals(ReplicationDnsSrvConnectionUrl.class,
                ConnectionUrl.getConnectionUrlInstance("jdbc:mysql:replication://hostname1,hostname2", props).getClass());
        assertEquals(ReplicationDnsSrvConnectionUrl.class,
                ConnectionUrl.getConnectionUrlInstance("jdbc:mysql:replication://hostname1,hostname2?dnsSrv=true", props).getClass());
        assertEquals(ReplicationDnsSrvConnectionUrl.class,
                ConnectionUrl.getConnectionUrlInstance("jdbc:mysql:replication://hostname1,hostname2?dnsSrv=false", props).getClass());

        // Correct ConnectionUrl instances - jdbc:mysql:replication:.
        props.setProperty(PropertyKey.dnsSrv.getKeyName(), "false");
        assertEquals(ReplicationConnectionUrl.class,
                ConnectionUrl.getConnectionUrlInstance("jdbc:mysql:replication://hostname1,hostname2?dnsSrv=false", null).getClass());
        assertEquals(ReplicationConnectionUrl.class, ConnectionUrl.getConnectionUrlInstance("jdbc:mysql:replication://hostname1,hostname2", props).getClass());
        assertEquals(ReplicationConnectionUrl.class,
                ConnectionUrl.getConnectionUrlInstance("jdbc:mysql:replication://hostname1,hostname2?dnsArv=false", props).getClass());
        assertEquals(ReplicationConnectionUrl.class,
                ConnectionUrl.getConnectionUrlInstance("jdbc:mysql:replication://hostname1,hostname2?dnsSrv=true", props).getClass());
        assertEquals(ReplicationConnectionUrl.class, ConnectionUrl.getConnectionUrlInstance(
                "jdbc:mysql:replication://(host=hostname1,type=source)(host=hostname2,type=source)(host=hostname2,type=replica)(host=hostname4,type=replica)?dnsSrv=false",
                null).getClass());
        assertEquals(ReplicationConnectionUrl.class, ConnectionUrl.getConnectionUrlInstance(
                "jdbc:mysql:replication://(host=hostname1,type=source)(host=hostname2,type=source)(host=hostname2,type=replica)(host=hostname4,type=replica)",
                props).getClass());
        assertEquals(ReplicationConnectionUrl.class, ConnectionUrl.getConnectionUrlInstance(
                "jdbc:mysql:replication://(host=hostname1,type=source)(host=hostname2,type=source)(host=hostname2,type=replica)(host=hostname4,type=replica)?dnsArv=false",
                props).getClass());
        assertEquals(ReplicationConnectionUrl.class, ConnectionUrl.getConnectionUrlInstance(
                "jdbc:mysql:replication://(host=hostname1,type=source)(host=hostname2,type=source)(host=hostname2,type=replica)(host=hostname4,type=replica)?dnsSrv=true",
                props).getClass());
    }

    /**
     * Tests mysqlx+srv: connection strings.
     */
    @Test
    public void testMysqlxDnsSrvConnectionUrl() {
        Properties props;
        props = new Properties();
        props.setProperty(PropertyKey.xdevapiDnsSrv.getKeyName(), "true");

        // Host missing.
        assertThrows(InvalidConnectionAttributeException.class, "A host name is required for DNS SRV lookup enabled connections\\.",
                () -> ConnectionUrl.getConnectionUrlInstance("mysqlx+srv:", null));
        assertThrows(InvalidConnectionAttributeException.class, "A host name is required for DNS SRV lookup enabled connections\\.",
                () -> ConnectionUrl.getConnectionUrlInstance("mysqlx:?xdevapi.dns-srv=true", null));
        assertThrows(InvalidConnectionAttributeException.class, "A host name is required for DNS SRV lookup enabled connections\\.",
                () -> ConnectionUrl.getConnectionUrlInstance("mysqlx:", props));

        // More than one host.
        assertThrows(InvalidConnectionAttributeException.class, "Specifying multiple host names with DNS SRV lookup is not allowed\\.",
                () -> ConnectionUrl.getConnectionUrlInstance("mysqlx+srv://hostname1,hostname2", null));
        assertThrows(InvalidConnectionAttributeException.class, "Specifying multiple host names with DNS SRV lookup is not allowed\\.",
                () -> ConnectionUrl.getConnectionUrlInstance("mysqlx://hostname1,hostname2?xdevapi.dns-srv=true", null));
        assertThrows(InvalidConnectionAttributeException.class, "Specifying multiple host names with DNS SRV lookup is not allowed\\.",
                () -> ConnectionUrl.getConnectionUrlInstance("mysqlx://hostname1,hostname2", props));

        // Port specified.
        props.setProperty(PropertyKey.PORT.getKeyName(), "12345");
        assertThrows(InvalidConnectionAttributeException.class, "Specifying a port number with DNS SRV lookup is not allowed\\.",
                () -> ConnectionUrl.getConnectionUrlInstance("mysqlx+srv://hostname:12345", null));
        assertThrows(InvalidConnectionAttributeException.class, "Specifying a port number with DNS SRV lookup is not allowed\\.",
                () -> ConnectionUrl.getConnectionUrlInstance("mysqlx://hostname?xdevapi.dns-srv=true&port=12345", null));
        assertThrows(InvalidConnectionAttributeException.class, "Specifying a port number with DNS SRV lookup is not allowed\\.",
                () -> ConnectionUrl.getConnectionUrlInstance("mysqlx://hostname", props));
        props.remove(PropertyKey.PORT.getKeyName());

        // Conflicting options.
        props.setProperty(PropertyKey.xdevapiDnsSrv.getKeyName(), "false");
        assertThrows(InvalidConnectionAttributeException.class, "'xdevapi\\.dns-srv' cannot be set to false with DNS SRV lookup enabled\\.",
                () -> ConnectionUrl.getConnectionUrlInstance("mysqlx+srv://hostname?xdevapi.dns-srv=false", null));
        assertThrows(InvalidConnectionAttributeException.class, "'xdevapi\\.dns-srv' cannot be set to false with DNS SRV lookup enabled\\.",
                () -> ConnectionUrl.getConnectionUrlInstance("mysqlx+srv://hostname", props));
        props.setProperty(PropertyKey.xdevapiDnsSrv.getKeyName(), "true");

        // Resolving hosts fails.
        assertThrows(CJException.class, "Unable to locate any hosts for hostname\\.",
                () -> ConnectionUrl.getConnectionUrlInstance("mysqlx+srv://hostname", null).getHostsList());

        // Correct ConnectionUrl instances - mysqlx+srv:.
        assertEquals(XDevApiDnsSrvConnectionUrl.class, ConnectionUrl.getConnectionUrlInstance("mysqlx+srv://hostname", null).getClass());
        assertEquals(XDevApiDnsSrvConnectionUrl.class, ConnectionUrl.getConnectionUrlInstance("mysqlx+srv://hostname?xdevapi.dns-srv=true", null).getClass());
        assertEquals(XDevApiDnsSrvConnectionUrl.class, ConnectionUrl.getConnectionUrlInstance("mysqlx+srv://hostname?xdevapi.dns-srv=true", props).getClass());
        assertEquals(XDevApiDnsSrvConnectionUrl.class, ConnectionUrl.getConnectionUrlInstance("mysqlx+srv://hostname?xdevapi.dns-srv=false", props).getClass());
        assertEquals(XDevApiDnsSrvConnectionUrl.class, ConnectionUrl.getConnectionUrlInstance("mysqlx://hostname?xdevapi.dns-srv=true", null).getClass());
        assertEquals(XDevApiDnsSrvConnectionUrl.class, ConnectionUrl.getConnectionUrlInstance("mysqlx://hostname", props).getClass());
        assertEquals(XDevApiDnsSrvConnectionUrl.class, ConnectionUrl.getConnectionUrlInstance("mysqlx://hostname?xdevapi.dns-srv=true", props).getClass());
        assertEquals(XDevApiDnsSrvConnectionUrl.class, ConnectionUrl.getConnectionUrlInstance("mysqlx://hostname?xdevapi.dns-srv=false", props).getClass());

        // Correct ConnectionUrl instances - mysqlx:.
        props.setProperty(PropertyKey.xdevapiDnsSrv.getKeyName(), "false");
        assertEquals(XDevApiConnectionUrl.class, ConnectionUrl.getConnectionUrlInstance("mysqlx://hostname?xdevapi.dns-srv=false", null).getClass());
        assertEquals(XDevApiConnectionUrl.class, ConnectionUrl.getConnectionUrlInstance("mysqlx://hostname", props).getClass());
        assertEquals(XDevApiConnectionUrl.class, ConnectionUrl.getConnectionUrlInstance("mysqlx://hostname?xdevapi.dns-srv=false", props).getClass());
        assertEquals(XDevApiConnectionUrl.class, ConnectionUrl.getConnectionUrlInstance("mysqlx://hostname?xdevapi.dns-srv=true", props).getClass());
    }

    /**
     * Tests fix for BUG#28150662, CONNECTOR/J 8 MALFORMED DATABASE URL EXCEPTION WHIT CORRECT URL STRING.
     */
    @Test
    public void testBug28150662() {
        List<String> connStr = new ArrayList<>();
        connStr.add(
                "jdbc:mysql://localhost:3306/db1?connectionCollation=utf8mb4_unicode_ci&user=user1&sessionVariables=sql_mode='IGNORE_SPACE,ANSI',FOREIGN_KEY_CHECKS=0");
        connStr.add(
                "jdbc:mysql://localhost:3306/db1?connectionCollation=utf8mb4_unicode_ci&sessionVariables=sql_mode='IGNORE_SPACE,ANSI',FOREIGN_KEY_CHECKS=0&user=user1");
        connStr.add(
                "jdbc:mysql://address=(host=localhost)(port=3306)(connectionCollation=utf8mb4_unicode_ci)(sessionVariables=sql_mode='IGNORE_SPACE,ANSI',FOREIGN_KEY_CHECKS=0)(user=user1)/db1");
        connStr.add(
                "jdbc:mysql://(host=localhost,port=3306,connectionCollation=utf8mb4_unicode_ci,sessionVariables=sql_mode='IGNORE_SPACE%2CANSI'%2CFOREIGN_KEY_CHECKS=0,user=user1)/db1");

        connStr.add(
                "jdbc:mysql://verylonghostname01234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789:3306/db1?connectionCollation=utf8mb4_unicode_ci&user=user1&sessionVariables=sql_mode='IGNORE_SPACE,ANSI',FOREIGN_KEY_CHECKS=0");
        connStr.add(
                "jdbc:mysql://verylonghostname01234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789:3306/db1?connectionCollation=utf8mb4_unicode_ci&sessionVariables=sql_mode='IGNORE_SPACE,ANSI',FOREIGN_KEY_CHECKS=0&user=user1");
        connStr.add(
                "jdbc:mysql://address=(host=verylonghostname01234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789)(port=3306)(connectionCollation=utf8mb4_unicode_ci)(sessionVariables=sql_mode='IGNORE_SPACE,ANSI',FOREIGN_KEY_CHECKS=0)(user=user1)/db1");
        connStr.add(
                "jdbc:mysql://(host=verylonghostname01234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789,port=3306,connectionCollation=utf8mb4_unicode_ci,sessionVariables=sql_mode='IGNORE_SPACE%2CANSI'%2CFOREIGN_KEY_CHECKS=0,user=user1)/db1");

        for (String cs : connStr) {
            ConnectionUrl url = ConnectionUrl.getConnectionUrlInstance(cs, null);
            HostInfo hi = url.getMainHost();
            assertEquals("utf8mb4_unicode_ci", hi.getHostProperties().get("connectionCollation"));
            assertEquals("user1", hi.getUser());
            assertEquals("sql_mode='IGNORE_SPACE,ANSI',FOREIGN_KEY_CHECKS=0", hi.getHostProperties().get("sessionVariables"));
        }
    }
}
