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

package com.mysql.cj.core;

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

import org.junit.Test;

import com.mysql.cj.api.conf.ConnectionPropertiesTransform;
import com.mysql.cj.core.conf.PropertyDefinitions;
import com.mysql.cj.core.conf.url.ConnectionUrlParser;
import com.mysql.cj.core.conf.url.HostInfo;
import com.mysql.cj.core.conf.url.ConnectionUrl;
import com.mysql.cj.core.exceptions.WrongArgumentException;

public class ConnectionUrlTest {
    /**
     * Internal class for generating hundreds of thousands of connection strings.
     */
    private static class ConnectionStringGenerator implements Iterator<String>, Iterable<String> {
        private static final String[] PROTOCOL = new String[] { "jdbc:mysql:", "mysqlx:" };
        private static final String[] USER = new String[] { "", "@", "user@", "user:@", "user:pwd@", ":pwd@", ":@" };
        private static final String[] STD_HOST = new String[] { "", "myhost", "192.168.0.1", "[1000:abcd::1]" };
        private static final String[] STD_PORT = new String[] { "", ":", ":1234" };
        private static final String[] ALT_HOST = new String[] { "address=", "address=(flag)", "address=(protocol=tcp)(host=myhost)(port=1234)",
                "address=(protocol=tcp)(host=myhost)(port=1234)(key=value%28%29)" };
        private static final String[] HOST;

        static {
            int i = 0;
            String[] hosts = new String[STD_HOST.length * STD_PORT.length + ALT_HOST.length];
            for (String h : STD_HOST) {
                for (String p : STD_PORT) {
                    hosts[i++] = h + p;
                }
            }
            for (String h : ALT_HOST) {
                hosts[i++] = h;
            }
            HOST = hosts;
        }

        private static final String[] DB = new String[] { "", "/", "/mysql" };
        private static final String[] PARAMS = new String[] { "", "?", "?key=value&flag", "?key=value%26&flag&26", "?file=%2Fpath%2Fto%2Ffile&flag&key=value",
                "?file=(/path/to/file)&flag&key=value" };

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
        public ConnectionStringGenerator(int numberOfHosts) {
            this.numberOfHosts = numberOfHosts;
            this.current = new int[3 + numberOfHosts * 2];
            this.next = new int[3 + numberOfHosts * 2];
            this.ceiling = new int[3 + numberOfHosts * 2];
            this.ceiling[0] = PROTOCOL.length;
            for (int i = 0; i < numberOfHosts; i++) {
                this.ceiling[1 + i * 2] = USER.length;
                this.ceiling[1 + i * 2 + 1] = HOST.length;
            }
            this.ceiling[1 + numberOfHosts * 2] = DB.length;
            this.ceiling[2 + numberOfHosts * 2] = PARAMS.length;
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
            sb.append(PROTOCOL[this.current[0]]).append("//");
            for (int i = 0; i < this.numberOfHosts; i++) {
                if (i != 0) {
                    sb.append(",");
                }
                sb.append(USER[this.current[1 + i * 2]]);
                sb.append(HOST[this.current[1 + i * 2 + 1]]);
            }
            sb.append(DB[this.current[1 + this.numberOfHosts * 2]]);
            sb.append(PARAMS[this.current[2 + this.numberOfHosts * 2]]);
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
            return PROTOCOL[this.current[0]];
        }

        /**
         * Returns the user info part for the current position and the given host.
         * 
         * @param fromHostIndex
         *            the host from where to get user info
         * @return the user info part
         */
        public String getUserInfo(int fromHostIndex) {
            if (fromHostIndex <= 0 || fromHostIndex - 1 > this.numberOfHosts) {
                throw new IllegalArgumentException("Argument \"fromHostIndex\" out of bounds.");
            }
            fromHostIndex--;
            return USER[this.current[1 + fromHostIndex * 2]];
        }

        /**
         * Returns the host info part for the current position and the given host.
         * 
         * @param fromHostIndex
         *            the host from where to get host info
         * @return the host info part
         */
        public String getHostInfo(int fromHostIndex) {
            if (fromHostIndex <= 0 || fromHostIndex - 1 > this.numberOfHosts) {
                throw new IllegalArgumentException("Argument \"fromHostIndex\" out of bounds.");
            }
            fromHostIndex--;
            return HOST[this.current[1 + fromHostIndex * 2 + 1]];
        }

        /**
         * Returns the database part for the current position.
         * 
         * @return the database part
         */
        public String getDatabase() {
            return DB[this.current[1 + this.numberOfHosts * 2]];
        }

        /**
         * Returns the connection parameters part for the current position.
         * 
         * @return the connection parameter part
         */
        public String getParams() {
            return PARAMS[this.current[2 + this.numberOfHosts * 2]];
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
            StringBuilder sbKv = new StringBuilder("(");
            sbKv.append(key);
            if (value != null) {
                sbKv.append("=").append(value);
            }
            sbKv.append(")");
            return getHostInfo(hostIndex).indexOf(sbKv.toString()) != -1 || decode(getHostInfo(hostIndex)).indexOf(sbKv.toString()) != -1;
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
            if (hi.startsWith("address=")) {
                hi = hi.substring(8);
                return hi.isEmpty() ? 0 : hi.split("\\)\\(").length;
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
        assertEquals(ConnectionUrl.Type.FABRIC_CONNECTION, ConnectionUrl.Type.fromValue("jdbc:mysql:fabric:", 1));
        assertEquals(ConnectionUrl.Type.MYSQLX_SESSION, ConnectionUrl.Type.fromValue("mysqlx:", 1));
    }

    /**
     * Checks the expected exception from an incorrect usage of {@link ConnectionUrl.Type#fromValue(String, int)}.
     */
    @Test(expected = WrongArgumentException.class)
    public void testTypeEnumWrongFabricValue() {
        ConnectionUrl.Type.fromValue("jdbc:mysql:fabric:", 2);
    }

    /**
     * Checks the expected exception from an incorrect usage of {@link ConnectionUrl.Type#fromValue(String, int)}.
     */
    @Test(expected = WrongArgumentException.class)
    public void testTypeEnumWrongMysqlxValue() {
        ConnectionUrl.Type.fromValue("mysqlx:", 2);
    }

    /**
     * Tests the {@link ConnectionUrlParser} with hundreds of thousands of different connection string variations.
     */
    @Test
    public void testConnectionStringParser() {
        for (int hostsCount = 1; hostsCount <= 2; hostsCount++) {
            ConnectionStringGenerator csg = new ConnectionStringGenerator(hostsCount);
            for (String cs : csg) {
                ConnectionUrlParser csp = ConnectionUrlParser.parseConnectionString(cs);
                String expected;
                String actual;
                // Protocol:
                assertEquals(cs, csg.getProtocol(), csp.getScheme());
                // User & Host:
                for (int hostIndex = 1; hostIndex <= 2; hostIndex++) {
                    HostInfo hi = csp.getHosts().get(hostIndex - 1);
                    // User(n):
                    expected = testCSParserTrimTail(testCSParserTrimHead(csg.getUserInfo(hostIndex), ":"), "@", ":");
                    actual = new StringBuilder(hi.getUser() == null ? "" : hi.getUser()).append(":").append(hi.getPassword() == null ? "" : hi.getPassword())
                            .toString();
                    actual = testCSParserTrimTail(testCSParserTrimHead(actual, ":"), ":");
                    assertEquals(cs, expected, actual);
                    if (!csg.getHostInfo(hostIndex).startsWith("address=")) {
                        // Host(n)
                        expected = testCSParserTrimTail(testCSParserTrimHead(csg.getHostInfo(hostIndex), ":"), ":");
                        actual = new StringBuilder(hi.getHost() == null ? "" : hi.getHost()).append(":").append(hi.getPort() == -1 ? "" : hi.getPort())
                                .toString();
                        actual = testCSParserTrimTail(testCSParserTrimHead(actual, ":"), ":");
                        assertEquals(cs, expected, actual);
                    } else {
                        // Host props(n):
                        assertEquals(cs, csg.getHostParamsCount(hostIndex), hi.getHostProperties().size());
                        for (Entry<String, String> kv : hi.getHostProperties().entrySet()) {
                            assertTrue(cs, csg.hasHostParam(hostIndex, kv.getKey(), kv.getValue()));
                        }
                    }
                    hostIndex++;
                }
                // Database:
                expected = testCSParserTrimHead(csg.getDatabase(), "/");
                actual = csp.getPath() == null ? "" : testCSParserTrimHead(csp.getPath(), "/");
                assertEquals(cs, expected, actual);
                // Connection arguments:
                assertEquals(cs, csg.getParamsCount(), csp.getProperties().size());
                for (Entry<String, String> kv : csp.getProperties().entrySet()) {
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
        assertTrue(ConnectionUrl.acceptsUrl("jdbc:mysql:fabric:"));
        assertTrue(ConnectionUrl.acceptsUrl("jdbc:mysql:fabric://somehost:1234/db?key=value"));
        assertTrue(ConnectionUrl.acceptsUrl("jdbc:mysql:"));
        assertTrue(ConnectionUrl.acceptsUrl("mysqlx://somehost:1234/db?key=value"));
        assertTrue(ConnectionUrl.acceptsUrl("mysqlx://127.0.0.1:1234/db?key=value"));
        assertTrue(ConnectionUrl.acceptsUrl("mysqlx://[::1]:1234/db?key=value"));
        assertTrue(ConnectionUrl.acceptsUrl("mysqlx://[fe80::250:56ff:fec0:8]:1234/db?key=value"));

        // Non-supported URLs:
        assertFalse(ConnectionUrl.acceptsUrl("jdbc:"));
        assertFalse(ConnectionUrl.acceptsUrl("jdbc://somehost:1234/db?key=value"));
        assertFalse(ConnectionUrl.acceptsUrl("mysql:"));
        assertFalse(ConnectionUrl.acceptsUrl("mysql://somehost:1234/db?key=value"));
        assertFalse(ConnectionUrl.acceptsUrl("mysql:jdbc:"));
        assertFalse(ConnectionUrl.acceptsUrl("mysql:jdbc://somehost:1234/db?key=value"));
        assertFalse(ConnectionUrl.acceptsUrl("loadbalance:"));
        assertFalse(ConnectionUrl.acceptsUrl("loadbalance://somehost:1234/db?key=value"));
        assertFalse(ConnectionUrl.acceptsUrl("jdbc:loadbalance:"));
        assertFalse(ConnectionUrl.acceptsUrl("jdbc:loadbalance://somehost:1234/db?key=value"));
        assertFalse(ConnectionUrl.acceptsUrl("jdbc:replication:"));
        assertFalse(ConnectionUrl.acceptsUrl("jdbc:replication://somehost:1234/db?key=value"));
        assertFalse(ConnectionUrl.acceptsUrl("replication:"));
        assertFalse(ConnectionUrl.acceptsUrl("replication://somehost:1234/db?key=value"));
        assertFalse(ConnectionUrl.acceptsUrl("fabric:"));
        assertFalse(ConnectionUrl.acceptsUrl("fabric://somehost:1234/db?key=value"));
        assertFalse(ConnectionUrl.acceptsUrl("jdbc:mysql:unknown:"));
        assertFalse(ConnectionUrl.acceptsUrl("jdbc:mysql:unknown://somehost:1234/db?key=value"));
        assertFalse(ConnectionUrl.acceptsUrl("mysql-x:"));
        assertFalse(ConnectionUrl.acceptsUrl("mysql-x://somehost:1234/db?key=value"));
        assertFalse(ConnectionUrl.acceptsUrl(""));
        assertFalse(ConnectionUrl.acceptsUrl("//somehost:1234/db?key=value"));
    }

    /**
     * Tests the {@link ConnectionUrl} with hundreds of thousands of different connection string variations.
     */
    @Test
    public void testConnectionUrl() {
        Properties props = new Properties();
        props.setProperty("propKey", "propValue");

        for (int hostsCount = 1; hostsCount <= 2; hostsCount++) {
            ConnectionStringGenerator csg = new ConnectionStringGenerator(hostsCount);
            for (String cs : csg) {
                try {
                    ConnectionUrl.getConnectionUrlInstance(cs, props);
                    if (csg.getProtocol().equals("mysqlx:") && hostsCount == 2) {
                        fail("WrongArgumentException expected");
                    }
                } catch (WrongArgumentException e) {
                    assertEquals("Connector/J cannot handle a database URL of type 'mysqlx:' that takes 2 hosts.", e.getMessage());
                }
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
        connStr.put("jdbc:mysql:,", 3306);
        connStr.put("jdbc:mysql:loadbalance:,", 3306);
        connStr.put("jdbc:mysql:replication:,", 3306);
        connStr.put("jdbc:mysql:fabric:", 32274);
        connStr.put("mysqlx:", 33060);

        for (String cs : connStr.keySet()) {
            ConnectionUrl connUrl = ConnectionUrl.getConnectionUrlInstance(cs, null);
            for (HostInfo hi : connUrl.getHostList()) {
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
    /**
     * Tests loading properties from config files.
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
}
