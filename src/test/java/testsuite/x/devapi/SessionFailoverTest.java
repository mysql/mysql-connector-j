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

package testsuite.x.devapi;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.io.InputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import com.mysql.cj.conf.ConnectionUrl;
import com.mysql.cj.conf.PropertyDefinitions;
import com.mysql.cj.conf.PropertyKey;
import com.mysql.cj.exceptions.CJCommunicationsException;
import com.mysql.cj.exceptions.WrongArgumentException;
import com.mysql.cj.protocol.x.XProtocolError;
import com.mysql.cj.xdevapi.Session;

/**
 * Tests for Session client side failover features.
 */
public class SessionFailoverTest extends DevApiBaseTestCase {
    private String testsHost = "";

    /**
     * Builds a connection string with the given hosts while setting priorities according to their positions.
     * 
     * @param hosts
     *            the hosts list, 1st has priority=100, 2nd has priority=99, and so on
     * @return a single host or a multi-host connection string
     */
    private String buildConnectionString(String... hosts) {
        StringBuilder url = new StringBuilder(ConnectionUrl.Type.XDEVAPI_SESSION.getScheme()).append("//");
        url.append(getTestUser()).append(":").append(getTestPassword()).append("@").append("[");
        String separator = "";
        int priority = 100;
        for (String h : hosts) {
            url.append(separator).append("(address=").append(h).append(",priority=").append(priority--).append(")");
            separator = ",";
        }
        url.append("]").append("/").append(getTestDatabase());
        return url.toString();
    }

    private String buildConnectionStringNoUser(String... hosts) {
        StringBuilder url = new StringBuilder(ConnectionUrl.Type.XDEVAPI_SESSION.getScheme()).append("//");
        url.append("[");
        String separator = "";
        int priority = 100;
        for (String h : hosts) {
            url.append(separator).append("(address=").append(h).append(",priority=").append(priority--).append(")");
            separator = ",";
        }
        url.append("]").append("/").append(getTestDatabase());
        return url.toString();
    }

    @BeforeEach
    public void setupFailoverTest() {
        if (this.isSetForXTests) {
            StringBuilder sb = new StringBuilder();
            sb.append(getEncodedTestHost()).append(":").append(getTestPort());
            this.testsHost = sb.toString();
        }
    }

    /**
     * Assures that failover support doesn't affect single host connections.
     * 
     * @throws Exception
     */
    @Test
    public void testGetSessionForSingleHost() throws Exception {
        if (!this.isSetForXTests) {
            return;
        }

        ConnectionsCounterFakeServer fakeServer = new ConnectionsCounterFakeServer();
        String fakeHost = fakeServer.getHostPortPair();

        try {
            this.fact.getSession(buildConnectionString(this.testsHost)).close();
            assertThrows(CJCommunicationsException.class, ".*", () -> this.fact.getSession(buildConnectionString(fakeHost)));
            assertEquals(1, fakeServer.getAndResetConnectionsCounter());
        } finally {
            fakeServer.shutdownSilently();
        }
    }

    /**
     * Tests basic failover while getting a {@link Session} instance.
     * 
     * @throws Exception
     */
    @Test
    public void testGetSessionForMultipleHostsWithFailover() throws Exception {
        if (!this.isSetForXTests) {
            return;
        }

        ConnectionsCounterFakeServer fakeServer = new ConnectionsCounterFakeServer();
        String fakeHost = fakeServer.getHostPortPair();

        try {
            this.fact.getSession(buildConnectionString(this.testsHost, fakeHost, fakeHost, fakeHost)).close();
            assertEquals(0, fakeServer.getAndResetConnectionsCounter());

            this.fact.getSession(buildConnectionString(fakeHost, this.testsHost, fakeHost, fakeHost)).close();
            assertEquals(1, fakeServer.getAndResetConnectionsCounter());

            this.fact.getSession(buildConnectionString(fakeHost, fakeHost, this.testsHost, fakeHost)).close();
            assertEquals(2, fakeServer.getAndResetConnectionsCounter());

            this.fact.getSession(buildConnectionString(fakeHost, fakeHost, fakeHost, this.testsHost)).close();
            assertEquals(3, fakeServer.getAndResetConnectionsCounter());

            assertThrows(CJCommunicationsException.class, ".*", () -> this.fact.getSession(buildConnectionString(fakeHost, fakeHost, fakeHost, fakeHost)));
            assertEquals(4, fakeServer.getAndResetConnectionsCounter());
        } finally {
            fakeServer.shutdownSilently();
        }
    }

    /*
     * A fake server that counts how many connection attempts were made.
     */
    private class ConnectionsCounterFakeServer implements Callable<Void> {
        ExecutorService executor = null;
        ServerSocket serverSocket = null;
        int connectionsCounter = 0;

        ConnectionsCounterFakeServer() throws IOException {
            this.serverSocket = new ServerSocket(0);
            this.executor = Executors.newSingleThreadExecutor();
            this.executor.submit(this);
        }

        String getHostPortPair() throws IOException {
            return "localhost:" + this.serverSocket.getLocalPort();
        }

        int getAndResetConnectionsCounter() {
            int c = this.connectionsCounter;
            this.connectionsCounter = 0;
            return c;
        }

        void shutdownSilently() {
            try {
                this.serverSocket.close();
                this.executor.shutdown();
            } catch (Exception e) {
                // Swallow this exception.
            }
        }

        @Override
        public Void call() {
            try {
                for (;;) {
                    Socket clientSocket = this.serverSocket.accept();
                    this.connectionsCounter++;
                    InputStream is = clientSocket.getInputStream();
                    is.read(new byte[100]);
                    clientSocket.close();
                }
            } catch (IOException e) {
                // Server socket closed.
            }
            return null;
        }
    }

    /**
     * Tests xdevapi.connect-timeout and connectTimeout functionality.
     * 
     * The real socket connect timeout can be revealed only when trying to connect to the unavailable remote host
     * pointed by IP address. Neither localhost IP nor domain names are working, they fail much faster then the timeout
     * is reached.
     * If default 10.77.77.77:37070 doesn't work in a particular testing setup (if the ip address is available)
     * please add this variable to ant call:
     * -Dcom.mysql.cj.testsuite.unavailable.host=unavailable_ip:port
     * 
     * @throws Exception
     */
    @Test
    @Disabled("This test doesn't execute deterministically on some systems. It can be run manually in local systems when needed.")
    public void testConnectionTimeout() throws Exception {
        if (!this.isSetForXTests) {
            return;
        }

        String customFakeHost = System.getProperty(PropertyDefinitions.SYSP_testsuite_unavailable_host);
        String fakeHost = (customFakeHost != null && customFakeHost.trim().length() != 0) ? customFakeHost : "10.77.77.77:37070";

        // TS1_1 Create a session to a Server using explicit "xdevapi.connect-timeout" overriding implicit "connectTimeout".
        testConnectionTimeout_assertFailureTimeout(buildConnectionString(fakeHost) + "?" + makeParam(PropertyKey.xdevapiConnectTimeout, "500", true), 500,
                1500);

        // TS1_2 Create a session to a Server using explicit "xdevapi.connect-timeout" overriding explicit "connectTimeout".
        testConnectionTimeout_assertFailureTimeout(buildConnectionString(fakeHost) + "?" + makeParam(PropertyKey.xdevapiConnectTimeout, "500", true)
                + makeParam(PropertyKey.connectTimeout, "8000"), 500, 1500);

        // TS1_3 Create a session to a Server using explicit "connectTimeout" overriding implicit "xdevapi.connect-timeout".
        testConnectionTimeout_assertFailureTimeout(buildConnectionString(fakeHost) + "?" + makeParam(PropertyKey.connectTimeout, "800", true), 800, 1800);

        // TS3_1 Create a session to a remote offline host not setting the "connect-timeout" parameter. The connection must timeout in ~10 seconds.
        // Default "connect-timeout" (10000 ms) overrides implicit "connectTimeout".
        testConnectionTimeout_assertFailureTimeout(buildConnectionString(fakeHost), 10000, 11000);

        // TS4_1 Create a session to a remote offline host setting "connect-timeout" to zero (0). The connection must not timeout until cancelled.
        testConnectionTimeout_assertFailureTimeout(buildConnectionString(fakeHost) + "?" + makeParam(PropertyKey.xdevapiConnectTimeout, "0", true), 12000,
                600000);

        // TS6_1 Create a session using the fail over functionality passing two different Server addresses.
        // The Server with the higher priority must be offline. The connection must succeed after connect-timeout milliseconds.
        testConnectionTimeout_assertSuccessTimeout(
                buildConnectionString(fakeHost, this.testsHost) + "?" + makeParam(PropertyKey.xdevapiConnectTimeout, "1000", true), 1000, 2000);

        // TS6_2 Create a session using the fail over functionality passing two different Server addresses.
        // Both Servers must be offline. The connection must time out after connect-timeout * 2 milliseconds.
        testConnectionTimeout_assertFailureTimeout(buildConnectionString(fakeHost, fakeHost) + "?" + makeParam(PropertyKey.xdevapiConnectTimeout, "500", true),
                1000, 2000);

        // TS8_1 Create a session to a Server using valid credentials passing the "connect-timeout" and set it to a valid value.
        // Call the function SLEEP() and set it to 10 seconds once the connections is established. No timeout exception/error must be displayed.
        long begin = System.currentTimeMillis();
        Session sess = this.fact.getSession(buildConnectionString(this.testsHost) + "?" + makeParam(PropertyKey.xdevapiConnectTimeout, "3000", true));
        sess.sql("SELECT SLEEP(11)").execute();
        long end = System.currentTimeMillis() - begin;
        assertTrue(end >= 11000 && end < 12000, "Expected: " + 11000 + ".." + 12000 + ". Got " + end);

        // TS11_1 Set connection property xdevapi.connect-timeout=null, try to create Session, check that WrongArgumentException is thrown
        // with message "The connection property 'xdevapi.connect-timeout' only accepts integer values. The value 'null' can not be converted to an integer."
        assertThrows(WrongArgumentException.class,
                "The connection property 'xdevapi.connect-timeout' only accepts integer values. The value 'null' can not be converted to an integer.",
                () -> this.fact.getSession(buildConnectionString(fakeHost, this.testsHost) + "?" + makeParam(PropertyKey.xdevapiConnectTimeout, "null", true)));

        // TS11_2 Set connection property xdevapi.connect-timeout=-1, try to create Session, check that WrongArgumentException is thrown with
        // message "The connection property 'xdevapi.connect-timeout' only accepts integer values in the range of 0 - 2147483647, the value '-1' exceeds this range."
        assertThrows(WrongArgumentException.class,
                "The connection property 'xdevapi.connect-timeout' only accepts integer values in the range of 0 - 2147483647, the value '-1' exceeds this range.",
                () -> this.fact.getSession(buildConnectionString(fakeHost, this.testsHost) + "?" + makeParam(PropertyKey.xdevapiConnectTimeout, "-1", true)));

        // TS11_3 Set connection property xdevapi.connect-timeout=abc, try to create Session, check that WrongArgumentException is thrown with
        // message "The connection property 'xdevapi.connect-timeout' only accepts integer values. The value 'abc' can not be converted to an integer."
        assertThrows(WrongArgumentException.class,
                "The connection property 'xdevapi.connect-timeout' only accepts integer values. The value 'abc' can not be converted to an integer.",
                () -> this.fact.getSession(buildConnectionString(fakeHost, this.testsHost) + "?" + makeParam(PropertyKey.xdevapiConnectTimeout, "abc", true)));

        // TS11_4 Set connection property xdevapi.connect-timeout=, try to create Session, check that WrongArgumentException is thrown with
        // message "The connection property 'xdevapi.connect-timeout' only accepts integer values. The value '' can not be converted to an integer."
        assertThrows(WrongArgumentException.class,
                "The connection property 'xdevapi.connect-timeout' only accepts integer values. The value '' can not be converted to an integer.",
                () -> this.fact.getSession(buildConnectionString(fakeHost, this.testsHost) + "?" + makeParam(PropertyKey.xdevapiConnectTimeout, "", true)));

        // TS11_5 Set connection property xdevapi.connect-timeout=12.8. Please note that c/J truncates decimals w/o exception for integer parameters thus
        // the error message is not thrown against the property value. Try to connect with this connection string and ensure that CJCommunicationsException
        // is thrown not earlier than 12 ms and not later than 1000 ms.
        testConnectionTimeout_assertFailureTimeout(buildConnectionString(fakeHost) + "?" + makeParam(PropertyKey.xdevapiConnectTimeout, "12.8", true), 12,
                1000);

        // TS12_1 Create a session to a Server giving a valid value for the "connect-timeout", and use invalid credentials.
        testConnectionTimeout_assertFailureTimeout(buildConnectionStringNoUser(this.testsHost) + "?"
                + makeParam(PropertyKey.xdevapiConnectTimeout, "1000", true) + makeParam(PropertyKey.USER, "nosuchuser"), 0, 1000, XProtocolError.class);
    }

    private <EX extends Throwable> void testConnectionTimeout_assertFailureTimeout(String url, int expLowLimit, int expUpLimit, Class<EX> throwable) {
        long begin = System.currentTimeMillis();
        assertThrows(throwable, () -> this.fact.getSession(url));
        long end = System.currentTimeMillis() - begin;
        assertTrue(end >= expLowLimit && end < expUpLimit, "Expected: " + expLowLimit + ".." + expUpLimit + ". Got " + end);
    }

    private void testConnectionTimeout_assertFailureTimeout(String url, int expLowLimit, int expUpLimit) {
        testConnectionTimeout_assertFailureTimeout(url, expLowLimit, expUpLimit, CJCommunicationsException.class);
    }

    private void testConnectionTimeout_assertSuccessTimeout(String url, int expLowLimit, int expUpLimit) {
        long begin = System.currentTimeMillis();
        this.fact.getSession(url);
        long end = System.currentTimeMillis() - begin;
        assertTrue(end >= expLowLimit && end < expUpLimit, "Expected: " + expLowLimit + ".." + expUpLimit + ". Got " + end);
    }
}
