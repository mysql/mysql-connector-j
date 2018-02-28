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

package testsuite.x.devapi;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.io.InputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.junit.Before;
import org.junit.Test;

import com.mysql.cj.conf.ConnectionUrl;
import com.mysql.cj.exceptions.CJCommunicationsException;
import com.mysql.cj.xdevapi.Session;

import testsuite.x.internal.InternalXBaseTestCase;

/**
 * Tests for Session client side failover features.
 */
public class SessionFailoverTest extends InternalXBaseTestCase {
    private String testsHost = "";

    /**
     * Builds a connection string with the given hosts while setting priorities according to their positions.
     * 
     * @param hosts
     *            the hosts list, 1st has priority=100, 2nd has priority=99, and so on
     * @return a single host or a multi-host connection string
     */
    private String buildConnectionString(String... hosts) {
        StringBuilder url = new StringBuilder(ConnectionUrl.Type.XDEVAPI_SESSION.getProtocol()).append("//");
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

    @Before
    public void setupFailoverTest() {
        if (this.isSetForXTests) {
            StringBuilder sb = new StringBuilder();
            sb.append(getEncodedTestHost()).append(":").append(getTestPort());
            this.testsHost = sb.toString();
        }
    }

    /**
     * Assures that failover support doesn't affect single host connections.
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
}
