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

package testsuite.mysqlx.devapi;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.junit.Before;
import org.junit.Test;

import com.mysql.cj.api.x.NodeSession;
import com.mysql.cj.api.x.XSession;
import com.mysql.cj.core.conf.url.ConnectionUrl;
import com.mysql.cj.core.exceptions.CJCommunicationsException;
import com.mysql.cj.core.exceptions.InvalidConnectionAttributeException;

import testsuite.mysqlx.internal.InternalMysqlxBaseTestCase;

/**
 * Tests for XSession client side failover features.
 */
public class XSessionFailoverTest extends InternalMysqlxBaseTestCase {
    private String testsHost = "";

    /**
     * Builds a connection string with the given hosts while setting priorities according to their positions.
     * 
     * @param hosts
     *            the hosts list, 1st has priority=100, 2nd has priority=99, and so on
     * @return a single host or a multi-host connection string
     */
    private String buildConnectionString(String... hosts) {
        StringBuilder url = new StringBuilder(ConnectionUrl.Type.MYSQLX_SESSION.getProtocol()).append("//");
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
        if (this.isSetForMySQLxTests) {
            StringBuilder sb = new StringBuilder();
            sb.append(getTestHost()).append(":").append(getTestPort());
            this.testsHost = sb.toString();
        }
    }

    /**
     * Assures that failover support doesn't affect single host connections.
     */
    @Test
    public void testGetSessionForSingleHost() throws Exception {
        if (!this.isSetForMySQLxTests) {
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
     * Tests basic failover while getting a {@link XSession} instance.
     */
    @Test
    public void testGetSessionForMultipleHostsWithFailover() throws Exception {
        if (!this.isSetForMySQLxTests) {
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

    /**
     * Tests {@link NodeSession} instance creation using multi-host URL.
     */
    @Test
    public void testGetNodeSessionForMultipleHosts() throws Exception {
        if (!this.isSetForMySQLxTests) {
            return;
        }

        ConnectionsCounterFakeServer fakeServer = new ConnectionsCounterFakeServer();
        String fakeHost = fakeServer.getHostPortPair();

        try {
            assertThrows(InvalidConnectionAttributeException.class, "A NodeSession cannot be initialized with a multi-host URL.",
                    () -> this.fact.getNodeSession(buildConnectionString(this.testsHost, fakeHost)));
            assertEquals(0, fakeServer.getAndResetConnectionsCounter());

            assertThrows(InvalidConnectionAttributeException.class, "A NodeSession cannot be initialized with a multi-host URL.",
                    () -> this.fact.getNodeSession(buildConnectionString(fakeHost, this.testsHost)));
            assertEquals(0, fakeServer.getAndResetConnectionsCounter());

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
                    clientSocket.close();
                }
            } catch (IOException e) {
                // Server socket closed.
            }
            return null;
        }
    }
}
