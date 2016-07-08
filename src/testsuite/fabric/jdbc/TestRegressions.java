/*
  Copyright (c) 2014, 2016, Oracle and/or its affiliates. All rights reserved.

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

package testsuite.fabric.jdbc;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.concurrent.TimeUnit;

import com.mysql.fabric.FabricConnection;
import com.mysql.fabric.Server;
import com.mysql.fabric.jdbc.FabricMySQLConnection;
import com.mysql.fabric.jdbc.FabricMySQLDataSource;

import testsuite.fabric.BaseFabricTestCase;

/**
 * Testsuite for C/J Fabric regression tests.
 */
public class TestRegressions extends BaseFabricTestCase {
    private FabricMySQLConnection conn;

    public TestRegressions() throws Exception {
        super();
    }

    /**
     * Test for Bug#73070 - prepareCall() throws NPE
     * 
     * To test this, we create a basic stored procedure with a
     * parameter, call it and check the result.
     */
    public void testBug73070() throws Exception {
        if (!this.isSetForFabricTest) {
            return;
        }
        this.conn = (FabricMySQLConnection) getNewDefaultDataSource().getConnection(this.username, this.password);
        this.conn.setServerGroupName("fabric_test1_global");

        this.conn.createStatement().executeUpdate("drop procedure if exists bug73070");
        this.conn.createStatement().executeUpdate("create procedure bug73070(in x integer) select x");
        CallableStatement stmt = this.conn.prepareCall("{call bug73070(?)}");
        stmt.setInt(1, 42);
        ResultSet rs = stmt.executeQuery();
        rs.next();
        assertEquals(42, rs.getInt(1));
        rs.close();
        stmt.close();
        this.conn.createStatement().executeUpdate("drop procedure bug73070");

        this.conn.close();
    }

    /**
     * Test Bug#75080 - NPE when setting a timestamp on a Fabric connection
     */
    public void testBug75080() throws Exception {
        if (!this.isSetForFabricTest) {
            return;
        }

        class TestBugInternal {
            @SuppressWarnings("synthetic-access")
            void test(FabricMySQLDataSource ds) throws Exception {
                TestRegressions.this.conn = (FabricMySQLConnection) ds.getConnection(TestRegressions.this.username, TestRegressions.this.password);
                TestRegressions.this.conn.setServerGroupName("fabric_test1_global");

                PreparedStatement ps = TestRegressions.this.conn.prepareStatement("select ?");
                Timestamp ts = new Timestamp(System.currentTimeMillis());
                ps.setTimestamp(1, ts);
                ResultSet rs = ps.executeQuery();
                rs.next();
                Timestamp tsResult = rs.getTimestamp(1);
                assertEquals(ts, tsResult);
                rs.close();
                ps.close();
                TestRegressions.this.conn.close();
            }
        }

        FabricMySQLDataSource ds = getNewDefaultDataSource();

        // test includes both "legacy" and "new" datetime code
        ds.setUseLegacyDatetimeCode(false);
        new TestBugInternal().test(ds);
        ds.setUseLegacyDatetimeCode(true);
        new TestBugInternal().test(ds);
    }

    /**
     * Test Bug#77217 - ClassCastException when executing a PreparedStatement with Fabric (using a streaming result with timeout set)
     */
    public void testBug77217() throws Exception {
        if (!this.isSetForFabricTest) {
            return;
        }

        this.conn = (FabricMySQLConnection) getNewDefaultDataSource().getConnection(this.username, this.password);
        this.conn.setServerGroupName("ha_config1_group");

        PreparedStatement ps = this.conn.prepareStatement("select ? from dual");
        ps.setFetchSize(Integer.MIN_VALUE);
        ps.setString(1, "abc");
        ResultSet rs = ps.executeQuery();
        rs.next();
        assertEquals("abc", rs.getString(1));
        rs.close();
        ps.close();
        this.conn.close();
    }

    public void testBug21876798() throws Exception {
        if (!this.isSetForFabricTest) {
            return;
        }

        FabricMySQLDataSource ds = getNewDefaultDataSource();
        ds.setRewriteBatchedStatements(true);

        this.conn = (FabricMySQLConnection) ds.getConnection(this.username, this.password);
        this.conn.setServerGroupName("ha_config1_group");

        this.conn.createStatement().executeUpdate("drop table if exists bug21876798");
        this.conn.createStatement().executeUpdate("create table bug21876798(x varchar(100))");
        PreparedStatement ps = this.conn.prepareStatement("update bug21876798 set x = ?");
        ps.setString(1, "abc");
        ps.addBatch();
        ps.setString(1, "def");
        ps.addBatch();
        ps.setString(1, "def");
        ps.addBatch();
        ps.setString(1, "def");
        ps.addBatch();
        ps.setString(1, "def");
        ps.addBatch();
        // this would throw a ClassCastException
        ps.executeBatch();
    }

    /**
     * Test Bug#21296840 - CONNECTION DATA IS NOT UPDATED DURING FAILOVER.
     * Test Bug#17910835 - SERVER INFORMATION FROM FABRIC NOT REFRESHED WITH SHORTER TTL.
     * 
     * Test that the local cache is refreshed after expired TTL. This test connects to the master of "ha_config1_group" and requires the master to be changed
     * manually during the wait period. The Fabric must also be setup to communicate a TTL of less than 10s to the client.
     */
    public void manualTestRefreshFabricStateCache() throws Exception {
        if (!this.isSetForFabricTest) {
            return;
        }

        this.conn = (FabricMySQLConnection) getNewDefaultDataSource().getConnection(this.username, this.password);
        this.conn.setServerGroupName("ha_config1_group");
        this.conn.setReadOnly(false);
        this.conn.setAutoCommit(false);

        Statement stmt = this.conn.createStatement();

        ResultSet rs = stmt.executeQuery("show variables like 'server_uuid'");
        rs.next();
        String firstServerUuid = rs.getString(2);
        rs.close();
        this.conn.commit();

        // sleep for TTL+1 secs
        int seconds = 10;
        System.err.println("Waiting " + seconds + " seconds for new master to be chosen");
        Thread.sleep(TimeUnit.SECONDS.toMillis(1 + seconds));

        // force the LB proxy to pick a new connection
        this.conn.rollback();

        // verify change is seen by client
        rs = stmt.executeQuery("show variables like 'server_uuid'");
        rs.next();
        String secondServerUuid = rs.getString(2);
        rs.close();

        System.err.println("firstServerUuid=" + firstServerUuid + "\nsecondServerUuid=" + secondServerUuid);
        if (firstServerUuid.equals(secondServerUuid)) {
            fail("Server ID should change to reflect new topology");
        }

        this.conn.close();
    }

    /**
     * Test Bug#82094 - ConcurrentModificationException on Fabric connections after topology changes.
     * 
     * This test requires a Fabric instance running with a HA group (ha_config1_group) containing three servers, one promoted to master and failure detection
     * turned on.
     * Note that removing one or the other secondary server is a distinct case and only one of them causes the reported failure. This is so because of the order
     * the elements on the slave servers HashSet, from the ReplicationConnectionGroup object, are iterated. So, we remove one at a time in this test to make
     * sure we cover both cases.
     */
    public void testBug82094() throws Exception {
        if (!this.isSetForFabricTest) {
            return;
        }

        FabricMySQLDataSource ds = getNewDefaultDataSource();
        ds.setFabricServerGroup("ha_config1_group");
        this.conn = (FabricMySQLConnection) ds.getConnection(this.username, this.password);
        this.conn.createStatement().close(); // Make sure there is an internal ReplicationConnection.

        FabricConnection fabricConn = new FabricConnection(this.fabricUrl, this.fabricUsername, this.fabricPassword);

        for (Server server : fabricConn.getServerGroup("ha_config1_group").getServers()) {
            if (server.isSlave()) {
                try {
                    this.conn.transactionCompleted();

                    // Remove Secondary server.
                    fabricConn.getClient().removeServerFromGroup(server.getGroupName(), server.getHostname(), server.getPort());
                    // Make sure the TTL expires before moving on.
                    fabricConn.refreshState();
                    while (!fabricConn.isStateExpired()) {
                        Thread.sleep(1000);
                    }

                    this.conn.transactionCompleted();
                } finally {
                    // Add Secondary server back.
                    fabricConn.getClient().addServerToGroup(server.getGroupName(), server.getHostname(), server.getPort());
                    // Make sure the TTL expires before moving on.
                    fabricConn.refreshState();
                    while (!fabricConn.isStateExpired()) {
                        Thread.sleep(1000);
                    }
                }
            }
        }
        this.conn.close();
    }

    /**
     * Test Bug#22750465 - CONNECTOR/J HANGS WHEN FABRIC NODE IS DOWN.
     * 
     * This test connects to the master of "ha_config1_group" and requires the master to be changed manually during the first wait period. The fabric node must
     * be shut down during the second wait period. The Fabric must also be setup to communicate a TTL of less than 10s to the client.
     */
    public void manualTestBug22750465() throws Exception {
        this.conn = (FabricMySQLConnection) getNewDefaultDataSource().getConnection(this.username, this.password);
        this.conn.setServerGroupName("ha_config1_group");

        String initialMasterUuid = this.conn.getCurrentServerGroup().getMaster().getUuid();

        Statement stmt = this.conn.createStatement();
        ResultSet rs = stmt.executeQuery("SHOW VARIABLES LIKE 'server_uuid'");
        rs.next();
        String firstServerUuid = rs.getString(2);
        rs.close();

        assertEquals(initialMasterUuid, firstServerUuid);
        manualTestBug22750465SomeReadWriteOperations(this.conn);

        // Promote new primary server.
        int seconds = 10;
        System.err.println("Waiting " + seconds + " seconds for new master to be chosen (execute: 'mysqlfabric group promote ha_config1_group')");
        Thread.sleep(TimeUnit.SECONDS.toMillis(1 + seconds));

        rs = stmt.executeQuery("SHOW VARIABLES LIKE 'server_uuid'");
        rs.next();
        String secondServerUuid = rs.getString(2);
        rs.close();

        assertFalse(initialMasterUuid.equals(secondServerUuid));
        manualTestBug22750465SomeReadWriteOperations(this.conn);

        // Shutdown the Fabric node.
        System.err.println("Waiting " + seconds + " seconds for Fabric node shutdown (execute: 'mysqlfabric group manage stop')");
        Thread.sleep(TimeUnit.SECONDS.toMillis(1 + seconds));

        rs = stmt.executeQuery("SHOW VARIABLES LIKE 'server_uuid'");
        rs.next();
        String thirdServerUuid = rs.getString(2);
        rs.close();

        assertEquals(secondServerUuid, thirdServerUuid);
        manualTestBug22750465SomeReadWriteOperations(this.conn);

        this.conn.close();

        try {
            getNewDefaultDataSource().getConnection(this.username, this.password);
            fail("Exception was expected when trying to connect to a non-running Fabric node.");
        } catch (SQLException e) {
            assertEquals("Unable to establish connection to the Fabric server", e.getMessage());
        }
    }

    private void manualTestBug22750465SomeReadWriteOperations(Connection testConn) throws Exception {
        Statement stmt = testConn.createStatement();
        try {
            stmt.executeUpdate("CREATE TABLE IF NOT EXISTS testBug22750465 (id INT)");
            stmt.executeUpdate("INSERT INTO testBug22750465 VALUES (1)");
            ResultSet rs = stmt.executeQuery("SELECT * FROM testBug22750465");
            assertTrue(rs.next());
        } finally {
            stmt.executeUpdate("DROP TABLE IF EXISTS testBug22750465");
            stmt.close();
        }
    }
}
