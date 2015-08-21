/*
  Copyright (c) 2014, 2015, Oracle and/or its affiliates. All rights reserved.

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

import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.SQLException;

import testsuite.fabric.BaseFabricTestCase;

import com.mysql.fabric.jdbc.FabricMySQLConnection;
import com.mysql.fabric.jdbc.FabricMySQLDataSource;

/**
 * TODO: document required setup for this test
 */
public class TestHABasics extends BaseFabricTestCase {
    private FabricMySQLDataSource ds;
    private FabricMySQLConnection conn;
    private String masterPort = System.getProperty("com.mysql.fabric.testsuite.global.port");

    public TestHABasics() throws Exception {
        super();
        if (this.isSetForFabricTest) {
            this.ds = getNewDefaultDataSource();
        }
    }

    @Override
    public void setUp() throws Exception {
        if (this.isSetForFabricTest) {
            this.conn = (FabricMySQLConnection) this.ds.getConnection(this.username, this.password);
            this.conn.setServerGroupName("ha_config1_group");
        }
    }

    @Override
    public void tearDown() throws Exception {
        if (this.isSetForFabricTest) {
            this.conn.close();
        }
    }

    private String getPort() throws Exception {
        ResultSet rs = this.conn.createStatement().executeQuery("show variables like 'port'");
        assertTrue(rs.next());
        String port1 = rs.getString(2);
        rs.close();
        return port1;
    }

    /**
     * Test that writes go to the master and reads go to the slave(s).
     */
    public void testReadWriteSplitting() throws Exception {
        if (!this.isSetForFabricTest) {
            return;
        }
        // make sure we start on the master
        assertEquals(this.masterPort, getPort());

        Statement s = this.conn.createStatement();
        s.executeUpdate("drop table if exists fruits");
        s.executeUpdate("create table fruits (name varchar(30))");
        s.executeUpdate("insert into fruits values ('Rambutan'), ('Starfruit')");

        // go to the slave and verify
        this.conn.setReadOnly(true);
        assertTrue(!this.masterPort.equals(getPort()));

        // allow a little replication lag and check for data
        Thread.sleep(3000);
        ResultSet rs = s.executeQuery("select name from fruits order by 1");
        assertTrue(rs.next());
        assertEquals("Rambutan", rs.getString(1));
        assertTrue(rs.next());
        assertEquals("Starfruit", rs.getString(1));
        assertFalse(rs.next());
        rs.close();

        this.conn.setReadOnly(false);
        s.executeUpdate("drop table fruits");
    }

    /**
     * Test for failover that requires manual intervention.
     */
    public void manualTestFailover() throws Exception {
        Statement s = this.conn.createStatement();
        ResultSet rs = s.executeQuery("show variables like 'port'");
        rs.next();
        System.err.println("Starting master Port: " + rs.getString(2));
        rs.close();

        int secs = 15000;
        System.err.println("Sleeping " + (secs / 1000.0) + " seconds.... Please perform manual failover");
        Thread.sleep(secs);
        System.err.println("Continuing");
        try {
            s.executeQuery("show variables like 'port'");
            fail("Master should be unavailable");
        } catch(SQLException ex) {
            rs = s.executeQuery("show variables like 'port'");
            rs.next();
            System.err.println("New master Port: " + rs.getString(2));
            rs.close();
            s.close();
        }
    }

    /**
     * Test that new connections get the state changes from the Fabric node. The current implementation queries the Fabric node on every new connection and
     * therefore always has the latest state. Future shared state implementation may loosen this constraint.
     */
    public void manualTestNewConnHasNewState() throws Exception {
        FabricMySQLConnection conn = (FabricMySQLConnection) this.ds.getConnection(this.username, this.password);
        conn.setServerGroupName("ha_config1_group");

        String query = "SELECT CONCAT(@@hostname, ':', @@port) AS 'Value'";
        ResultSet rs = conn.createStatement().executeQuery(query);
        rs.next();
        String startingMaster = rs.getString(1);
        System.err.println("Starting master: " + startingMaster);
        rs.close();
        conn.close();

        // allow time to perform manual failover
        int secs = 15000;
        System.err.println("Sleeping " + (secs / 1000.0) + " seconds.... Please perform manual failover");
        Thread.sleep(secs);
        System.err.println("Continuing");

        // do the exact same thing and expect it to connect to the new master without an exception
        conn = (FabricMySQLConnection) this.ds.getConnection(this.username, this.password);
        conn.setServerGroupName("ha_config1_group");

        rs = conn.createStatement().executeQuery(query);
        rs.next();
        String newMaster = rs.getString(1);
        System.err.println("New master: " + newMaster);
        assertFalse(startingMaster.equals(newMaster));
        conn.close();
    }

    /**
     * Test that partially failed over connections (those that failed but could not immediately get a new connection) don't impact the creation of new
     * connections by being a part of the replication connection group.
     */
    public void manualTestFailedOldMasterDoesntBlockNewConnections() throws Exception {
        FabricMySQLConnection conn = (FabricMySQLConnection) this.ds.getConnection(this.username, this.password);
        conn.setServerGroupName("ha_config1_group");

        Statement s = conn.createStatement();

        // run a query until a failure happens. this will cause the master connection in the replication connection to be closed
        try {
            while (true) {
                s.executeUpdate("set @x = 1");
                try {
                    Thread.sleep(500);
                } catch (Exception ex) {
                }
            }
        } catch (SQLException ex) {
            System.err.println("Failure encountered: " + ex.getMessage());
            System.err.println("Waiting 10 seconds before trying a new connection");
            try {
                Thread.sleep(10*1000);
            } catch (Exception ex2) {
            }
        }

        // we leave the conn *open* and therefore in the connection group to make sure it doesn't prevent changing master which would happen if the
        // removeMasterHost() call failed

        // make sure a new connection is successful
        conn = (FabricMySQLConnection) this.ds.getConnection(this.username, this.password);
        conn.setServerGroupName("ha_config1_group");

        ResultSet rs = conn.createStatement().executeQuery("SELECT CONCAT(@@hostname, ':', @@port) AS 'Value'");
        rs.next();
        System.err.println("New master: " + rs.getString(1));
        rs.close();
        conn.close();
    }

    /**
     * Same as test `manualTestFailedOldMasterDoesntBlockNewConnections' for slaves. There must be only one slave in the HA group for this test.
     */
    public void manualTestFailedSingleSlaveDoesntBlockNewConnections() throws Exception {
        FabricMySQLConnection conn = (FabricMySQLConnection) this.ds.getConnection(this.username, this.password);
        conn.setServerGroupName("ha_config1_group");
        conn.setReadOnly(true);

        Statement s = conn.createStatement();

        // run a query until a failure happens. this will cause the slaves connection in the replication connection to be closed
        try {
            while (true) {
                ResultSet rs = s.executeQuery("select 1");
                rs.close();
                try {
                    Thread.sleep(500);
                } catch (Exception ex) {
                }
            }
        } catch (SQLException ex) {
            System.err.println("Failure encountered: " + ex.getMessage());
            System.err.println("Waiting 10 seconds before trying a new connection");
            try {
                Thread.sleep(10*1000);
            } catch (Exception ex2) {
            }
        }

        // we leave the conn *open* and therefore in the connection group to make sure it doesn't prevent changing SLAVE which would happen if the
        // removeSlaveHost() call failed

        // make sure a new connection is successful
        conn = (FabricMySQLConnection) this.ds.getConnection(this.username, this.password);
        conn.setServerGroupName("ha_config1_group");
        conn.setReadOnly(true);

        ResultSet rs = conn.createStatement().executeQuery("SELECT CONCAT(@@hostname, ':', @@port) AS 'Value'");
        rs.next();
        System.err.println("New slave: " + rs.getString(1));
        rs.close();
        conn.close();
    }
}
