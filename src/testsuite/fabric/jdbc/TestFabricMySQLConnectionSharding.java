/*
  Copyright (c) 2013, 2014, Oracle and/or its affiliates. All rights reserved.

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
import java.sql.SQLException;
import java.sql.Statement;

import testsuite.fabric.BaseFabricTestCase;

import com.mysql.fabric.jdbc.FabricMySQLConnection;
import com.mysql.fabric.jdbc.FabricMySQLDataSource;

public class TestFabricMySQLConnectionSharding extends BaseFabricTestCase {
    private FabricMySQLDataSource ds;
    private FabricMySQLConnection conn;

    public TestFabricMySQLConnectionSharding() throws Exception {
        super();
        if (this.isSetForFabricTest) {
            this.ds = getNewDefaultDataSource();
        }
    }

    @Override
    public void setUp() throws Exception {
        if (this.isSetForFabricTest) {
            this.conn = (FabricMySQLConnection) this.ds.getConnection(this.username, this.password);
        }
    }

    @Override
    public void tearDown() throws Exception {
        if (this.isSetForFabricTest) {
            this.conn.close();
        }
    }

    /**
     * Test that we can create a table in the global group and see
     * it in other groups.
     */
    public void testGlobalTableCreation() throws Exception {
        if (!this.isSetForFabricTest) {
            return;
        }
        // get to the global group
        this.conn.setServerGroupName("fabric_test1_global");
        assertEquals("fabric_test1_global", this.conn.getCurrentServerGroup().getName());

        // create the table
        Statement stmt = this.conn.createStatement();
        stmt.executeUpdate("drop table if exists testGlobalTableCreation");
        stmt.executeUpdate("create table testGlobalTableCreation (x int)");
        stmt.executeUpdate("insert into testGlobalTableCreation values (999), (752)");
        stmt.close();

        // we have to wait for replication. This is not reliable
        // because replication can take an arbitrary amount of time to
        // provide data access on the slave
        Thread.sleep(3000);

        // check other groups for table
        ResultSet rs;
        String groupsToTest[] = new String[] { "fabric_test1_shard1", "fabric_test1_shard2", "fabric_test1_global" };
        for (String groupName : groupsToTest) {
            System.out.println("Testing data present in group `" + groupName + "'");
            this.conn.setServerGroupName(groupName);
            rs = this.conn.createStatement().executeQuery("select x from testGlobalTableCreation order by 1");
            assertTrue(rs.next()); // If test fails here, check replication wait above
            assertEquals("752", rs.getString(1));
            assertTrue(rs.next());
            assertEquals(999, rs.getInt(1));
            assertFalse(rs.next());
            rs.close();
        }

        // cleanup
        this.conn.setServerGroupName("fabric_test1_global");
        this.conn.createStatement().executeUpdate("drop table testGlobalTableCreation");
    }

    /**
     * Test that sharding works by creating data in two shards and making sure it's
     * only visible in the respective server group.
     */
    public void testShardSelection() throws Exception {
        if (!this.isSetForFabricTest) {
            return;
        }
        Statement stmt;
        ResultSet rs;

        // create table globally
        this.conn.setServerGroupName("fabric_test1_global");
        stmt = this.conn.createStatement();
        stmt.executeUpdate("drop table if exists employees");
        stmt.executeUpdate("create table employees (emp_no INT PRIMARY KEY, first_name CHAR(40), last_name CHAR(40))");

        // begin by inserting data directly in groups
        this.conn.setAutoCommit(false);

        this.conn.setServerGroupName("fabric_test1_shard1");
        stmt.executeUpdate("insert into employees values (1, 'Mickey', 'Mouse')");
        stmt.executeUpdate("insert into employees values (2, 'Donald', 'Duck')");
        this.conn.commit();

        this.conn.setServerGroupName("fabric_test1_shard2");
        stmt.executeUpdate("insert into employees values (10001, 'Jerry', 'Garcia')");
        stmt.executeUpdate("insert into employees values (10002, 'Jimmy', 'Page')");
        this.conn.commit();

        // insert more data using shard selection
        this.conn.setAutoCommit(true);
        this.conn.clearServerSelectionCriteria();
        this.conn.setShardTable("employees.employees");

        this.conn.setShardKey("3");
        assertEquals("fabric_test1_shard1", this.conn.getCurrentServerGroup().getName());
        stmt.executeUpdate("insert into employees values (3, 'Daffy', 'Duck')");

        this.conn.setShardKey("10003");
        assertEquals("fabric_test1_shard2", this.conn.getCurrentServerGroup().getName());
        stmt.executeUpdate("insert into employees values (10003, 'Jim', 'Morrison')");

        // check data by shard selection
        this.conn.setShardKey("1");
        assertEquals("fabric_test1_shard1", this.conn.getCurrentServerGroup().getName());
        rs = stmt.executeQuery("select * from employees where emp_no = 1");
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        assertEquals("Mickey", rs.getString(2));
        assertEquals("Mouse", rs.getString(3));
        assertFalse(rs.next());
        rs.close();
        this.conn.setShardKey("10001"); // emp_no=1 should NOT be present
        assertEquals("fabric_test1_shard2", this.conn.getCurrentServerGroup().getName());
        rs = stmt.executeQuery("select * from employees where emp_no = 1");
        assertFalse(rs.next());
        rs.close();
        // TODO check additional values

        // check data by group selection
        // TODO

        // cleanup
        this.conn.setServerGroupName("fabric_test1_global");
        this.conn.createStatement().executeUpdate("drop table employees");
    }

    /**
     * Test that providing the query table(s) to the connection chooses
     * the proper shard mapping/table.
     */
    public void testQueryTableShardSelection() throws Exception {
        if (!this.isSetForFabricTest) {
            return;
        }
        this.conn.setCatalog("employees");
        this.conn.addQueryTable("name of non-existing table");
        try {
            this.conn.createStatement();
            fail("Cannot do anything without a mapping/server group");
        } catch (SQLException ex) {
            assertEquals(com.mysql.jdbc.SQLError.SQL_STATE_CONNECTION_REJECTED, ex.getSQLState());
        }
        this.conn.addQueryTable("employees");
        this.conn.createStatement();

        this.conn.clearQueryTables();
    }

    /**
     * Test that providing several tables with conflicting shard mappings
     * prevents a cross-shard query.
     */
    public void testQueryTablesPreventCrossShardQuery() throws Exception {
        // TODO - need a test env with a configuration having
        // different shard mappings
    }
}
