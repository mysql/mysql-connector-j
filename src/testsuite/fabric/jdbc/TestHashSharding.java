/*
  Copyright (c) 2013, 2014, Oracle and/or its affiliates. All rights reserved.

  The MySQL Connector/J is licensed under the terms of the GPLv2
  <http://www.gnu.org/licenses/old-licenses/gpl-2.0.html>, like most MySQL Connectors.
  There are special exceptions to the terms and conditions of the GPLv2 as it is applied to
  this software, see the FLOSS License Exception
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

import testsuite.fabric.BaseFabricTestCase;

import com.mysql.fabric.jdbc.FabricMySQLConnection;
import com.mysql.fabric.jdbc.FabricMySQLDataSource;

/**
 * @todo this hash sharding test is incompatible with the
 *       default Fabric configuration for these C/J Fabric tests.
 */
public class TestHashSharding extends BaseFabricTestCase {
    private FabricMySQLDataSource ds;
    private FabricMySQLConnection conn;

    public TestHashSharding() throws Exception {
        super();
        if (this.isSetForFabricTest) {
            this.ds = getNewDefaultDataSource();
        }
    }

    @Override
    public void setUp() throws Exception {
        if (this.isSetForFabricTest) {
            this.conn = (FabricMySQLConnection) this.ds.getConnection(this.username, this.password);

            // create table globally
            this.conn.setServerGroupName("fabric_test1_global");
            Statement stmt = this.conn.createStatement();
            stmt.executeUpdate("drop table if exists employees");
            stmt.executeUpdate("create table employees (emp_no INT PRIMARY KEY, first_name CHAR(40), last_name CHAR(40))");
            this.conn.clearServerSelectionCriteria();
        }
    }

    @Override
    public void tearDown() throws Exception {
        if (this.isSetForFabricTest) {
            this.conn.close();
        }
    }

    /**
     * Run the given query and assert that the result contains at
     * least one row.
     */
    protected void checkRowExists(String sql) throws Exception {
        Statement s = this.conn.createStatement();
        ResultSet rs = s.executeQuery(sql);
        assertTrue("Row should exist", rs.next());
        rs.close();
        s.close();
    }

    /**
     * Run the given query and assert that the result set is empty.
     */
    protected void checkRowDoesntExist(String sql) throws Exception {
        Statement s = this.conn.createStatement();
        ResultSet rs = s.executeQuery(sql);
        assertFalse("Row should not exist", rs.next());
        rs.close();
        s.close();
    }

    /**
     * Assert that a row exists in the given server group.
     */
    protected void checkRowExistsInServerGroup(String sql, String groupName) throws Exception {
        this.conn.clearServerSelectionCriteria();
        this.conn.setServerGroupName(groupName);

        checkRowExists(sql);
    }

    /**
     * Assert that a row exists in the given shard determined by the key.
     */
    protected void checkRowExistsInKeyGroup(String sql, String key) throws Exception {
        this.conn.setShardKey(key);

        checkRowExists(sql);
    }

    /**
     * Assert that no row exists in the given server group.
     */
    protected void checkRowDoesntExistInServerGroup(String sql, String groupName) throws Exception {
        this.conn.clearServerSelectionCriteria();
        this.conn.setServerGroupName(groupName);

        checkRowDoesntExist(sql);
    }

    /**
     * Assert that no row exists in the given shard determined by the key.
     */
    public void checkRowDoesntExistInKeyGroup(String sql, String key) throws Exception {
        this.conn.setShardKey(key);

        checkRowDoesntExist(sql);
    }

    /**
     * Check data used by basic tests is in proper groups.
     * Test both by direct group selection and shard table/key selection.
     */
    protected void assertBasicDataIsInProperPlaces() throws Exception {
        String sql;
        sql = "select * from employees where emp_no = 1";
        checkRowExistsInServerGroup(sql, "fabric_test1_shard2");
        checkRowDoesntExistInServerGroup(sql, "fabric_test1_shard1");
        this.conn.clearServerSelectionCriteria();
        this.conn.setShardTable("employees");
        checkRowExistsInKeyGroup(sql, "1");
        checkRowDoesntExistInKeyGroup(sql, "9");
        sql = "select * from employees where emp_no = 6"; // repeat with key = 6
        checkRowExistsInServerGroup(sql, "fabric_test1_shard2");
        checkRowDoesntExistInServerGroup(sql, "fabric_test1_shard1");
        this.conn.clearServerSelectionCriteria();
        this.conn.setShardTable("employees");
        checkRowExistsInKeyGroup(sql, "6");
        checkRowDoesntExistInKeyGroup(sql, "19");

        sql = "select * from employees where emp_no = 9"; // other shard
        checkRowExistsInServerGroup(sql, "fabric_test1_shard1");
        checkRowDoesntExistInServerGroup(sql, "fabric_test1_shard2");
        this.conn.clearServerSelectionCriteria();
        this.conn.setShardTable("employees");
        checkRowExistsInKeyGroup(sql, "9");
        checkRowDoesntExistInKeyGroup(sql, "6");
        sql = "select * from employees where emp_no = 19"; // repeat with key = 19
        checkRowExistsInServerGroup(sql, "fabric_test1_shard1");
        checkRowDoesntExistInServerGroup(sql, "fabric_test1_shard2");
        this.conn.clearServerSelectionCriteria();
        this.conn.setShardTable("employees");
        checkRowExistsInKeyGroup(sql, "19");
        checkRowDoesntExistInKeyGroup(sql, "1");
    }

    /**
     * Basic tests for shard selection with a HASH shard mapping.
     */
    public void testBasicInsertByGroup() throws Exception {
        if (!this.isSetForFabricTest) {
            return;
        }
        Statement stmt;

        // insert data directly by group selection
        this.conn.setServerGroupName("fabric_test1_shard2");
        stmt = this.conn.createStatement();
        stmt.executeUpdate("insert into employees values (1, 'William', 'Gisbon')");
        stmt.executeUpdate("insert into employees values (6, 'Samuel', 'Delany')");

        this.conn.setServerGroupName("fabric_test1_shard1");
        stmt.executeUpdate("insert into employees values (9, 'William', 'Turner')");
        stmt.executeUpdate("insert into employees values (19, 'Albrecht', 'Durer')");

        // check everything is where it should be
        assertBasicDataIsInProperPlaces();
    }

    /**
     * Basic tests for shard selection with a HASH shard mapping.
     */
    public void testBasicInsertByKey() throws Exception {
        if (!this.isSetForFabricTest) {
            return;
        }
        Statement stmt;

        // insert data using shard selection
        this.conn.clearServerSelectionCriteria();
        this.conn.setShardTable("employees");
        stmt = this.conn.createStatement();

        this.conn.setShardKey("1"); // hash = c4ca4238a0b923820dcc509a6f75849b
        assertEquals("fabric_test1_shard2", this.conn.getCurrentServerGroup().getName());
        stmt.executeUpdate("insert into employees values (1, 'William', 'Gisbon')");
        this.conn.setShardKey("6"); // hash = 1679091c5a880faf6fb5e6087eb1b2dc
        assertEquals("fabric_test1_shard2", this.conn.getCurrentServerGroup().getName());
        stmt.executeUpdate("insert into employees values (6, 'Samuel', 'Delany')");

        this.conn.setShardKey("9"); // hash = 45c48cce2e2d7fbdea1afc51c7c6ad26
        assertEquals("fabric_test1_shard1", this.conn.getCurrentServerGroup().getName());
        stmt.executeUpdate("insert into employees values (9, 'William', 'Turner')");
        this.conn.setShardKey("19"); // hash = 1f0e3dad99908345f7439f8ffabdffc4
        assertEquals("fabric_test1_shard1", this.conn.getCurrentServerGroup().getName());
        stmt.executeUpdate("insert into employees values (19, 'Albrecht', 'Durer')");

        // check everything is where it should be
        assertBasicDataIsInProperPlaces();
    }
}
