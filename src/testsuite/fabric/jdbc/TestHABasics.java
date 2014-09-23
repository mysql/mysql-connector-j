/*
  Copyright (c) 2014, Oracle and/or its affiliates. All rights reserved.

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

    public void testFailoverManual() throws Exception {
        // Statement s = this.conn.createStatement();
        // s.executeUpdate("drop table if exists fruits");
        // s.executeUpdate("create table fruits (name varchar(30))");

        // Thread.sleep(10000);
        // try {
        // 	s.executeUpdate("insert into fruits values ('')");
        // 	fail("Master should be unavailable");
        // } catch(SQLException ex) {
        // 	ex.printStackTrace();
        // 	s.executeUpdate("insert into fruits values ('Starfruit')");
        // }
    }
}
